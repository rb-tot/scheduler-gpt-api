# scheduler_api.py
import os
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# DB helpers
from supabase_client import sb_select, sb_rpc, supabase_client

# Scheduler core
import scheduler_V4a_fixed as sched
from db_queries import job_pool_df as _jp, technicians_df as _techs

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(title="SchedulerGPT API", version="1.6.0")
# Serve frontend files
app.mount("/static", StaticFiles(directory="frontend"), name="static")
@app.get("/")
def serve_frontend():
    return FileResponse("frontend/index.html")

# Auth sources
ACTIONS_API_KEY = os.getenv("ACTIONS_API_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY")
BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://scheduler-gpt-api.onrender.com")


def _allowed_keys():
    return {k for k in (ACTIONS_API_KEY, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_ANON_KEY) if k}


def _auth(x_api_key: Optional[str], authorization: Optional[str], apikey: Optional[str]):
    allowed = _allowed_keys()
    if not allowed:  # allow in dev if no keys configured
        return
    token = x_api_key or apikey
    if not token and authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1]
    if token not in allowed:
        raise HTTPException(status_code=401, detail="invalid or missing API key")


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------
class ScheduleRequest(BaseModel):
    tech_id: int
    start_date: str  # 'YYYY-MM-DD'
    assigned_clusters: Optional[List[int]] = None
    priority_work_orders: Optional[List[int]] = None
    horizon_days: int = 21
    target_sow_list: Optional[List[str]] = None
    anchor_week_strategy: str = "exhaustive"
    allow_in_day_filler: bool = True
    fixed_jobs: Optional[List[dict]] = None
    weekly_target_hours_min: int = 45
    weekly_target_hours_max: int = 50
    max_drive_minutes: int = 60
    pre_night_day_job_cap: int = 4
    radius_miles_cap: Optional[int] = 250
    seed_region: Optional[str] = None
    seed_cluster: Optional[int] = None


class ValidateReq(BaseModel):
    work_order: int
    technician_id: int
    date: date
    start_time: Optional[str] = None
    end_time: Optional[str] = None


class WorkOrdersReq(BaseModel):
    work_orders: List[int]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _parse_date(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _summary(schedule):
    # enrich for readable preview
    jp = _jp()
    by_wo: Dict[int, Dict[str, Any]] = {}
    if not jp.empty:
        cols = [c for c in ["work_order", "site_name", "site_city", "site_state", "sow_1",
                            "due_date", "region", "jp_priority"] if c in jp.columns]
        meta = jp[cols].copy()
        if "due_date" in meta.columns:
            meta["due_date"] = meta["due_date"].astype(str)
        by_wo = meta.set_index("work_order").to_dict(orient="index")

    out, total_jobs = [], 0
    for day in schedule:
        jobs = []
        for j in day["jobs"]:
            m = by_wo.get(int(j["work_order"]), {})
            jobs.append({
                "work_order": int(j["work_order"]),
                "date": str(j["date"].date()),
                "site_name": m.get("site_name"),
                "site_city": m.get("site_city"),
                "site_state": m.get("site_state"),
                "region": m.get("region"),
                "sow_1": m.get("sow_1"),
                "due_date": m.get("due_date"),
                "jp_priority": m.get("jp_priority"),
                "travel_time": float(j["travel_time"]),
                "job_time": float(j["job_time"]),
                "total_time": float(j["total_time"]),
                "night_job": bool(j["night_job"]),
            })
        total_jobs += len(jobs)
        out.append({"date": str(day["date"].date()), "total_hours": float(day["total_hours"]), "jobs": jobs})
    return {"days": len(out), "jobs": total_jobs, "schedule": out}


def _sum_est_hours_for_sched(sched_rows: List[Dict[str, Any]], job_pool_map: Dict[int, Dict[str, Any]]) -> float:
    total = 0.0
    for r in sched_rows:
        wo = r.get("work_order")
        if wo is None:
            continue
        jp = job_pool_map.get(int(wo))
        if jp and isinstance(jp.get("est_hours"), (int, float)):
            total += float(jp["est_hours"])
    return total


# -----------------------------------------------------------------------------
# Diagnostics
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/debug/keys")
def debug_keys():
    return {
        "has_ACTIONS_API_KEY": bool(ACTIONS_API_KEY),
        "has_SERVICE_ROLE": bool(SUPABASE_SERVICE_ROLE_KEY),
        "has_ANON": bool(SUPABASE_ANON_KEY),
    }


# -----------------------------------------------------------------------------
# Schedule endpoints
# -----------------------------------------------------------------------------
@app.post("/schedule/preview")
def schedule_preview(
    body: ScheduleRequest,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    apikey: Optional[str] = Header(default=None, alias="apikey"),
):
    _auth(x_api_key, authorization, apikey)
    dt = _parse_date(body.start_date)
    sch = sched.assign_technician(
        tech_id=body.tech_id,
        start_date=dt,
        assigned_clusters=body.assigned_clusters,
        priority_work_orders=body.priority_work_orders,
        horizon_days=body.horizon_days,
        target_sow_list=body.target_sow_list,
        anchor_week_strategy=body.anchor_week_strategy,
        allow_in_day_filler=body.allow_in_day_filler,
        fixed_jobs=body.fixed_jobs,
        weekly_target_hours_min=body.weekly_target_hours_min,
        weekly_target_hours_max=body.weekly_target_hours_max,
        max_drive_minutes=body.max_drive_minutes,
        pre_night_day_job_cap=body.pre_night_day_job_cap,
        radius_miles_cap=body.radius_miles_cap,
        seed_region=body.seed_region,
        seed_cluster=body.seed_cluster,
        commit=False,
    )
    return _summary(sch)


@app.post("/schedule/commit")
def schedule_commit(
    body: ScheduleRequest,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    apikey: Optional[str] = Header(default=None, alias="apikey"),
):
    _auth(x_api_key, authorization, apikey)
    dt = _parse_date(body.start_date)
    try:
        sch = sched.assign_technician(
            tech_id=body.tech_id,
            start_date=dt,
            assigned_clusters=body.assigned_clusters,
            priority_work_orders=body.priority_work_orders,
            horizon_days=body.horizon_days,
            target_sow_list=body.target_sow_list,
            anchor_week_strategy=body.anchor_week_strategy,
            allow_in_day_filler=body.allow_in_day_filler,
            fixed_jobs=body.fixed_jobs,
            weekly_target_hours_min=body.weekly_target_hours_min,
            weekly_target_hours_max=body.weekly_target_hours_max,
            max_drive_minutes=body.max_drive_minutes,
            pre_night_day_job_cap=body.pre_night_day_job_cap,
            radius_miles_cap=body.radius_miles_cap,
            seed_region=body.seed_region,
            seed_cluster=body.seed_cluster,
            commit=True,
        )
        return _summary(sch)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Jobs search (PostGIS RPC)
# -----------------------------------------------------------------------------
@app.get("/jobs/search")
def jobs_search(
    tech_id: int = Query(..., ge=1),
    radius_miles: int = Query(250, ge=1, le=1000),
    due_within_days: int = Query(21, ge=1, le=180),
    limit: int = Query(50, ge=1, le=500),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    apikey: Optional[str] = Header(default=None, alias="apikey"),
):
    _auth(x_api_key, authorization, apikey)
    rows = sb_rpc("jobs_within_radius", {
        "_tech_id": tech_id,
        "_radius_miles": radius_miles,
        "_due_within_days": due_within_days,
        "_limit": limit
    }) or []
    # normalize names for Actions consumers
    for r in rows:
        if "site_city" in r and "city" not in r:
            r["city"] = r["site_city"]
        if "site_state" in r and "state" not in r:
            r["state"] = r["site_state"]
    return rows


# -----------------------------------------------------------------------------
# Raw job pool window
# -----------------------------------------------------------------------------
@app.get("/jobs/pool")
def jobs_pool(
    start: date,
    end: date,
    states: Optional[str] = Query(None, description="CSV list, e.g. CO,UT,AZ"),
    statuses: Optional[str] = Query(None, description="CSV list. If omitted or '*', no status filter."),
    is_night: Optional[bool] = Query(None),
    limit: int = Query(500, ge=1, le=5000),
):
    # Returns job_pool rows in a due_date window
    filters: List = [("due_date", "gte", str(start)), ("due_date", "lte", str(end))]
    if states:
        # use site_state (not state) per schema
        filters.append(("site_state", "in", [s.strip() for s in states.split(",") if s.strip()]))
    if statuses and statuses.strip() != "*":
        filters.append(("jp_status", "in", [s.strip() for s in statuses.split(",") if s.strip()]))

    rows = sb_select("job_pool", filters=filters)

    if is_night is not None:
        rows = [r for r in rows if bool(r.get("is_night")) == is_night]

    if limit:
        rows = rows[:limit]

    return {"count": len(rows), "jobs": rows}


# -----------------------------------------------------------------------------
# Technicians and existing schedule
# -----------------------------------------------------------------------------
@app.get("/technicians")
def technicians(
    active_only: bool = True,
    night_eligible: Optional[bool] = Query(None),
    region: Optional[str] = Query(None),
    limit: int = Query(500, ge=1, le=5000),
):
    filters = [("active", "eq", True)] if active_only else None
    rows = sb_select("technicians", filters=filters)
    if night_eligible is not None:
        rows = [r for r in rows if bool(r.get("night_eligible")) == bool(night_eligible)]
    if region:
        out = []
        for r in rows:
            regs = r.get("regions")
            if isinstance(regs, list) and region in regs:
                out.append(r)
            elif isinstance(regs, str) and region.lower() in regs.lower():
                out.append(r)
        rows = out
    return {"count": len(rows), "technicians": rows[:limit]}


@app.get("/schedule/existing")
def schedule_existing(
    start: date,
    end: date,
    technician_ids: Optional[str] = Query(None, description="CSV IDs"),
    limit: int = Query(1000, ge=1, le=5000),
):
    filters: List = [("date", "gte", str(start)), ("date", "lte", str(end))]
    if technician_ids:
        ids = [int(x) for x in technician_ids.split(",") if x.strip().isdigit()]
        if ids:
            filters.append(("technician_id", "in", ids))
    rows = sb_select("scheduled_jobs", filters=filters)
    return {"count": len(rows), "rows": rows[:limit]}


# -----------------------------------------------------------------------------
# Validation endpoint
# -----------------------------------------------------------------------------
@app.post("/schedule/validate")
def schedule_validate(req: ValidateReq):
    sb = supabase_client()

    # Load job
    job_rows = sb_select("job_pool", filters=[("work_order", "eq", int(req.work_order))], limit=1)
    if not job_rows:
        return {"ok": False, "errors": [f"work_order {req.work_order} not found"], "warnings": [], "metrics": {}}
    job = job_rows[0]

    # Already scheduled?
    sj = sb.table("scheduled_jobs").select("work_order").eq("work_order", req.work_order).limit(1).execute().data
    if sj:
        return {"ok": False, "errors": ["Job already scheduled"], "warnings": [], "metrics": {}}

    # Technician row (use technician_id, not id)
    tech_rows = sb_select("technicians", filters=[("technician_id", "eq", int(req.technician_id))], limit=1)
    if not tech_rows:
        return {"ok": False, "errors": [f"technician {req.technician_id} not found"], "warnings": [], "metrics": {}}
    tech = tech_rows[0]
    max_daily = float(tech.get("max_daily_hours") or 8)
    max_weekly = float(tech.get("max_weekly_hours") or 40)

    # Eligibility
    elig_rows = sb_select("job_technician_eligibility", filters=[("work_order", "eq", int(req.work_order))])
    if elig_rows:  # if table has entries for this job, enforce
        allowed = any(int(e.get("technician_id")) == int(req.technician_id) for e in elig_rows)
        if not allowed:
            return {"ok": False, "errors": ["Technician not eligible for this job"], "warnings": [], "metrics": {}}

    # Blackouts on that date
    blks = sb_select("blackouts", filters=[("technician_id", "eq", int(req.technician_id)), ("date", "eq", str(req.date))])
    blocked_hours = float(blks[0].get("hours_blocked")) if blks else 0.0

    # Existing schedule for day + week window
    day_sched = sb.table("scheduled_jobs").select("*").eq("technician_id", req.technician_id).eq("date", str(req.date)).execute().data
    weekday = req.date.weekday()  # Mon=0
    week_start = req.date - timedelta(days=weekday)
    week_end = week_start + timedelta(days=6)
    wk_sched = sb.table("scheduled_jobs").select("*").eq("technician_id", req.technician_id).gte("date", str(week_start)).lte("date", str(week_end)).execute().data

    # Compute est hours
    wos = [int(x["work_order"]) for x in wk_sched if x.get("work_order") is not None]
    jp_rows = sb_select("job_pool", filters=[("work_order", "in", list(set(wos + [int(req.work_order)])))])
    jp_map = {int(r["work_order"]): r for r in jp_rows if r.get("work_order") is not None}

    est_this = float(job.get("est_hours") or 0.0)
    day_hours_before = _sum_est_hours_for_sched(day_sched, jp_map)
    week_hours_before = _sum_est_hours_for_sched(wk_sched, jp_map)
    day_hours_after = day_hours_before + est_this
    week_hours_after = week_hours_before + est_this

    errors: List[str] = []
    warnings: List[str] = []

    # Daily capacity (respect blackouts)
    if day_hours_after > max_daily - blocked_hours + 1e-6:
        errors.append(f"Daily cap exceeded: {day_hours_after:.1f} > {max_daily - blocked_hours:.1f}")

    # Weekly capacity
    if week_hours_after > max_weekly + 1e-6:
        errors.append(f"Weekly cap exceeded: {week_hours_after:.1f} > {max_weekly:.1f}")

    # Night job rules
    is_night = bool(job.get("is_night"))
    if is_night:
        if not bool(tech.get("night_eligible")):
            errors.append("Tech not night eligible")
        # recovery day check
        rec_day = req.date + timedelta(days=1)
        rec_sched = sb.table("scheduled_jobs").select("*").eq("technician_id", req.technician_id).eq("date", str(rec_day)).execute().data
        rec_hours = _sum_est_hours_for_sched(rec_sched, jp_map)
        if rec_hours > 0.5 * max_daily + 1e-6:
            errors.append(f"Recovery day over 50% cap: {rec_hours:.1f} > {0.5*max_daily:.1f}")

    # Cluster proximity heuristic (warn only)
    cluster_id = job.get("cluster_id")
    if cluster_id is not None and day_sched:
        sched_wos = [int(r["work_order"]) for r in day_sched if r.get("work_order") is not None]
        sched_jp = [jp_map.get(w) for w in sched_wos]
        mixed = any(j and j.get("cluster_id") is not None and j.get("cluster_id") != cluster_id for j in sched_jp)
        if mixed:
            warnings.append("Mixing clusters on the same day")

    metrics = {
        "daily_hours_before": round(day_hours_before, 2),
        "daily_hours_after": round(day_hours_after, 2),
        "max_daily_hours": max_daily,
        "blocked_hours": blocked_hours,
        "weekly_hours_before": round(week_hours_before, 2),
        "weekly_hours_after": round(week_hours_after, 2),
        "max_weekly_hours": max_weekly,
        "is_night": is_night
    }

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings, "metrics": metrics}


# -----------------------------------------------------------------------------
# Jobs by work orders (object body for GPT Actions)
# -----------------------------------------------------------------------------
@app.post("/jobs/by_work_orders")
def jobs_by_work_orders(req: WorkOrdersReq):
    jp = _jp()
    if jp.empty or not req.work_orders:
        return {"rows": []}
    want = jp[jp["work_order"].isin(req.work_orders)].copy()
    cols = [c for c in ["work_order", "site_name", "site_city", "site_state", "sow_1",
                         "due_date", "region", "jp_priority", "days_til_due", "cluster_id"]
            if c in want.columns]
    want = want[cols]
    if "due_date" in want.columns:
        want["due_date"] = want["due_date"].astype(str)
    return {"rows": want.astype(object).where(want.notna(), None).to_dict(orient="records")}

#-----------------------------------------------------------------------------------------------------------------------------------------
# Add to scheduler_api.py

@app.get("/analysis/monthly")
def analyze_monthly_jobs(
    year: int = Query(...),
    month: int = Query(...),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
):
    _auth(x_api_key, None, None)
    
    # Get all jobs for the month
    month_start = f"{year}-{month:02d}-01"
    month_end = f"{year}-{month:02d}-31"
    
    jobs = sb_select("job_pool", filters=[
        ("due_date", "gte", month_start),
        ("due_date", "lte", month_end),
        ("jp_status", "in", ["Call", "Waiting to Schedule"])
    ])
    
    # Get tech locations for distance calculations
    techs = sb_select("technicians", filters=[("active", "eq", True)])
    
       
    # Analyze problems
    problem_jobs = {
        "remote_locations": [],  # >100 miles from nearest tech
        "limited_eligibility": [],  # Only 1-2 techs qualified
        "night_jobs": [],
        "friday_restricted": []  # King Soopers, City Market, Alta
    }
    
    # Weekly distribution
    weekly_dist = {
        "week_1": {"must_do": [], "should_do": []},
        "week_2": {"must_do": [], "should_do": []},
        "week_3": {"must_do": [], "should_do": []},
        "week_4": {"must_do": [], "should_do": []},
    }
    
    for job in jobs:
        # Check eligibility - IT'S ALREADY IN THE DATA!
        if job.get("tech_count", 0) <= 2:
            problem_jobs["limited_eligibility"].append({
                "work_order": job["work_order"],
                "site_name": job["site_name"],
                "eligible_techs": job["tech_count"]
            })
        
        # Check remoteness
        if techs:
            min_distance = min([
                haversine(t["home_latitude"], t["home_longitude"], 
                         job["latitude"], job["longitude"])
                for t in techs
            ])
            if min_distance > 100:
                problem_jobs["remote_locations"].append({
                    "work_order": job["work_order"],
                    "site_name": job["site_name"],
                    "distance": round(min_distance, 1)
                })
        
        # Check night jobs
        if job.get("is_night") or job.get("night_test"):
            problem_jobs["night_jobs"].append({
                "work_order": job["work_order"], # Use job
                "site_name": job["site_name"]
            })
        
        # Check Friday restrictions
        if any(x in job.get("site_name", "") for x in ["King Soopers", "City Market", "Alta"]):
            problem_jobs["friday_restricted"].append({
                "work_order": wo,
                "site_name": job["site_name"]
            })
        
        # Categorize by week
        due_date = pd.to_datetime(job["due_date"])
        week_num = (due_date.day - 1) // 7 + 1
        week_key = f"week_{min(week_num, 4)}"
        
        if job["jp_priority"] in ["NOV", "Urgent", "Monthly O&M"]:
            weekly_dist[week_key]["must_do"].append(wo)
        else:
            weekly_dist[week_key]["should_do"].append(wo)
    
    # Summary stats
    summary = {
        "total_jobs": len(jobs),
        "problem_jobs_count": {
            "remote": len(problem_jobs["remote_locations"]),
            "limited_techs": len(problem_jobs["limited_eligibility"]),
            "night": len(problem_jobs["night_jobs"]),
            "friday_restricted": len(problem_jobs["friday_restricted"])
        },
        "weekly_summary": {
            k: {
                "must_do": len(v["must_do"]),
                "should_do": len(v["should_do"])
            } for k, v in weekly_dist.items()
        }
    }
    
    return {
        "summary": summary,
        "problem_jobs": problem_jobs,
        "weekly_distribution": weekly_dist
    }

# Add haversine helper if not exists
def haversine(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, sqrt, atan2
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    c = 2*atan2(sqrt(a), sqrt(1-a))
    return R * c

# -----------------------------------------------------------------------------
# Custom OpenAPI for GPT Actions
# -----------------------------------------------------------------------------
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)

    # 1) servers (required by GPT Actions)
    schema["servers"] = [{"url": BASE_URL}]

    # 2) strip header params from operations to avoid warnings
    for _, methods in schema.get("paths", {}).items():
        for _, op in list(methods.items()):
            if not isinstance(op, dict):
                continue
            params = op.get("parameters", [])
            op["parameters"] = [p for p in params if p.get("in") != "header"]
            if "security" in op:
                del op["security"]

    # 3) single security scheme
    schema.setdefault("components", {})
    schema["components"]["securitySchemes"] = {
        "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
    }
    schema["security"] = [{"ApiKeyAuth": []}]

    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi
