# scheduler_api.py
import os
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import traceback
import pandas as pd
from fastapi.openapi.utils import get_openapi
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi import Query
from datetime import date
from typing import Optional, List
from supabase_client import sb_select
# reuse your scheduler + db helpers
import scheduler_V4a_fixed as sched
from db_queries import job_pool_df as _jp, technicians_df as _techs
#Added for databse function
from supabase_client import sb_select, sb_rpc



#SchedulerGPT API — aligned with Supabase schema
#Endpoints:
#- GET  /health              -> liveness check
#- GET  /debug/keys          -> see which API keys are loaded
#- POST /schedule/preview    -> build one-week plan (no writes)
#- POST /schedule/commit     -> build + write to Supabase (uses db_writes)
#- GET  /jobs/search         -> find unscheduled jobs with filters
#- POST /jobs/by_work_orders -> fetch human-readable details for given WOs
# Notes:
#- Reads use db_queries.py (live Supabase).
#- Writes happen inside scheduler_V4a_fixed.assign_technician(commit=True) via db_writes.py.

app = FastAPI(title="SchedulerGPT API", version="1.4.6")

# --- Auth keys (accept X-API-Key, Authorization: Bearer, or apikey) ---
ACTIONS_API_KEY = os.getenv("ACTIONS_API_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY")

def _allowed_keys():
    return {k for k in (ACTIONS_API_KEY, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_ANON_KEY) if k}

def _auth(x_api_key: Optional[str], authorization: Optional[str], apikey: Optional[str]):
    allowed = _allowed_keys()
    if not allowed:  # no keys set -> allow in dev
        return
    token = x_api_key or apikey
    if not token and authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1]
    if token not in allowed:
        raise HTTPException(status_code=401, detail="invalid or missing API key")

# --- Models ---
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

def _parse_date(s: str) -> datetime:
    return datetime.fromisoformat(s)

def _summary(schedule):
    # enrich jobs from job_pool so responses are human-readable
    jp = _jp()
    by_wo = {}
    if not jp.empty:
        cols = [c for c in ["work_order","site_name","site_city","site_state","sow_1","due_date","region","jp_priority"] if c in jp.columns]
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


# --- Diagnostics ---
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

# --- Endpoints ---
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
        # return exact reason from scheduler/db_writes
        raise HTTPException(status_code=500, detail=str(e))


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
    # Optional: normalize names for Actions schema
    for r in rows:
        if "site_city" in r and "city" not in r:
            r["city"] = r["site_city"]
        if "site_state" in r and "state" not in r:
            r["state"] = r["site_state"]
    return rows


@app.get("/jobs/pool")
def jobs_pool(
    start: date,
    end: date,
    states: Optional[str] = Query(None, description="CSV list, e.g. CO,UT,AZ"),
    statuses: Optional[str] = Query(None, description="CSV list. If omitted or '*', no status filter."),
    is_night: Optional[bool] = Query(None),
    limit: int = Query(500, ge=1, le=5000),
):
    
    #Returns all job_pool rows in the due_date window.
    #No default jp_status filter. GPT can pass statuses if needed.
    
    filters: List = [("due_date", "gte", str(start)), ("due_date", "lte", str(end))]
    if states:
        filters.append(("state", "in", [s.strip() for s in states.split(",") if s.strip()]))
    if statuses and statuses.strip() != "*":
        filters.append(("jp_status", "in", [s.strip() for s in statuses.split(",") if s.strip()]))

    rows = sb_select("job_pool", filters=filters)

    if is_night is not None:
        rows = [r for r in rows if bool(r.get("is_night")) == is_night]

    if limit:
        rows = rows[:limit]

    return {"count": len(rows), "jobs": rows}


@app.get("/openapi-gpt.json", include_in_schema=False)
def openapi_gpt(request: Request):
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)

    # 1) servers: set to your live base URL
    base = str(request.base_url).rstrip("/")
    schema["servers"] = [{"url": base}]

    # 2) strip header parameters (Authorization, apikey, X-API-Key) from all ops
    for _, methods in schema.get("paths", {}).items():
        for _, op in list(methods.items()):
            if not isinstance(op, dict):
                continue
            params = op.get("parameters", [])
            op["parameters"] = [p for p in params if p.get("in") != "header"]
            # also remove any per-operation security to avoid multiple schemes
            if "security" in op:
                del op["security"]

    # 3) declare a single security scheme (API key in header)
    schema.setdefault("components", {})
    schema["components"]["securitySchemes"] = {
        "ApiKeyAuth": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
    }
    schema["security"] = [{"ApiKeyAuth": []}]

    return JSONResponse(schema)

# === READ endpoints for GPT ===
from fastapi import Query
from datetime import date, timedelta
from typing import Optional, List, Dict, Any
from supabase_client import sb_select, supabase_client

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
    filters: List = [("date","gte",str(start)), ("date","lte",str(end))]
    if technician_ids:
        ids = [int(x) for x in technician_ids.split(",") if x.strip().isdigit()]
        if ids:
            filters.append(("technician_id", "in", ids))
    rows = sb_select("scheduled_jobs", filters=filters)
    return {"count": len(rows), "rows": rows[:limit]}

# === VALIDATION endpoint (rule checks) ===
from pydantic import BaseModel

class ValidateReq(BaseModel):
    work_order: int
    technician_id: int
    date: date
    start_time: Optional[str] = None
    end_time: Optional[str] = None

def _sum_est_hours_for_sched(sched_rows: List[Dict[str, Any]], job_pool_map: Dict[int, Dict[str, Any]]) -> float:
    total = 0.0
    for r in sched_rows:
        wo = r.get("work_order")
        if wo is None: 
            continue
        jp = job_pool_map.get(int(wo))
        if jp and isinstance(jp.get("est_hours"), (int,float)):
            total += float(jp["est_hours"])
    return total

@app.post("/schedule/validate")
def schedule_validate(req: ValidateReq):
    sb = supabase_client()

    # Load job
    job_rows = sb_select("job_pool", filters=[("work_order","eq", int(req.work_order))], limit=1)
    if not job_rows:
        return {"ok": False, "errors": [f"work_order {req.work_order} not found"], "warnings": [], "metrics": {}}
    job = job_rows[0]

    # Already scheduled?
    sj = sb.table("scheduled_jobs").select("work_order").eq("work_order", req.work_order).limit(1).execute().data
    if sj:
        return {"ok": False, "errors": ["Job already scheduled"], "warnings": [], "metrics": {}}

    # Technician row
    tech_rows = sb_select("technicians", filters=[("id","eq", int(req.technician_id))], limit=1)
    if not tech_rows:
        return {"ok": False, "errors": [f"technician {req.technician_id} not found"], "warnings": [], "metrics": {}}
    tech = tech_rows[0]
    max_daily = float(tech.get("max_daily_hours") or 8)
    max_weekly = float(tech.get("max_weekly_hours") or 40)

    # Eligibility
    elig_rows = sb_select("job_technician_eligibility", filters=[("work_order","eq", int(req.work_order))])
    if elig_rows:  # if table has entries for this job, enforce
        allowed = any(int(e.get("technician_id")) == int(req.technician_id) for e in elig_rows)
        if not allowed:
            return {"ok": False, "errors": ["Technician not eligible for this job"], "warnings": [], "metrics": {}}

    # Blackouts on that date
    blks = sb_select("blackouts", filters=[("technician_id","eq", int(req.technician_id)), ("date","eq", str(req.date))])
    blocked_hours = float(blks[0].get("hours_blocked")) if blks else 0.0

    # Existing schedule for day + week window
    day_sched = sb.table("scheduled_jobs").select("*").eq("technician_id", req.technician_id).eq("date", str(req.date)).execute().data
    # week Monday..Sunday around req.date
    weekday = req.date.weekday()  # Mon=0
    week_start = req.date - timedelta(days=weekday)
    week_end = week_start + timedelta(days=6)
    wk_sched = sb.table("scheduled_jobs").select("*").eq("technician_id", req.technician_id).gte("date", str(week_start)).lte("date", str(week_end)).execute().data

    # Compute est hours
    # map of job_pool by work_order for lookup
    # get all work_orders referenced in existing schedule that we know
    wos = [int(x["work_order"]) for x in wk_sched if x.get("work_order") is not None]
    jp_rows = sb_select("job_pool", filters=[("work_order","in", list(set(wos + [int(req.work_order)])))])
    jp_map = {int(r["work_order"]): r for r in jp_rows if r.get("work_order") is not None}

    est_this = float(job.get("est_hours") or 0.0)
    day_hours_before = _sum_est_hours_for_sched(day_sched, jp_map)
    week_hours_before = _sum_est_hours_for_sched(wk_sched, jp_map)
    day_hours_after = day_hours_before + est_this
    week_hours_after = week_hours_before + est_this

    errors = []
    warnings = []

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


@app.post("/jobs/by_work_orders")
def jobs_by_work_orders(work_orders: List[int]):
    jp = _jp()
    if jp.empty or not work_orders:
        return {"rows": []}
    want = jp[jp["work_order"].isin(work_orders)].copy()
    cols = [c for c in ["work_order","site_name","site_city","site_state","sow_1","due_date","region","jp_priority","days_til_due","cluster_id"] if c in want.columns]
    want = want[cols]
    if "due_date" in want.columns:
        want["due_date"] = want["due_date"].astype(str)
    return {"rows": want.astype(object).where(want.notna(), None).to_dict(orient="records")}
