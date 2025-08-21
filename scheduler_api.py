# scheduler_api.py
import os
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import traceback
import pandas as pd


# reuse your scheduler + db helpers
import scheduler_V4a_fixed as sched
from db_queries import job_pool_df as _jp, technicians_df as _techs

app = FastAPI(title="SchedulerGPT API", version="1.4.5")

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
    return {"days": len(schedule), "jobs": sum(len(d["jobs"]) for d in schedule), "schedule": schedule}

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
        print("COMMIT ERROR:", traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"commit_failed; service_role_loaded={bool(SUPABASE_SERVICE_ROLE_KEY)}; err={e}"
        )

@app.get("/jobs/search")
def jobs_search(
    region: Optional[str] = None,
    sow: Optional[str] = None,
    horizon_days: Optional[int] = None,
    tech_id: Optional[int] = None,
    radius_miles: Optional[int] = None,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    apikey: Optional[str] = Header(default=None, alias="apikey"),
):
    _auth(x_api_key, authorization, apikey)
    try:
        jp = _jp().copy()
        if "jp_status" in jp.columns:
            jp = jp[jp["jp_status"] != "Scheduled"]
        if region and "region" in jp.columns:
            jp = jp[jp["region"] == region]
        if sow and "sow_1" in jp.columns:
            jp = jp[jp["sow_1"].fillna("").str.contains(sow, case=False, regex=False)]
        if horizon_days is not None and all(c in jp.columns for c in ("days_til_due","jp_priority","night_test")):
            jp = jp[(jp["days_til_due"] <= horizon_days) | (jp["jp_priority"] == "Monthly O&M") | (jp["night_test"] == True)]
        if tech_id is not None and radius_miles is not None and all(c in jp.columns for c in ("latitude","longitude")):
            t = _techs()
            home = t.loc[t["technician_id"] == tech_id].iloc[0]
            def _m(row):
                return sched.haversine(home["home_latitude"], home["home_longitude"], row["latitude"], row["longitude"])
            jp = jp[jp.apply(_m, axis=1) <= float(radius_miles)]
        cols = ["work_order","site_name","region","sow_1","due_date","jp_priority","days_til_due","latitude","longitude","cluster_id"]
        cols = [c for c in cols if c in jp.columns]

        # JSON-safe: cast dates to str, cast all columns to object, replace NaN with None
        safe = jp[cols].copy()
        if "due_date" in safe.columns:
            safe["due_date"] = safe["due_date"].astype(str)
        safe = safe.astype(object)               # <- important, prevents None -> NaN coercion
        safe = safe.where(pd.notna(safe), None)  # replace remaining NaN with JSON-null

        return {"count": int(len(jp)), "rows": safe.head(200).to_dict(orient="records")}
    except Exception as e:
        print("SEARCH ERROR:", traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"search_failed: {e}")

#AI told me to add
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    # Add bearer security so GPT can inject Authorization automatically
    schema.setdefault("components", {}).setdefault("securitySchemes", {})
    schema["components"]["securitySchemes"]["bearerAuth"] = {"type": "http", "scheme": "bearer"}
    schema["security"] = [{"bearerAuth": []}]
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi
