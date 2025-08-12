
import os
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

# Reuse your scheduler and helpers
import scheduler_V4a_fixed as sched
from db_queries import job_pool_df as _jp, technicians_df as _techs

ACTIONS_API_KEY = os.getenv("ACTIONS_API_KEY")  # put this in your .env

app = FastAPI(title="SchedulerGPT API", version="1.0.0")

def _auth(x_api_key: Optional[str]):
    if not ACTIONS_API_KEY:
        return  # no key configured -> allow (local dev)
    if x_api_key != ACTIONS_API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")

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

def _summarize(schedule):
    days = len(schedule)
    jobs = sum(len(d["jobs"]) for d in schedule)
    return {"days": days, "jobs": jobs, "schedule": schedule}

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/schedule/preview")
def schedule_preview(body: ScheduleRequest, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    _auth(x_api_key)
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
        commit=False
    )
    return _summarize(sch)

@app.post("/schedule/commit")
def schedule_commit(body: ScheduleRequest, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    _auth(x_api_key)
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
        commit=True
    )
    return _summarize(sch)

@app.get("/jobs/search")
def jobs_search(region: Optional[str] = None,
                sow: Optional[str] = None,
                horizon_days: Optional[int] = None,
                tech_id: Optional[int] = None,
                radius_miles: Optional[int] = None,
                x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    _auth(x_api_key)
    jp = _jp().copy()
    jp = jp[jp["jp_status"] != "Scheduled"]
    if region:
        jp = jp[jp["region"] == region]
    if sow:
        jp = jp[jp["sow_1"].str.contains(sow, na=False)]
    if horizon_days is not None:
        jp = jp[(jp["days_til_due"] <= horizon_days) | (jp["jp_priority"] == "Monthly O&M") | (jp["night_test"] == True)]
    if tech_id is not None and radius_miles is not None:
        t = _techs()
        home = t.loc[t["technician_id"] == tech_id].iloc[0]
        def _m(row):
            return sched.haversine(home["home_latitude"], home["home_longitude"], row["latitude"], row["longitude"])
        jp = jp[jp.apply(_m, axis=1) <= float(radius_miles)]
    # return small fields to GPT
    cols = ["work_order","site_name","region","sow_1","due_date","jp_priority","days_til_due","latitude","longitude","cluster_id"]
    cols = [c for c in cols if c in jp.columns]
    return {"count": int(len(jp)), "rows": jp[cols].head(200).to_dict(orient="records")}
