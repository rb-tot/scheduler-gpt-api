# scheduler_api_unified.py - CLEAN UNIFIED API
import os
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Environment setup
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("ACTIONS_API_KEY", "devkey123")
BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost:8000")

# Import your existing modules
try:
    from supabase_client import sb_select, sb_insert, sb_update
    from db_queries import job_pool_df as _jp, technicians_df as _techs
    import scheduler_V4a_fixed as sched
except ImportError:
    print("⚠️ Missing dependencies - install: supabase, pandas")

# ============================================================================
# APP SETUP
# ============================================================================
app = FastAPI(title="Unified Scheduler API", version="2.0.0")

# Serve frontend
try:
    app.mount("/static", StaticFiles(directory="frontend"), name="static")
except:
    print("⚠️ Frontend directory not found")

@app.get("/")
def serve_app():
    try:
        return FileResponse("frontend/scheduler.html")
    except:
        return {"message": "Scheduler API Running", "docs": f"{BASE_URL}/docs"}

# ============================================================================
# AUTH
# ============================================================================
def verify_auth(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    if x_api_key != API_KEY:
        raise HTTPException(401, "Invalid API Key")

# ============================================================================
# MODELS
# ============================================================================
class AssignJobRequest(BaseModel):
    work_order: int
    technician_id: int
    date: str  # YYYY-MM-DD
    start_time: Optional[str] = "09:00"

class OptimizeDayRequest(BaseModel):
    technician_id: int
    date: str

class BulkAssignRequest(BaseModel):
    mode: str  # "urgent" | "fill_capacity" | "monthly_spread"
    technician_ids: Optional[List[int]] = None
    target_utilization: Optional[float] = 0.8

# ============================================================================
# CORE ENDPOINTS
# ============================================================================

@app.get("/api/health")
def health():
    """Health check"""
    return {"status": "ok", "version": "2.0.0"}

# ----------------------------------------------------------------------------
# JOB POOL
# ----------------------------------------------------------------------------

@app.get("/api/jobs/unscheduled")
def get_unscheduled_jobs(
    region: Optional[str] = Query(None),
    priority: Optional[str] = Query(None),
    limit: int = Query(500, le=1000)
):
    """Get all unscheduled jobs with eligibility info"""
    
    # Get jobs not yet scheduled
    jobs = sb_select("job_pool", filters=[
        ("jp_status", "in", ["Call", "Waiting to Schedule"])
    ])
    
    if not jobs:
        return {"count": 0, "jobs": [], "summary": {}}
    
    # Apply filters
    if region:
        jobs = [j for j in jobs if j.get("site_state") == region]
    if priority:
        jobs = [j for j in jobs if j.get("jp_priority") == priority]
    
    jobs = jobs[:limit]
    
    # Add metadata
    for job in jobs:
        # Get eligible techs count
        elig = sb_select("job_technician_eligibility", filters=[
            ("work_order", "eq", job["work_order"])
        ])
        job["eligible_tech_count"] = len(elig)
        job["eligible_tech_ids"] = [e["technician_id"] for e in elig]
        
        # Calculate urgency
        due = pd.to_datetime(job.get("due_date"))
        days_left = (due - pd.Timestamp.now()).days
        job["days_until_due"] = days_left
        
        if days_left < 7:
            job["urgency"] = "critical"
        elif days_left < 14:
            job["urgency"] = "high"
        else:
            job["urgency"] = "normal"
    
    # Summary stats
    summary = {
        "total_jobs": len(jobs),
        "total_hours": sum(float(j.get("est_hours", 2)) for j in jobs),
        "by_priority": {},
        "by_region": {},
        "by_urgency": {}
    }
    
    for job in jobs:
        # Count by priority
        pri = job.get("jp_priority", "Unknown")
        summary["by_priority"][pri] = summary["by_priority"].get(pri, 0) + 1
        
        # Count by region
        reg = job.get("site_state", "Unknown")
        summary["by_region"][reg] = summary["by_region"].get(reg, 0) + 1
        
        # Count by urgency
        urg = job.get("urgency", "normal")
        summary["by_urgency"][urg] = summary["by_urgency"].get(urg, 0) + 1
    
    return {
        "count": len(jobs),
        "jobs": jobs,
        "summary": summary
    }

# ----------------------------------------------------------------------------
# TECHNICIANS
# ----------------------------------------------------------------------------

@app.get("/api/technicians/all")
def get_all_techs(active_only: bool = True):
    """Get all technicians with current workload"""
    
    filters = [("active", "eq", True)] if active_only else None
    techs = sb_select("technicians", filters=filters)
    
    # Get current week's schedule for each
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    
    for tech in techs:
        tech_id = tech["technician_id"]
        
        # Get scheduled jobs this week
        scheduled = sb_select("scheduled_jobs", filters=[
            ("technician_id", "eq", tech_id),
            ("date", "gte", str(week_start)),
            ("date", "lte", str(week_end))
        ])
        
        # Calculate hours
        total_hours = sum(float(j.get("est_hours", 2)) for j in scheduled)
        max_weekly = float(tech.get("max_weekly_hours", 40))
        
        tech["current_week_hours"] = total_hours
        tech["utilization_percent"] = round((total_hours / max_weekly) * 100, 1)
        tech["available_hours"] = max_weekly - total_hours
        tech["scheduled_job_count"] = len(scheduled)
    
    return {"count": len(techs), "technicians": techs}

# ----------------------------------------------------------------------------
# SCHEDULE OPERATIONS
# ----------------------------------------------------------------------------

@app.get("/api/schedule/week")
def get_tech_week_schedule(
    tech_id: int,
    week_start: str  # YYYY-MM-DD
):
    """Get a tech's schedule for one week"""
    
    week_start_date = datetime.fromisoformat(week_start).date()
    week_end_date = week_start_date + timedelta(days=6)
    
    scheduled = sb_select("scheduled_jobs", filters=[
        ("technician_id", "eq", tech_id),
        ("date", "gte", str(week_start_date)),
        ("date", "lte", str(week_end_date))
    ])
    
    # Organize by day
    by_day = {}
    for i in range(7):
        day = week_start_date + timedelta(days=i)
        by_day[str(day)] = {
            "date": str(day),
            "day_name": day.strftime("%A"),
            "jobs": [],
            "total_hours": 0.0
        }
    
    for job in scheduled:
        day_key = job["date"]
        if day_key in by_day:
            by_day[day_key]["jobs"].append(job)
            by_day[day_key]["total_hours"] += float(job.get("est_hours", 2))
    
    return {
        "tech_id": tech_id,
        "week_start": str(week_start_date),
        "days": list(by_day.values())
    }

@app.post("/api/schedule/assign")
def assign_single_job(req: AssignJobRequest):
    """Assign one job to a tech on a specific date"""
    
    # 1. Validate using existing endpoint logic
    from scheduler_api import schedule_validate, ValidateReq
    
    validation = schedule_validate(ValidateReq(
        work_order=req.work_order,
        technician_id=req.technician_id,
        date=datetime.fromisoformat(req.date).date()
    ))
    
    if not validation["ok"]:
        return {
            "success": False,
            "errors": validation["errors"],
            "warnings": validation["warnings"]
        }
    
    # 2. Get job details
    job = sb_select("job_pool", filters=[("work_order", "eq", req.work_order)])
    if not job:
        raise HTTPException(404, "Job not found")
    
    job = job[0]
    
    # 3. Insert into scheduled_jobs
    scheduled_row = {
        "work_order": req.work_order,
        "technician_id": req.technician_id,
        "date": req.date,
        "start_time": req.start_time,
        "est_hours": float(job.get("est_hours", 2)),
        "site_name": job.get("site_name"),
        "site_city": job.get("site_city"),
        "site_state": job.get("site_state"),
        "created_at": datetime.now().isoformat()
    }
    
    sb_insert("scheduled_jobs", [scheduled_row])
    
    # 4. Update job status
    sb_update("job_pool", {"work_order": req.work_order}, {"jp_status": "Scheduled"})
    
    return {
        "success": True,
        "assigned": scheduled_row,
        "warnings": validation.get("warnings", [])
    }

@app.delete("/api/schedule/remove/{work_order}")
def remove_job_from_schedule(work_order: int):
    """Remove a job from schedule"""
    
    from supabase_client import supabase_client
    sb = supabase_client()
    
    # Delete from scheduled_jobs
    sb.table("scheduled_jobs").delete().eq("work_order", work_order).execute()
    
    # Reset job status
    sb_update("job_pool", {"work_order": work_order}, {"jp_status": "Call"})
    
    return {"success": True, "work_order": work_order}

# ----------------------------------------------------------------------------
# SMART FEATURES
# ----------------------------------------------------------------------------

@app.get("/api/schedule/suggestions")
def get_smart_suggestions(
    tech_id: int,
    date: str,
    radius_miles: int = 50
):
    """Get smart job suggestions for a tech on a specific day"""
    
    # Get tech info
    tech = sb_select("technicians", filters=[("technician_id", "eq", tech_id)])
    if not tech:
        raise HTTPException(404, "Tech not found")
    tech = tech[0]
    
    # Get jobs already scheduled that day
    scheduled = sb_select("scheduled_jobs", filters=[
        ("technician_id", "eq", tech_id),
        ("date", "eq", date)
    ])
    
    if not scheduled:
        return {"suggestions": [], "reason": "No jobs scheduled this day yet"}
    
    # Get unscheduled jobs
    unscheduled = sb_select("job_pool", filters=[
        ("jp_status", "in", ["Call", "Waiting to Schedule"])
    ])
    
    suggestions = []
    
    # Find jobs near scheduled jobs
    from scheduler_V4a_fixed import haversine
    
    for sch in scheduled:
        sch_job = sb_select("job_pool", filters=[("work_order", "eq", sch["work_order"])])
        if not sch_job:
            continue
        sch_job = sch_job[0]
        
        for unsched in unscheduled:
            # Check eligibility
            elig = sb_select("job_technician_eligibility", filters=[
                ("work_order", "eq", unsched["work_order"]),
                ("technician_id", "eq", tech_id)
            ])
            if not elig:
                continue
            
            # Calculate distance
            distance = haversine(
                sch_job["latitude"], sch_job["longitude"],
                unsched["latitude"], unsched["longitude"]
            )
            
            if distance <= radius_miles:
                suggestions.append({
                    "work_order": unsched["work_order"],
                    "site_name": unsched["site_name"],
                    "distance_miles": round(distance, 1),
                    "est_hours": unsched.get("est_hours", 2),
                    "reason": f"Only {distance:.1f} miles from WO {sch['work_order']}",
                    "priority": unsched.get("jp_priority")
                })
    
    # Sort by distance
    suggestions.sort(key=lambda x: x["distance_miles"])
    
    return {"suggestions": suggestions[:10]}

@app.post("/api/schedule/optimize-day")
def optimize_day_route(req: OptimizeDayRequest):
    """Reorder jobs on a day to minimize drive time"""
    
    # Get jobs for that day
    jobs = sb_select("scheduled_jobs", filters=[
        ("technician_id", "eq", req.technician_id),
        ("date", "eq", req.date)
    ])
    
    if len(jobs) < 2:
        return {"optimized": False, "reason": "Need at least 2 jobs to optimize"}
    
    # Get tech home location
    tech = sb_select("technicians", filters=[("technician_id", "eq", req.technician_id)])[0]
    
    # Simple nearest-neighbor optimization
    from scheduler_V4a_fixed import haversine
    
    # Get job details with locations
    job_details = []
    for j in jobs:
        jp = sb_select("job_pool", filters=[("work_order", "eq", j["work_order"])])[0]
        job_details.append({
            **j,
            "latitude": jp["latitude"],
            "longitude": jp["longitude"]
        })
    
    # Start from home
    current_lat = tech["home_latitude"]
    current_lon = tech["home_longitude"]
    
    optimized_order = []
    remaining = job_details.copy()
    
    while remaining:
        # Find nearest job
        nearest = min(remaining, key=lambda j: haversine(
            current_lat, current_lon, j["latitude"], j["longitude"]
        ))
        optimized_order.append(nearest)
        remaining.remove(nearest)
        current_lat = nearest["latitude"]
        current_lon = nearest["longitude"]
    
    # Calculate savings
    original_distance = sum(
        haversine(job_details[i]["latitude"], job_details[i]["longitude"],
                  job_details[i+1]["latitude"], job_details[i+1]["longitude"])
        for i in range(len(job_details) - 1)
    )
    
    optimized_distance = sum(
        haversine(optimized_order[i]["latitude"], optimized_order[i]["longitude"],
                  optimized_order[i+1]["latitude"], optimized_order[i+1]["longitude"])
        for i in range(len(optimized_order) - 1)
    )
    
    savings = original_distance - optimized_distance
    
    return {
        "optimized": True,
        "original_distance_miles": round(original_distance, 1),
        "optimized_distance_miles": round(optimized_distance, 1),
        "savings_miles": round(savings, 1),
        "optimized_order": [j["work_order"] for j in optimized_order]
    }

# ----------------------------------------------------------------------------
# BULK OPERATIONS
# ----------------------------------------------------------------------------

@app.post("/api/schedule/bulk-assign")
def bulk_assign_jobs(req: BulkAssignRequest):
    """Bulk assign jobs based on mode"""
    
    results = {"assigned": 0, "failed": 0, "details": []}
    
    if req.mode == "urgent":
        # Assign all urgent/NOV jobs
        urgent = sb_select("job_pool", filters=[
            ("jp_status", "in", ["Call"]),
            ("jp_priority", "in", ["NOV", "Urgent"])
        ])
        
        for job in urgent:
            # Find best tech using your existing scheduler
            # (Simplified - would use full scheduler logic)
            elig = sb_select("job_technician_eligibility", filters=[
                ("work_order", "eq", job["work_order"])
            ])
            
            if elig:
                tech_id = elig[0]["technician_id"]
                due_date = str(job["due_date"])
                
                try:
                    assign_single_job(AssignJobRequest(
                        work_order=job["work_order"],
                        technician_id=tech_id,
                        date=due_date
                    ))
                    results["assigned"] += 1
                    results["details"].append(f"✓ WO {job['work_order']} → Tech {tech_id}")
                except Exception as e:
                    results["failed"] += 1
                    results["details"].append(f"✗ WO {job['work_order']}: {str(e)}")
    
    return results

# ============================================================================
# RUN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
