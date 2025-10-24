# scheduler_api_unified.py - CLEAN UNIFIED API
import os
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import scheduler_v5_geographic as sched_v5

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
app = FastAPI(title="Unified Scheduler API", version="2.1.0")

# Serve frontend


@app.get("/")
def serve_app():
    return {
        "message": "Scheduler API Running", 
        "status": "healthy",
        "docs": f"{BASE_URL}/docs",
        "endpoints": {
            "technicians": f"{BASE_URL}/api/technicians/all",
            "jobs": f"{BASE_URL}/api/jobs/unscheduled",
            "schedule": f"{BASE_URL}/api/schedule/week"
        }
    }

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
# ----------------------------------------------------------------------------
# REQUEST/RESPONSE MODELS
# ----------------------------------------------------------------------------

class ScheduleWeekRequest(BaseModel):
    """Request to schedule a week using geographic-first approach"""
    tech_ids: List[int]  # Can be multiple techs (we'll loop through them)
    region_names: List[str]  # User-selected regions to focus on
    week_start: str  # YYYY-MM-DD format
    sow_filter: Optional[str] = None  # Filter by SOW (e.g., "NT")
    target_weekly_hours: int = 40

class ScheduleWeekResponse(BaseModel):
    """Response with full week schedule"""
    success: bool
    tech_schedules: List[dict]  # One per tech
    total_jobs_scheduled: int
    total_hours_scheduled: float
    warnings: List[str]
    suggestions: List[str]

frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/static", StaticFiles(directory=frontend_path), name="static")

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
# NEW ENDPOINT
# ----------------------------------------------------------------------------

@app.post("/api/schedule/generate-week-smart")
def generate_week_smart_schedule(
    req: ScheduleWeekRequest,
 ):
    """
    🆕 SMART SCHEDULER - Geographic-First Approach
    
    This endpoint:
    1. Analyzes regions for job distribution
    2. Picks best region to focus on
    3. Schedules ALL jobs in that region (no wasted trips)
    4. Optimizes routes within region
    5. Calculates drive times and hotel stays
    6. Suggests nearby regions if capacity not filled
    
    Example request:
    {
        "tech_ids": [7],
        "region_names": ["CO_Denver_Metro", "CO_NoCo"],
        "week_start": "2025-09-08",
        "sow_filter": "NT",
        "target_weekly_hours": 40
    }
    """
    # Verify auth
   
    
    try:
        # Parse week start date
        week_start = datetime.fromisoformat(req.week_start).date()
        
        # Schedule for each tech
        tech_schedules = []
        total_jobs = 0
        total_hours = 0
        all_warnings = []
        all_suggestions = []
        
        for tech_id in req.tech_ids:
            print(f"\n{'='*80}")
            print(f"Scheduling Tech {tech_id}")
            print(f"{'='*80}")
            
            result = sched_v5.schedule_week_geographic(
                tech_id=tech_id,
                region_names=req.region_names,
                week_start=week_start,
                sow_filter=req.sow_filter,
                target_weekly_hours=req.target_weekly_hours
            )
            
            if "error" in result:
                all_warnings.append(f"Tech {tech_id}: {result['error']}")
                continue
            
            tech_schedules.append(result)
            total_jobs += result.get('jobs_scheduled', 0)
            total_hours += result.get('total_hours', 0)
            all_warnings.extend(result.get('warnings', []))
            all_suggestions.extend(result.get('suggestions', []))
        
        return {
            "success": True,
            "tech_schedules": tech_schedules,
            "total_jobs_scheduled": total_jobs,
            "total_hours_scheduled": round(total_hours, 2),
            "warnings": all_warnings,
            "suggestions": all_suggestions
        }
        
    except Exception as e:
        import traceback
        print(f"ERROR: {str(e)}")
        print(traceback.format_exc())
        
        return {
            "success": False,
            "error": str(e),
            "tech_schedules": [],
            "total_jobs_scheduled": 0,
            "total_hours_scheduled": 0,
            "warnings": [str(e)],
            "suggestions": []
        }
@app.post("/api/schedule/save")
def save_schedule_to_database(
    schedule_data: dict,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    """
    Save a generated schedule to the database
    
    Expects:
    {
        "tech_schedule": {...},  # The full schedule from generate-week-smart
        "week_start": "2025-10-20"
    }
    """
    verify_auth(x_api_key)
    
    try:
        tech_schedule = schedule_data.get('tech_schedule')
        week_start = schedule_data.get('week_start')
        
        if not tech_schedule or not week_start:
            return {
                "success": False,
                "error": "Missing required fields: tech_schedule, week_start"
            }
        
        tech_id = tech_schedule['tech_id']
        tech_name = tech_schedule['tech_name']
        
        saved_jobs = []
        work_orders_scheduled = []
        
        # Process each day
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
        for day in days:
            day_schedule = tech_schedule['schedule'].get(day, {})
            day_jobs = day_schedule.get('jobs', [])
            
            for job in day_jobs:
                work_order = job['work_order']
                
                # Get job details from job_pool
                job_details = sb_select("job_pool", filters=[
                    ("work_order", "eq", work_order)
                ])
                
                if not job_details:
                    continue
                
                job_detail = job_details[0]
                
                # Create scheduled_jobs record
                scheduled_record = {
                    "work_order": work_order,
                    "technician_id": tech_id,
                    "assigned_tech_name": tech_name,
                    "date": day_schedule['date'],
                    "site_name": job_detail.get('site_name'),
                    "site_city": job_detail.get('site_city'),
                    "site_state": job_detail.get('site_state'),
                    "site_id": job_detail.get('site_id'),
                    "duration": float(job['est_hours']),
                    "sow_1": job['sow'],
                    "due_date": job_detail.get('due_date'),
                    "is_night_job": job_detail.get('night_test', False)
                }
                
                saved_jobs.append(scheduled_record)
                work_orders_scheduled.append(work_order)
        
        if not saved_jobs:
            return {
                "success": False,
                "error": "No jobs to save"
            }
        
        # Save to database
        sb_insert("scheduled_jobs", saved_jobs)
        
        # Update job_pool status
        for wo in work_orders_scheduled:
            sb_update("job_pool", {"work_order": wo}, {"jp_status": "Scheduled"})
        
        return {
            "success": True,
            "jobs_saved": len(saved_jobs),
            "work_orders": work_orders_scheduled,
            "tech_id": tech_id,
            "tech_name": tech_name,
            "week_start": week_start
        }
        
    except Exception as e:
        import traceback
        print(f"ERROR saving schedule: {str(e)}")
        print(traceback.format_exc())
        
        return {
            "success": False,
            "error": str(e)
        }
# ----------------------------------------------------------------------------
# HELPER ENDPOINT: Preview Region Analysis
# ----------------------------------------------------------------------------

@app.get("/api/schedule/analyze-regions")
def analyze_regions_preview(
    tech_id: int,
    month_year: str,  # Format: "2025-09"
    sow_filter: Optional[str] = None,

):
    """
    Preview which regions have jobs for a tech in a given month
    
    Helps user decide which regions to include in schedule request
    
    Example: GET /api/schedule/analyze-regions?tech_id=7&month_year=2025-09
    """
    
    
    try:
        # Parse month
        year, month = map(int, month_year.split('-'))
        month_start = date(year, month, 1)
        
        if month == 12:
            month_end = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(year, month + 1, 1) - timedelta(days=1)
        
        # Get region analysis
        regions = sched_v5.analyze_regions_for_tech(
            tech_id=tech_id,
            month_start=month_start,
            month_end=month_end,
            sow_filter=sow_filter
        )
        
        return {
            "success": True,
            "tech_id": tech_id,
            "month": month_year,
            "regions": regions
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "regions": []
        }

# ----------------------------------------------------------------------------
# TECHNICIANS
# ----------------------------------------------------------------------------

@app.get("/api/technicians/all")
def get_all_techs(active_only: bool = True):
    """Get all technicians with current workload"""
    
    filters = [("active", "eq", True)] if active_only else None
    techs = sb_select("technicians", filters=filters)
    
    # Handle empty results
    if not techs:
        return {"count": 0, "technicians": []}
    
    # Fix field name compatibility
    for tech in techs:
        # Handle both 'id' and 'technician_id'
        if "id" in tech and "technician_id" not in tech:
            tech["technician_id"] = tech["id"]
        # Handle home location field names
        if "home_lat" in tech and "home_latitude" not in tech:
            tech["home_latitude"] = tech["home_lat"]
        if "home_lng" in tech and "home_longitude" not in tech:
            tech["home_longitude"] = tech["home_lng"]
    
    # Get current week's schedule for each
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    
    for tech in techs:
        tech_id = tech.get("technician_id") or tech.get("id")
        if not tech_id:
            continue
        
        # Get scheduled jobs this week
        try:
            scheduled = sb_select("scheduled_jobs", filters=[
                ("technician_id", "eq", tech_id),
                ("date", "gte", str(week_start)),
                ("date", "lte", str(week_end))
            ])
        except Exception as e:
            print(f"Error loading schedule for tech {tech_id}: {e}")
            scheduled = []
        
        # Calculate hours
        total_hours = sum(float(j.get("est_hours", 2)) for j in scheduled)
        max_weekly = float(tech.get("max_weekly_hours", 40))
        
        tech["current_week_hours"] = round(total_hours, 1)
        tech["utilization_percent"] = round((total_hours / max_weekly) * 100, 1)
        tech["available_hours"] = round(max_weekly - total_hours, 1)
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
    
    # 1. Get job details
    job = sb_select("job_pool", filters=[("work_order", "eq", req.work_order)])
    if not job:
        return {"success": False, "errors": ["Job not found"]}
    
    job = job[0]
    
    # 2. Check tech eligibility
    elig = sb_select("job_technician_eligibility", filters=[
        ("work_order", "eq", req.work_order),
        ("technician_id", "eq", req.technician_id)
    ])
    
    if not elig:
        return {
            "success": False, 
            "errors": [f"Tech {req.technician_id} is not eligible for job {req.work_order}"]
        }
    
    # 3. Check if already scheduled
    existing = sb_select("scheduled_jobs", filters=[
        ("work_order", "eq", req.work_order)
    ])
    
    if existing:
        return {
            "success": False,
            "errors": [f"Job {req.work_order} is already scheduled"]
        }
    
    # 4. Insert into scheduled_jobs
    scheduled_row = {
        "work_order": req.work_order,
        "technician_id": req.technician_id,
        "date": req.date,
        "start_time": req.start_time if hasattr(req, 'start_time') and req.start_time else None,
        "duration": float(job.get("est_hours", 2)),
        "site_name": job.get("site_name"),
        "site_city": job.get("site_city"),
        "site_state": job.get("site_state"),
        "created_at": datetime.now().isoformat()
    }
    
    try:
        sb_insert("scheduled_jobs", [scheduled_row])
        
        # 5. Update job status
        sb_update("job_pool", {"work_order": req.work_order}, {"jp_status": "Scheduled"})
        
        return {
            "success": True,
            "assigned": scheduled_row,
            "message": f"Job {req.work_order} assigned to Tech {req.technician_id} on {req.date}"
        }
    except Exception as e:
        return {
            "success": False,
            "errors": [f"Failed to assign job: {str(e)}"]
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

@app.get("/api/analysis/monthly")
def monthly_analysis(year: int, month: int):
    """Monthly planning analysis with regional breakdown and drive time estimates"""
    
    from datetime import date, timedelta
    from collections import defaultdict
    
    try:
        # Calculate month boundaries
        month_start = date(year, month, 1)
        if month == 12:
            month_end = date(year + 1, 1, 1)
        else:
            month_end = date(year, month + 1, 1)
        
        # Get all jobs for the month
        jobs = sb_select("job_pool", filters=[
            ("due_date", "gte", str(month_start)),
            ("due_date", "lt", str(month_end)),
            ("jp_status", "in", ["Call", "Waiting to Schedule"])
        ])
        
        if not jobs:
            return {
                "summary": {
                    "total_jobs": 0,
                    "total_work_hours": 0,
                    "total_drive_hours": 0,
                    "total_hours": 0,
                    "tech_count": 0,
                    "total_tech_capacity": 0,
                    "utilization_percent": 0
                },
                "regional_breakdown": [],
                "weekly_breakdown": [],
                "problem_jobs": {"remote_locations": [], "limited_eligibility": []}
            }
        
        # Get techs and regions
        techs = sb_select("technicians", filters=[("active", "eq", True)])
        regions = sb_select("regions")
        
        # Create region lookup
        region_lookup = {}
        for region in regions:
            region_lookup[region['region_name']] = {
                'center_lat': region.get('center_latitude'),
                'center_lng': region.get('center_longitude')
            }
        
        # Helper function for distance calculation
        def haversine(lat1, lon1, lat2, lon2):
            from math import radians, sin, cos, sqrt, atan2
            R = 3958.8  # Earth radius in miles
            lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            return R * c
        
        # Calculate total work hours
        total_work_hours = sum(float(j.get("est_hours", 2)) for j in jobs)
        
        # Regional breakdown
        regional_stats = defaultdict(lambda: {
            'jobs': 0,
            'work_hours': 0,
            'job_list': []
        })
        
        for job in jobs:
            region = job.get('region', 'Unknown')
            regional_stats[region]['jobs'] += 1
            regional_stats[region]['work_hours'] += float(job.get('est_hours', 2))
            regional_stats[region]['job_list'].append(job)
        
        # Estimate drive time by region
        # Simple estimation: 30 miles average between jobs in same region
        # Plus distance from nearest tech home to region center
        AVG_SPEED = 55  # mph
        AVG_INTRA_REGION_DISTANCE = 30  # miles between jobs in same region
        
        total_drive_hours = 0
        regional_breakdown = []
        
        for region_name, stats in regional_stats.items():
            job_count = stats['jobs']
            work_hours = stats['work_hours']
            
            # Estimate drive time for this region
            # Formula: (jobs - 1) × avg_distance_between_jobs + 2 × home_to_region
            if job_count > 0:
                # Intra-region driving (between jobs)
                intra_region_miles = (job_count - 1) * AVG_INTRA_REGION_DISTANCE if job_count > 1 else 0
                
                # Find nearest tech to this region
                min_home_distance = 999999
                if region_name in region_lookup and region_lookup[region_name]['center_lat']:
                    region_center = region_lookup[region_name]
                    for tech in techs:
                        if tech.get('home_latitude') and region_center['center_lat']:
                            dist = haversine(
                                tech['home_latitude'], tech['home_longitude'],
                                region_center['center_lat'], region_center['center_lng']
                            )
                            min_home_distance = min(min_home_distance, dist)
                else:
                    min_home_distance = 50  # Default assumption if no coordinates
                
                # Home to region and back (assuming tech returns home each day)
                # For weekly planning, assume they go out once per day on average
                days_in_region = max(1, job_count // 3)  # Assume ~3 jobs per day
                home_to_region_miles = min_home_distance * 2 * days_in_region
                
                total_region_miles = intra_region_miles + home_to_region_miles
                region_drive_hours = total_region_miles / AVG_SPEED
            else:
                region_drive_hours = 0
            
            total_drive_hours += region_drive_hours
            
            regional_breakdown.append({
                'region': region_name,
                'jobs': job_count,
                'work_hours': round(work_hours, 1),
                'drive_hours': round(region_drive_hours, 1),
                'total_hours': round(work_hours + region_drive_hours, 1)
            })
        
        # Sort by total hours descending
        regional_breakdown.sort(key=lambda x: x['total_hours'], reverse=True)
        
        # Weekly breakdown
        weekly_stats = defaultdict(lambda: {
            'jobs': 0,
            'work_hours': 0,
            'urgent': 0,
            'monthly': 0,
            'annual': 0,
            'other': 0
        })
        
        for job in jobs:
            due = date.fromisoformat(str(job['due_date']))
            week_num = ((due - month_start).days // 7) + 1
            if week_num > 4:
                week_num = 4
            
            week_key = f"week_{week_num}"
            weekly_stats[week_key]['jobs'] += 1
            weekly_stats[week_key]['work_hours'] += float(job.get('est_hours', 2))
            
            # Categorize by priority
            priority = job.get('jp_priority', '')
            if priority in ['NOV', 'Urgent']:
                weekly_stats[week_key]['urgent'] += 1
            elif 'Monthly' in priority:
                weekly_stats[week_key]['monthly'] += 1
            elif 'Annual' in priority or 'Year' in priority:
                weekly_stats[week_key]['annual'] += 1
            else:
                weekly_stats[week_key]['other'] += 1
        
        # Estimate drive time per week (proportional to job distribution)
        weekly_breakdown = []
        for i in range(1, 5):
            week_key = f"week_{i}"
            week_data = weekly_stats[week_key]
            
            # Proportional drive time based on job percentage
            week_job_percent = week_data['jobs'] / len(jobs) if len(jobs) > 0 else 0
            week_drive_hours = total_drive_hours * week_job_percent
            
            # Calculate week date range
            week_start = month_start + timedelta(days=(i-1)*7)
            week_end = min(week_start + timedelta(days=6), month_end - timedelta(days=1))
            
            weekly_breakdown.append({
                'week': i,
                'date_range': f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d')}",
                'jobs': week_data['jobs'],
                'work_hours': round(week_data['work_hours'], 1),
                'drive_hours': round(week_drive_hours, 1),
                'total_hours': round(week_data['work_hours'] + week_drive_hours, 1),
                'urgent': week_data['urgent'],
                'monthly': week_data['monthly'],
                'annual': week_data['annual'],
                'other': week_data['other']
            })
        
        # Find problem jobs
        problem_jobs = {"remote_locations": [], "limited_eligibility": []}
        
        REMOTE_THRESHOLD = 300  # miles
        
        for job in jobs:
            # Check eligibility
            elig = sb_select("job_technician_eligibility", filters=[
                ("work_order", "eq", job["work_order"])
            ])
            
            if len(elig) <= 2:
                problem_jobs["limited_eligibility"].append({
                    "work_order": job["work_order"],
                    "site_name": job.get("site_name"),
                    "region": job.get("region", "Unknown"),
                    "est_hours": job.get("est_hours", 2),
                    "eligible_techs": len(elig),
                    "tech_names": [e.get("technician_name", "Unknown") for e in elig]
                })
            
            # Check if remote (>150 miles from any tech)
            if job.get('latitude') and job.get('longitude'):
                min_distance = 999999
                closest_tech = None
                
                for tech in techs:
                    if tech.get('home_latitude') and tech.get('home_longitude'):
                        dist = haversine(
                            tech['home_latitude'], tech['home_longitude'],
                            job['latitude'], job['longitude']
                        )
                        if dist < min_distance:
                            min_distance = dist
                            closest_tech = tech['name']
                
                if min_distance > REMOTE_THRESHOLD:
                    problem_jobs["remote_locations"].append({
                        "work_order": job["work_order"],
                        "site_name": job.get("site_name"),
                        "region": job.get("region", "Unknown"),
                        "est_hours": job.get("est_hours", 2),
                        "distance_from_nearest": round(min_distance, 1),
                        "nearest_tech": closest_tech
                    })
        
        # Calculate tech capacity
        weeks_in_month = 4
        tech_capacity = sum(float(t.get("max_weekly_hours", 40)) * weeks_in_month for t in techs)
        total_hours = total_work_hours + total_drive_hours
        utilization = (total_hours / tech_capacity * 100) if tech_capacity > 0 else 0
        
        return {
            "summary": {
                "total_jobs": len(jobs),
                "total_work_hours": round(total_work_hours, 1),
                "total_drive_hours": round(total_drive_hours, 1),
                "total_hours": round(total_hours, 1),
                "tech_count": len(techs),
                "total_tech_capacity": round(tech_capacity, 1),
                "utilization_percent": round(utilization, 1),
                "is_manageable": utilization <= 90,
                "remote_jobs_count": len(problem_jobs["remote_locations"]),
                "limited_eligibility_count": len(problem_jobs["limited_eligibility"])
            },
            "regional_breakdown": regional_breakdown,
            "weekly_breakdown": weekly_breakdown,
            "problem_jobs": problem_jobs
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "summary": {
                "total_jobs": 0,
                "total_work_hours": 0,
                "total_drive_hours": 0,
                "total_hours": 0,
                "tech_count": 0,
                "total_tech_capacity": 0,
                "utilization_percent": 0
            },
            "regional_breakdown": [],
            "weekly_breakdown": [],
            "problem_jobs": {"remote_locations": [], "limited_eligibility": []}
        }
# ============================================================================
# RUN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
