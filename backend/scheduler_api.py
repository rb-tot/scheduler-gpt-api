# scheduler_api_unified.py - CLEAN UNIFIED API
import os
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Header, HTTPException, UploadFile, File, Query
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from scheduler_fillin import schedule_week_fillin
import pandas as pd
import io
import scheduler_v5_geographic as sched_v5

# Environment setup
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("ACTIONS_API_KEY", "devkey123")
BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://localhost:8000")

# Import your existing modules
try:
    from supabase_client import sb_select, sb_insert, sb_update, supabase_client
    from db_queries import job_pool_df as _jp, technicians_df as _techs
    
except ImportError:
    print("Missing dependencies - install: supabase, pandas")

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
    start_time: Optional[str] = None

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

frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")

# ============================================================================
# ADDITIONAL TECHNICIANS ENDPOINTS
# ============================================================================

class AddAdditionalTechRequest(BaseModel):
    work_order: int
    technician_id: int

@app.post("/api/scheduled-jobs/add-additional-tech")
def add_additional_tech(request: AddAdditionalTechRequest):
    """Add an additional technician to a scheduled job"""
    try:
        # Check if job exists
        job = sb_select("scheduled_jobs", filters=[
            ("work_order", "eq", request.work_order)
        ])
        
        if not job:
            raise HTTPException(404, f"Work order {request.work_order} not found")
        
        # Don't allow adding the primary tech as additional
        if job[0]['technician_id'] == request.technician_id:
            raise HTTPException(400, "Cannot add primary tech as additional tech")
        
        # Check if already exists
        existing = sb_select("scheduled_job_additional_techs", filters=[
            ("work_order", "eq", request.work_order),
            ("technician_id", "eq", request.technician_id)
        ])
        
        if existing:
            raise HTTPException(400, "Tech already added to this job")
        
        # Insert additional tech
        sb_insert("scheduled_job_additional_techs", {
            "work_order": request.work_order,
            "technician_id": request.technician_id
        })
        
        return {
            "success": True,
            "message": f"Added tech {request.technician_id} to work order {request.work_order}",
            "primary_tech": job[0]['assigned_tech_name']
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f" Error adding additional tech: {e}")
        raise HTTPException(500, str(e))

@app.delete("/api/scheduled-jobs/remove-additional-tech")
def remove_additional_tech(work_order: int, technician_id: int):
    """Remove an additional technician from a scheduled job"""
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        result = sb.table("scheduled_job_additional_techs")\
            .delete()\
            .eq("work_order", work_order)\
            .eq("technician_id", technician_id)\
            .execute()
        
        return {
            "success": True,
            "message": f"Removed tech {technician_id} from work order {work_order}"
        }
    except Exception as e:
        print(f" Error removing additional tech: {e}")
        raise HTTPException(500, str(e))

@app.get("/api/scheduled-jobs/additional-techs")
def get_all_additional_techs(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get all additional tech assignments (for export or display)"""
    try:
        # Get additional tech records
        addl_techs = sb_select("scheduled_job_additional_techs")
        
        if not addl_techs:
            return {
                "success": True,
                "additional_techs": []
            }
        
        # Get related scheduled jobs and tech names
        result = []
        for addl in addl_techs:
            # Get the scheduled job details
            job = sb_select("scheduled_jobs", filters=[
                ("work_order", "eq", addl['work_order'])
            ])
            
            if not job:
                continue
                
            job = job[0]
            
            # Apply date filters if provided
            if start_date and job['date'] < start_date:
                continue
            if end_date and job['date'] > end_date:
                continue
            
            # Get additional tech name
            tech = sb_select("technicians", filters=[
                ("technician_id", "eq", addl['technician_id'])
            ])
            
            if not tech:
                continue
                
            result.append({
                "work_order": addl['work_order'],
                "technician_id": addl['technician_id'],
                "date": job['date'],
                "site_name": job.get('site_name'),
                "duration": job.get('duration'),
                "primary_tech": job.get('assigned_tech_name'),
                "additional_tech_name": tech[0]['name']
            })
        
        return {
            "success": True,
            "additional_techs": result
        }
    except Exception as e:
        print(f"Error getting additional techs: {e}")
        return {
            "success": False,
            "additional_techs": [],
            "error": str(e)
        }

@app.get("/api/scheduled-jobs/{work_order}/additional-techs")
def get_additional_techs_for_job(work_order: int):
    """Get all additional techs for a specific job"""
    try:
        addl_techs = sb_select("scheduled_job_additional_techs", filters=[
            ("work_order", "eq", work_order)
        ])
        
        if not addl_techs:
            return {
                "success": True,
                "work_order": work_order,
                "additional_techs": []
            }
        
        result = []
        for addl in addl_techs:
            tech = sb_select("technicians", filters=[
                ("technician_id", "eq", addl['technician_id'])
            ])
            if tech:
                result.append({
                    "technician_id": addl['technician_id'],
                    "name": tech[0]['name']
                })
        
        return {
            "success": True,
            "work_order": work_order,
            "additional_techs": result
        }
    except Exception as e:
        print(f" Error getting additional techs for job: {e}")
        raise HTTPException(500, str(e))

@app.get("/tech-manager", response_class=HTMLResponse)
def serve_tech_manager():
    """Serve the technician manager page"""
    html_path = os.path.join(frontend_dir, "tech-manager.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            content = f.read()
            # Add cache-busting timestamp
            import time
            cache_buster = f"<!-- Cache: {time.time()} -->"
            content = content.replace("<body>", f"<body>{cache_buster}")
            return content
    raise HTTPException(404, "tech-manager.html not found in frontend directory")

@app.get("/analysis", response_class=HTMLResponse)
def serve_analysis():
    """Serve the analysis page"""
    html_path = os.path.join(frontend_dir, "analysis.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    raise HTTPException(404, "analysis.html not found")

@app.get("/schedule-dashboard", response_class=HTMLResponse)
def serve_schedule_dashboard():
    """Serve the schedule dashboard page"""
    html_path = os.path.join(frontend_dir, "schedule-dashboard.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    raise HTTPException(404, "schedule-dashboard.html not found")

@app.get("/data-manager", response_class=HTMLResponse)
def serve_data_manager():
    """Serve the data manager page"""
    html_path = os.path.join(frontend_dir, "data-manager.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    raise HTTPException(404, "data-manager.html not found")

@app.get("/schedule-review-dashboard", response_class=HTMLResponse)
def serve_schedule_review_dashboard():
    """Redirect old dashboard URL to scheduler-helper"""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/scheduler-helper")

@app.get("/", response_class=HTMLResponse)
def redirect_to_main():
    """Redirect root to scheduler-helper (main page)"""
    html_path = os.path.join(frontend_dir, "scheduler-helper.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    return {"message": "SchedulerGPT", "main_page": "/scheduler-helper"}

@app.get("/ai-scheduler", response_class=HTMLResponse)
def serve_ai_scheduler():
    """Redirect old AI scheduler URL to scheduler-helper"""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/scheduler-helper")

@app.get("/scheduler-helper", response_class=HTMLResponse)
def serve_scheduler_helper():
    """Serve the scheduler helper page (MAIN PAGE)"""
    html_path = os.path.join(frontend_dir, "scheduler-helper.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    raise HTTPException(404, "scheduler-helper.html not found")


class TechnicianModel(BaseModel):
    technician_id: int
    name: str
    home_location: str
    home_latitude: float
    home_longitude: float
    qualified_tests: str  # Comma-separated: "PDT,PVCT,UST"
    states_allowed: str   # Comma-separated: "CO,WY,UT"
    states_excluded: Optional[str] = None
    max_weekly_hours: int = 40
    max_daily_hours: int = 10
    active: bool = True

class ToggleActiveRequest(BaseModel):
    technician_id: int
    active: bool

class TimeOffEntry(BaseModel):
    technician_id: int
    date: str  # YYYY-MM-DD
    hours_per_day: float  # 0 for full day off, 4 for partial, 8 for available
    reason: Optional[str] = None

class SaveTimeOffRequest(BaseModel):
    time_off: List[TimeOffEntry]

@app.get("/current-schedule", response_class=HTMLResponse)
def serve_current_schedule():
    """Serve the current schedule view page"""
    html_path = os.path.join(frontend_dir, "current-schedule.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    raise HTTPException(404, "current-schedule.html not found")
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
    limit: int = Query(1000, le=2000),
    weeks_ahead: int = Query(4),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """Get all unscheduled jobs with eligibility info"""
    
    from datetime import datetime, timedelta
    # ADD THIS DEBUG BLOCK ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬Å“
    print(f"\ DEBUG get_unscheduled_jobs:")
    print(f"  start_date received: {start_date}")
    print(f"  end_date received: {end_date}")
    
    # Build filters list
    filters = [("jp_status", "in", ["Call", "Waiting to Schedule"])]
    
    # Add date filters if provided
    if start_date:
        filters.append(("due_date", "gte", start_date))
        print(f"   Added start filter: due_date >= {start_date}")
    if end_date:
        # Use < next day instead of <= end day for reliable date filtering
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        next_day = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
        filters.append(("due_date", "lt", next_day))
        print(f"   Added end filter: due_date < {next_day}")
    print(f"  Final filters: {filters}")
    # Get jobs with filters - THIS MUST EXECUTE
    jobs = sb_select("job_pool", filters=filters)
    print(f"   Jobs returned: {len(jobs)}")  # ADD THIS TOO
    # Check if we got any jobs
    if not jobs:
        return {"count": 0, "jobs": [], "summary": {}}
    
    # Apply additional filters
    if region:
        jobs = [j for j in jobs if j.get("site_state") == region]
    if priority:
        jobs = [j for j in jobs if j.get("jp_priority") == priority]
    print(f"   Jobs after region/priority filter: {len(jobs)}")
    # Apply limit
    jobs = jobs[:limit]
    print(f"   Jobs after limit ({limit}): {len(jobs)}")
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
        "total_hours": sum(float(j.get("duration", 2)) for j in jobs),
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
    print(f"   Returning {len(jobs)} jobs to frontend\n")
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
        # Check for existing scheduled jobs
    existing_jobs = sb_select("scheduled_jobs", filters=[
        ("technician_id", "in", req.tech_ids),
        ("date", "gte", req.week_start),
        ("date", "lte", (datetime.fromisoformat(req.week_start) + timedelta(days=4)).isoformat())
    ])

    if existing_jobs:
        logger.warning(f"Found {len(existing_jobs)} existing jobs for techs {req.tech_ids} in week {req.week_start}")
        # Either exclude these dates/times or return a warning
    
    
    
    """
    ÃƒÂ°Ã…Â¸Ã¢â‚¬Â Ã¢â‚¬Â¢ SMART SCHEDULER - Geographic-First Approach
    
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
                    "start_time": f"{day_schedule['date']}T{job.get('start_time')}:00" if job.get('start_time') else None,
                    "end_time": None,  # Add this line too if you want to calculate end_time
                    "site_name": job_detail.get('site_name'),
                    "site_city": job_detail.get('site_city'),
                    "site_state": job_detail.get('site_state'),
                    "site_id": job_detail.get('site_id'),
                    "duration": float(job['duration']),
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
            by_day[day_key]["total_hours"] += float(job.get("duration", 2))
    
    return {
        "tech_id": tech_id,
        "week_start": str(week_start_date),
        "days": list(by_day.values())
    }


@app.get("/api/regions/analyze")
def analyze_regions_for_tech(
    tech_id: int,
    month_start: str,
    month_end: str
):
    """Get regions with job counts for a tech"""
    sb = supabase_client()
    
    result = sb.rpc(
        'analyze_regions_for_tech',
        {
            'p_tech_id': tech_id,
            'p_month_start': month_start,
            'p_month_end': month_end,
            'p_sow_filter': None
        }
    ).execute()
    
    return {
        "regions": result.data or []
    }


@app.get("/api/jobs/region")
def get_jobs_in_region(
    tech_id: int,
    region: str,
    month_start: str,
    month_end: str
):
    """Get all jobs in a region for a tech"""
    sb = supabase_client()
    
    result = sb.rpc(
        'get_all_jobs_in_region',
        {
            'p_tech_id': tech_id,
            'p_region_name': region,
            'p_month_start': month_start,
            'p_month_end': month_end,
            'p_sow_filter': None
        }
    ).execute()
    
    return {
        "jobs": result.data or []
    }


@app.get("/api/history/routes")
def get_historical_routes(
    regions: str,  # Comma-separated list of regions
    month: int = None,  # Optional: filter by month (1-12)
    year: int = None,  # Optional: filter by year
    tech_id: int = None  # Optional: filter by technician
):
    """
    Get historical routes for specified regions.
    Groups jobs by technician and date to show what routes were done.
    """
    sb = supabase_client()
    
    region_list = [r.strip() for r in regions.split(',')]
    
    # Build query
    query = sb.table('job_history').select('*').in_('region', region_list)
    
    # Apply filters
    if year:
        query = query.gte('scheduled_date', f'{year}-01-01').lte('scheduled_date', f'{year}-12-31')
    if month and year:
        month_str = str(month).zfill(2)
        last_day = 28 if month == 2 else 30 if month in [4, 6, 9, 11] else 31
        query = query.gte('scheduled_date', f'{year}-{month_str}-01').lte('scheduled_date', f'{year}-{month_str}-{last_day}')
    if tech_id:
        query = query.eq('technician_id', tech_id)
    
    result = query.order('scheduled_date').execute()
    
    if not result.data:
        return {"routes": [], "summary": {}}
    
    # Group by technician and date to form routes
    routes_by_tech_date = {}
    for job in result.data:
        tech = job.get('technician_id')
        date = job.get('scheduled_date')
        key = f"{tech}_{date}"
        
        if key not in routes_by_tech_date:
            routes_by_tech_date[key] = {
                'technician_id': tech,
                'date': date,
                'jobs': []
            }
        routes_by_tech_date[key]['jobs'].append(job)
    
    # Convert to list and sort
    routes = list(routes_by_tech_date.values())
    routes.sort(key=lambda r: (r['technician_id'], r['date']))
    
    # Group consecutive days into multi-day trips
    trips = []
    current_trip = None
    
    for route in routes:
        if current_trip is None:
            current_trip = {
                'technician_id': route['technician_id'],
                'start_date': route['date'],
                'end_date': route['date'],
                'days': [route]
            }
        elif (route['technician_id'] == current_trip['technician_id'] and 
              is_next_day(current_trip['end_date'], route['date'])):
            # Continue the trip
            current_trip['end_date'] = route['date']
            current_trip['days'].append(route)
        else:
            # Save current trip, start new one
            trips.append(current_trip)
            current_trip = {
                'technician_id': route['technician_id'],
                'start_date': route['date'],
                'end_date': route['date'],
                'days': [route]
            }
    
    if current_trip:
        trips.append(current_trip)
    
    # Calculate summary for each trip
    for trip in trips:
        trip['total_jobs'] = sum(len(d['jobs']) for d in trip['days'])
        trip['total_hours'] = sum(sum(j.get('duration', 2) or 2 for j in d['jobs']) for d in trip['days'])
        trip['regions'] = list(set(j.get('region') for d in trip['days'] for j in d['jobs']))
        trip['num_days'] = len(trip['days'])
    
    return {
        "trips": trips,
        "total_trips": len(trips),
        "regions_queried": region_list
    }


def is_next_day(date1_str, date2_str):
    """Check if date2 is the next working day after date1 (handles weekends)"""
    from datetime import datetime, timedelta
    try:
        d1 = datetime.strptime(date1_str, '%Y-%m-%d')
        d2 = datetime.strptime(date2_str, '%Y-%m-%d')
        diff = (d2 - d1).days
        
        # Direct next day
        if diff == 1:
            return True
        
        # Friday to Monday (skip weekend)
        if diff == 3 and d1.weekday() == 4:  # Friday is 4
            return True
            
        return False
    except:
        return False


@app.get("/api/schedule/scheduled-sites")
def get_scheduled_sites(year: int = None):
    """Get list of site_ids that already have jobs scheduled for the given year"""
    sb = supabase_client()
    
    if not year:
        year = 2026  # Default
    
    # Get scheduled jobs for this year
    result = sb.table('scheduled_jobs')\
        .select('site_id')\
        .gte('date', f'{year}-01-01')\
        .lte('date', f'{year}-12-31')\
        .execute()
    
    if not result.data:
        return {"scheduled_site_ids": []}
    
    # Get unique site_ids
    site_ids = list(set(j['site_id'] for j in result.data if j.get('site_id')))
    
    return {"scheduled_site_ids": site_ids, "count": len(site_ids)}


@app.post("/api/schedule/assign")
def assign_single_job(req: AssignJobRequest):
    """Assign one job to a tech on a specific date"""
    
    # 1. Get job details
    job = sb_select("job_pool", filters=[("work_order", "eq", req.work_order)])
    if not job:
        return {"success": False, "errors": ["Job not found"]}
    
    job = job[0]
    
    # 2. Get technician details  ÃƒÂ¢Ã¢â‚¬Â Ã‚Â ADD THIS SECTION
    tech_result = sb_select("technicians", filters=[("technician_id", "eq", req.technician_id)])
    if not tech_result:
        return {"success": False, "errors": [f"Technician {req.technician_id} not found"]}
    tech = tech_result[0]

    # 3. Check tech eligibility
    elig = sb_select("job_technician_eligibility", filters=[
        ("work_order", "eq", req.work_order),
        ("technician_id", "eq", req.technician_id)
    ])
    
    if not elig:
        return {
            "success": False, 
            "errors": [f"Tech {req.technician_id} is not eligible for job {req.work_order}"]
        }
    
    # 4. Check if already scheduled
    existing = sb_select("scheduled_jobs", filters=[
        ("work_order", "eq", req.work_order)
    ])
    
    if existing:
        return {
            "success": False,
            "errors": [f"Job {req.work_order} is already scheduled"]
        }
    
    # 5. Insert into scheduled_jobs
    scheduled_row = {
    "work_order": req.work_order,
    "technician_id": req.technician_id,
    "assigned_tech_name": tech.get("name"),
    "date": req.date,  # Just the date string, no modifications!
    "site_name": job.get("site_name"),
    "site_city": job.get("site_city"),
    "site_state": job.get("site_state"),
    "site_id": job.get("site_id"),
    "duration": float(job.get("duration", 2)),
    "sow_1": job.get("sow_1"),
    "due_date": job.get("due_date"),
    "latitude": job.get("latitude"),
    "longitude": job.get("longitude"),
    "is_night_job": job.get("night_test", False)
    }
    
    try:
        sb_insert("scheduled_jobs", [scheduled_row])
        
        # 6. Update job status
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


class UpdateScheduleRequest(BaseModel):
    work_order: int
    date: Optional[str] = None
    technician_id: Optional[int] = None


@app.post("/api/schedule/update")
def update_scheduled_job(req: UpdateScheduleRequest):
    """Update a scheduled job (change date or technician)"""
    sb = supabase_client()
    
    # Check job exists in scheduled_jobs
    existing = sb_select("scheduled_jobs", filters=[("work_order", "eq", req.work_order)])
    if not existing:
        return {"success": False, "error": "Job not found in schedule"}
    
    updates = {}
    
    if req.date:
        updates["date"] = req.date
    
    if req.technician_id:
        # Verify tech eligibility
        elig = sb_select("job_technician_eligibility", filters=[
            ("work_order", "eq", req.work_order),
            ("technician_id", "eq", req.technician_id)
        ])
        if not elig:
            return {"success": False, "error": f"Tech {req.technician_id} is not eligible for this job"}
        
        # Get tech name
        tech = sb_select("technicians", filters=[("technician_id", "eq", req.technician_id)])
        if tech:
            updates["technician_id"] = req.technician_id
            updates["assigned_tech_name"] = tech[0].get("name")
    
    if not updates:
        return {"success": False, "error": "No updates provided"}
    
    # Update the record
    sb.table("scheduled_jobs").update(updates).eq("work_order", req.work_order).execute()
    
    return {"success": True, "work_order": req.work_order, "updates": updates}


class UnscheduleRequest(BaseModel):
    work_order: int


@app.post("/api/schedule/unschedule")
def unschedule_job(req: UnscheduleRequest):
    """Remove a job from schedule and return it to Waiting to Schedule"""
    sb = supabase_client()
    
    # Delete from scheduled_jobs
    sb.table("scheduled_jobs").delete().eq("work_order", req.work_order).execute()
    
    # Reset job status to Waiting to Schedule
    sb.table("job_pool").update({"jp_status": "Waiting to Schedule"}).eq("work_order", req.work_order).execute()
    
    return {"success": True, "work_order": req.work_order}


class AddSecondaryTechRequest(BaseModel):
    work_order: int
    secondary_tech_id: int


@app.post("/api/schedule/add-secondary")
def add_secondary_tech(req: AddSecondaryTechRequest):
    """Add a secondary technician to a scheduled job using the additional_techs table"""
    sb = supabase_client()
    
    # Check job exists
    existing = sb_select("scheduled_jobs", filters=[("work_order", "eq", req.work_order)])
    if not existing:
        return {"success": False, "error": "Job not found in schedule"}
    
    job = existing[0]
    
    # Don't allow adding the primary tech as secondary
    if job.get('technician_id') == req.secondary_tech_id:
        return {"success": False, "error": "Cannot add primary tech as secondary tech"}
    
    # Get secondary tech name
    tech = sb_select("technicians", filters=[("technician_id", "eq", req.secondary_tech_id)])
    if not tech:
        return {"success": False, "error": "Secondary technician not found"}
    
    # Check if already added
    existing_addl = sb.table("scheduled_job_additional_techs")\
        .select("*")\
        .eq("work_order", req.work_order)\
        .eq("technician_id", req.secondary_tech_id)\
        .execute()
    
    if existing_addl.data:
        return {"success": False, "error": "This technician is already assigned to this job"}
    
    # Insert into the additional techs table
    try:
        sb.table("scheduled_job_additional_techs").insert({
            "work_order": req.work_order,
            "technician_id": req.secondary_tech_id
        }).execute()
        
        return {
            "success": True, 
            "work_order": req.work_order, 
            "secondary_tech": tech[0].get("name"),
            "secondary_tech_id": req.secondary_tech_id
        }
    except Exception as e:
        print(f" Error adding secondary tech: {e}")
        return {"success": False, "error": str(e)}


@app.delete("/api/schedule/remove-secondary")
def remove_secondary_tech(work_order: int, technician_id: int):
    """Remove a secondary technician from a scheduled job"""
    sb = supabase_client()
    try:
        sb.table("scheduled_job_additional_techs")\
            .delete()\
            .eq("work_order", work_order)\
            .eq("technician_id", technician_id)\
            .execute()
        
        return {"success": True, "message": f"Removed tech {technician_id} from WO {work_order}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/schedule/additional-techs")
def get_all_additional_techs(week_start: str = None):
    """Get all additional tech assignments, optionally filtered by week"""
    sb = supabase_client()
    try:
        # Get all additional tech assignments
        addl_techs = sb.table("scheduled_job_additional_techs").select("*").execute()
        
        if not addl_techs.data:
            return {"success": True, "additional_techs": []}
        
        # Get related job and tech info
        result = []
        for addl in addl_techs.data:
            # Get job details
            job = sb_select("scheduled_jobs", filters=[("work_order", "eq", addl['work_order'])])
            if not job:
                continue
            
            job = job[0]
            
            # Filter by week if specified
            if week_start:
                from datetime import datetime, timedelta
                start_date = datetime.fromisoformat(week_start).date()
                end_date = start_date + timedelta(days=4)
                job_date = datetime.fromisoformat(job['date']).date() if job.get('date') else None
                if not job_date or not (start_date <= job_date <= end_date):
                    continue
            
            # Get tech name
            tech = sb_select("technicians", filters=[("technician_id", "eq", addl['technician_id'])])
            
            result.append({
                "work_order": addl['work_order'],
                "technician_id": addl['technician_id'],
                "tech_name": tech[0]['name'] if tech else f"Tech {addl['technician_id']}",
                "date": job.get('date'),
                "site_name": job.get('site_name'),
                "site_city": job.get('site_city'),
                "duration": job.get('duration'),
                "primary_tech_id": job.get('technician_id'),
                "primary_tech_name": job.get('assigned_tech_name'),
                "sow_1": job.get('sow_1')
            })
        
        return {"success": True, "additional_techs": result}
        
    except Exception as e:
        print(f" Error getting additional techs: {e}")
        return {"success": False, "additional_techs": [], "error": str(e)}



@app.get("/api/fill-day/suggestions")
def get_fill_day_suggestions(
    tech_id: int,
    center_lat: float,
    center_lon: float,
    date: str,
    remaining_hours: float,
    month_start: str,
    month_end: str
):
    """Get job suggestions to fill a tech's day - nearby jobs first, then corridor jobs"""
    sb = supabase_client()
    
    suggestions = []
    
    # Get tech info for home location (for corridor)
    tech = sb_select("technicians", filters=[("technician_id", "eq", tech_id)])
    if not tech:
        return {"suggestions": []}
    tech = tech[0]
    
    # 1. Get nearby jobs (within 30 miles of center)
    try:
        nearby_result = sb.rpc('find_nearby_jobs', {
            'center_lat': center_lat,
            'center_lon': center_lon,
            'radius_miles': 30,
            'max_results': 20,
            'p_tech_id': tech_id,
            'p_schedule_date': date,
            'filter_region': None
        }).execute()
        
        if nearby_result.data:
            for job in nearby_result.data:
                if job.get('duration', 2) <= remaining_hours:
                    job['suggestion_type'] = 'nearby'
                    job['distance_miles'] = job.get('distance_miles', 0)
                    suggestions.append(job)
    except Exception as e:
        print(f"Error getting nearby jobs: {e}")
    
    # 2. Get corridor jobs (along route from tech home to center)
    try:
        corridor_result = sb.rpc('find_jobs_along_route', {
            'start_lat': tech['home_latitude'],
            'start_lon': tech['home_longitude'],
            'end_lat': center_lat,
            'end_lon': center_lon,
            'corridor_miles': 15,
            'max_results': 15,
            'p_tech_id': tech_id,
            'p_schedule_date': date
        }).execute()
        
        if corridor_result.data:
            # Filter out jobs already in nearby list
            nearby_work_orders = {j['work_order'] for j in suggestions}
            for job in corridor_result.data:
                if job['work_order'] not in nearby_work_orders and job.get('duration', 2) <= remaining_hours:
                    job['suggestion_type'] = 'corridor'
                    job['distance_miles'] = job.get('distance_from_start_miles', 0)
                    suggestions.append(job)
    except Exception as e:
        print(f"Error getting corridor jobs: {e}")
    
    # Sort: nearby jobs first (by due date, then distance), then corridor jobs (by due date, then distance)
    def sort_key(job):
        type_order = 0 if job['suggestion_type'] == 'nearby' else 1
        due_date = job.get('due_date', '9999-12-31')
        distance = job.get('distance_miles', 999)
        return (type_order, due_date, distance)
    
    suggestions.sort(key=sort_key)
    
    # Limit to 15
    return {"suggestions": suggestions[:15]}

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
    from scheduler_v5_geographic import haversine
    
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
                    "duration": unsched.get("duration", 2),
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
    from scheduler_utils import haversine
    
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
                    results["details"].append(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬Å“ WO {job['work_order']} ÃƒÂ¢Ã¢â‚¬Â Ã¢â‚¬â„¢ Tech {tech_id}")
                except Exception as e:
                    results["failed"] += 1
                    results["details"].append(f"ÃƒÂ¢Ã…â€œÃ¢â‚¬â€ WO {job['work_order']}: {str(e)}")
    
    return results

@app.get("/api/regions/list")
def get_regions_list():
   
    try:
        regions = sb_select("regions")
        return [{"region_name": r.get("region_name")} for r in regions]
    except Exception as e:
        raise HTTPException(500, f"Failed to load regions: {str(e)}")


@app.get("/api/schedule/week-all") 
def get_full_week_schedule(week_start: str):
    """
    Get all scheduled jobs for a week WITH hotel, initial drive, and between-job drive calculations
    """
    try:
        from scheduler_v5_geographic import haversine
        sb = supabase_client()
        
        start_date = datetime.fromisoformat(week_start).date()
        end_date = start_date + timedelta(days=4)
        
        # Get scheduled jobs
        scheduled_jobs = sb_select("scheduled_jobs", filters=[
            ("date", "gte", str(start_date)),
            ("date", "lte", str(end_date))
        ])
        
        # If no jobs, return early
        if not scheduled_jobs:
            return {
                "week_start": str(start_date),
                "scheduled_jobs": [],
                "total_jobs": 0
            }
        
        # Get all technicians for home location data
        technicians = sb_select("technicians")
        tech_homes = {}
        for t in technicians:
            if t.get('home_latitude') and t.get('home_longitude'):
                tech_homes[t['technician_id']] = (t['home_latitude'], t['home_longitude'])
        
        # Group jobs by tech and date
        jobs_by_tech_date = {}
        for job in scheduled_jobs:
            key = f"{job['technician_id']}-{job['date']}"
            if key not in jobs_by_tech_date:
                jobs_by_tech_date[key] = []
            jobs_by_tech_date[key].append(job)
        
        # Calculate hotel stays, initial drive, and between-job drive
        enhanced_jobs = []
        last_locations = {}  # Track where tech ended previous day
        
        for day_num in range(5):  # Mon-Fri
            current_date = str(start_date + timedelta(days=day_num))
            
            for tech_id in set(job['technician_id'] for job in scheduled_jobs):
                key = f"{tech_id}-{current_date}"
                daily_jobs = jobs_by_tech_date.get(key, [])
                
                if not daily_jobs:
                    continue
                
                # Check if tech has home location
                if tech_id not in tech_homes:
                    print(f" Warning: Tech {tech_id} has no home location, using defaults")
                    for job in daily_jobs:
                        job['initial_drive_hours'] = 0.5
                        job['drive_time'] = 0
                        job['needs_hotel'] = False
                        job['hotel_location'] = None
                        enhanced_jobs.append(job)
                    continue
                
                # Sort jobs by start_time (handle None)
                daily_jobs.sort(key=lambda j: j.get('start_time') or '08:00')
                
                # Determine starting location (home or last night's hotel)
                if tech_id in last_locations:
                    start_location = last_locations[tech_id]
                else:
                    start_location = tech_homes[tech_id]
                
                # Calculate drives for each job
                for i, job in enumerate(daily_jobs):
                    # Initial drive (only for first job)
                    if i == 0:
                        if job.get('latitude') and job.get('longitude'):
                            try:
                                initial_distance = haversine(
                                    start_location[0], start_location[1],
                                    job['latitude'], job['longitude']
                                )
                                job['initial_drive_hours'] = initial_distance / 45
                            except Exception:
                                job['initial_drive_hours'] = 0.5
                        else:
                            job['initial_drive_hours'] = 0.5
                    else:
                        job['initial_drive_hours'] = 0
                    
                    # Drive time to NEXT job (between jobs)
                    if i < len(daily_jobs) - 1:
                        next_job = daily_jobs[i + 1]
                        if (job.get('latitude') and job.get('longitude') and 
                            next_job.get('latitude') and next_job.get('longitude')):
                            try:
                                distance = haversine(
                                    job['latitude'], job['longitude'],
                                    next_job['latitude'], next_job['longitude']
                                )
                                job['drive_time'] = distance / 45
                            except Exception:
                                job['drive_time'] = 0.5
                        else:
                            job['drive_time'] = 0.5
                    else:
                        # Last job - no drive to next job
                        job['drive_time'] = 0
                
                # Calculate hotel for last job
                last_job = daily_jobs[-1]
                if last_job.get('latitude') and last_job.get('longitude'):
                    try:
                        distance_to_home = haversine(
                            last_job['latitude'], last_job['longitude'],
                            tech_homes[tech_id][0], tech_homes[tech_id][1]
                        )
                        
                        # Friday always go home, otherwise hotel if >90 miles
                        is_friday = (start_date + timedelta(days=day_num)).weekday() == 4
                        needs_hotel = distance_to_home > 90 and not is_friday
                        
                        last_job['needs_hotel'] = needs_hotel
                        last_job['hotel_location'] = f"{last_job.get('site_city', 'Unknown')}" if needs_hotel else None
                        last_job['distance_to_home'] = distance_to_home
                        
                        # Update last location for next day
                        if needs_hotel:
                            last_locations[tech_id] = (last_job['latitude'], last_job['longitude'])
                        else:
                            last_locations.pop(tech_id, None)
                    except Exception:
                        last_job['needs_hotel'] = False
                        last_job['hotel_location'] = None
                        last_job['distance_to_home'] = 0
                else:
                    last_job['needs_hotel'] = False
                    last_job['hotel_location'] = None
                    last_job['distance_to_home'] = 0
                
                # Add all jobs to enhanced list
                enhanced_jobs.extend(daily_jobs)
        
        # Fetch additional techs for this week and merge into results
        try:
            addl_techs = sb.table("scheduled_job_additional_techs").select("*").execute()
            if addl_techs.data:
                # Create lookup of additional techs by work_order
                addl_by_wo = {}
                for addl in addl_techs.data:
                    wo = addl['work_order']
                    if wo not in addl_by_wo:
                        addl_by_wo[wo] = []
                    addl_by_wo[wo].append(addl['technician_id'])
                
                # Add additional_techs array to each job
                for job in enhanced_jobs:
                    wo = job.get('work_order')
                    if wo in addl_by_wo:
                        job['additional_tech_ids'] = addl_by_wo[wo]
                        # Get names
                        names = []
                        for tid in addl_by_wo[wo]:
                            t = next((x for x in technicians if x['technician_id'] == tid), None)
                            if t:
                                names.append(t['name'])
                        job['additional_tech_names'] = names
        except Exception as e:
            print(f"Warning: Could not fetch additional techs: {e}")
        
        return {
            "week_start": str(start_date),
            "scheduled_jobs": enhanced_jobs,
            "total_jobs": len(enhanced_jobs)
        }
        
    except Exception as e:
        import traceback
        print(f" Error in get_full_week_schedule: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(500, f"Failed to load week: {str(e)}")


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
        total_work_hours = sum(float(j.get("duration", 2)) for j in jobs)
        
        # Regional breakdown
        regional_stats = defaultdict(lambda: {
            'jobs': 0,
            'work_hours': 0,
            'job_list': []
        })
        
        for job in jobs:
            region = job.get('region', 'Unknown')
            regional_stats[region]['jobs'] += 1
            regional_stats[region]['work_hours'] += float(job.get('duration', 2))
            regional_stats[region]['job_list'].append(job)
        
        # Estimate drive time by region
        # Simple estimation: 30 miles average between jobs in same region
        # Plus distance from nearest tech home to region center
        AVG_SPEED = 45  # mph (conservative for mountain/rural roads)
        AVG_INTRA_REGION_DISTANCE = 30  # miles between jobs in same region
        
        total_drive_hours = 0
        regional_breakdown = []
        
        for region_name, stats in regional_stats.items():
            job_count = stats['jobs']
            work_hours = stats['work_hours']
            
            # Estimate drive time for this region
            # Formula: (jobs - 1) ÃƒÆ’Ã¢â‚¬â€ avg_distance_between_jobs + 2 ÃƒÆ’Ã¢â‚¬â€ home_to_region
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
            weekly_stats[week_key]['work_hours'] += float(job.get('duration', 2))
            
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
                    "duration": job.get("duration", 2),
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
                        "duration": job.get("duration", 2),
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
    
# ============================================
# DATA MODELS
# ============================================

class SingleJob(BaseModel):
    work_order: int
    site_name: str
    site_city: str
    site_state: str
    due_date: str
    sow_1: Optional[str] = None
    site_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    duration: Optional[float] = 4.0  # Default 4 hours
    jp_priority: Optional[str] = "Normal"

class RemoveJobRequest(BaseModel):
    work_orders: List[int]
    reason: Optional[str] = "Customer cancellation"

# ============================================
# UPLOAD AND VALIDATION ENDPOINTS
# ============================================

@app.post("/api/upload-jobs")
async def upload_jobs(file: UploadFile = File(...)):
    """
    Upload Excel or CSV file with jobs to staging table
    Handles: BOM markers, mixed encodings, NULL strings, and NaN values
    """
    try:
        # Read file based on extension
        if file.filename.endswith('.csv'):
            contents = await file.read()
            
            # Try multiple approaches to read the CSV
            df = None
            encoding_used = None
            
            # Method 1: Try UTF-8 with BOM handling
            try:
                # Remove BOM if present
                if contents.startswith(b'\xef\xbb\xbf'):
                    contents_clean = contents[3:]
                    df = pd.read_csv(io.BytesIO(contents_clean), encoding='utf-8')
                    encoding_used = "UTF-8 (BOM removed)"
                else:
                    df = pd.read_csv(io.BytesIO(contents), encoding='utf-8')
                    encoding_used = "UTF-8"
            except UnicodeDecodeError:
                pass
            
            # Method 2: Try Latin-1 (handles Spanish characters like ÃƒÂ±)
            if df is None:
                try:
                    df = pd.read_csv(io.BytesIO(contents), encoding='latin-1')
                    encoding_used = "Latin-1"
                except:
                    pass
            
            # Method 3: Try Windows-1252 (common Excel encoding)
            if df is None:
                try:
                    df = pd.read_csv(io.BytesIO(contents), encoding='cp1252')
                    encoding_used = "Windows-1252"
                except:
                    pass
            
            # Method 4: Try with error handling
            if df is None:
                try:
                    df = pd.read_csv(io.BytesIO(contents), encoding='utf-8', errors='ignore')
                    encoding_used = "UTF-8 (errors ignored)"
                except:
                    pass
            
            if df is None:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot read CSV file. File contains characters that cannot be decoded. Try saving as plain ASCII or contact support."
                )
            
            print(f"Successfully read CSV using {encoding_used} encoding")
            
        elif file.filename.endswith(('.xlsx', '.xls')):
            contents = await file.read()
            df = pd.read_excel(io.BytesIO(contents))
            encoding_used = "Excel"
        else:
            raise HTTPException(status_code=400, detail="Invalid file type. Use CSV or Excel.")
        
        # Clean column names (remove BOM, spaces, etc.)
        df.columns = df.columns.str.replace('\ufeff', '').str.strip()
        
        # Replace string "NULL" with actual None
        df = df.replace('NULL', None)
        df = df.replace('null', None)
        df = df.replace('', None)  # Also replace empty strings
        
        # Validation results
        validation = {
            "errors": [],
            "warnings": [],
            "success": None,
            "info": [f"File read using {encoding_used} encoding"]
        }
        
        # Check required columns
        required_cols = ['work_order', 'site_name', 'site_city', 'site_state', 'due_date']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            validation["errors"].append(f"Missing required columns: {', '.join(missing_cols)}")
            validation["warnings"].append(f"Found columns: {', '.join(df.columns.tolist())}")
            return {"validation": validation, "jobs_count": 0}
        
        # Data validation
        if df['work_order'].duplicated().any():
            dupe_count = df['work_order'].duplicated().sum()
            dupe_wos = df[df['work_order'].duplicated()]['work_order'].tolist()[:5]
            validation["warnings"].append(f"Found {dupe_count} duplicate work orders (e.g., {dupe_wos})")
            # Remove duplicates, keeping first occurrence
            df = df.drop_duplicates(subset=['work_order'], keep='first')
        
        # Check for missing coordinates
        if 'latitude' in df.columns and 'longitude' in df.columns:
            missing_coords = df['latitude'].isna() | df['longitude'].isna()
            if missing_coords.any():
                count = missing_coords.sum()
                validation["warnings"].append(f"{count} jobs missing coordinates")
        
        # Check for missing due dates
        missing_due_dates = df['due_date'].isna()
        if missing_due_dates.any():
            count = missing_due_dates.sum()
            validation["errors"].append(f"{count} jobs missing due dates")
            # Remove rows with missing due dates
            df = df[~missing_due_dates]
            validation["warnings"].append(f"Removed {count} jobs with missing due dates")
        
        # Convert data types properly
        # Numeric columns
        numeric_cols = ['latitude', 'longitude', 'duration', 'siteid', 
                       'days_til_due_from_schedule', 'tech_count']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Boolean columns
        bool_cols = ['flag_missing_due_date', 'night_test', 'is_recurring_site']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].replace({
                    'TRUE': True, 'FALSE': False,
                    'true': True, 'false': False,
                    'True': True, 'False': False,
                    1: True, 0: False,
                    '1': True, '0': False
                })
        
        # CRITICAL: Replace NaN with None for JSON serialization
        df = df.where(pd.notnull(df), None)
        
        # Upload to staging table
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Clear existing staging data
        sb.table('stg_job_pool').delete().neq('work_order', 0).execute()
        
        # Prepare data for upload
        staging_data = df.to_dict('records')
        
        # Insert in batches
        batch_size = 100
        total_inserted = 0
        failed_records = []
        
        for i in range(0, len(staging_data), batch_size):
            batch = staging_data[i:i+batch_size]
            try:
                result = sb.table('stg_job_pool').insert(batch).execute()
                total_inserted += len(batch)
                print(f"Batch {i//batch_size + 1} inserted successfully")
            except Exception as batch_error:
                print(f"Batch {i//batch_size + 1} failed: {str(batch_error)}")
                # Try individual records
                for record in batch:
                    try:
                        sb.table('stg_job_pool').insert(record).execute()
                        total_inserted += 1
                    except Exception as record_error:
                        wo = record.get('work_order', 'unknown')
                        failed_records.append(wo)
                        print(f"Failed WO {wo}: {str(record_error)[:100]}")
        
        if failed_records:
            validation["warnings"].append(f"Failed to insert {len(failed_records)} records: {failed_records[:10]}")
        
        validation["success"] = f"Successfully uploaded {total_inserted} of {len(df)} jobs to staging"
        
        return {
            "validation": validation,
            "jobs_count": total_inserted,
            "filename": file.filename,
            "encoding": encoding_used
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Upload error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "validation": {
                "errors": [f"Upload failed: {str(e)}"],
                "warnings": ["Check that your CSV is properly formatted"],
                "success": None
            },
            "jobs_count": 0
        }


# ============================================
# PROCESS STAGING TO PRODUCTION
# ============================================

@app.post("/api/process-staging")
async def process_staging():
    """
    Process staging table to production job_pool.
    - Adds missing sites to sites table first
    - Inserts new jobs
    - Updates existing non-scheduled jobs (sow_1, due_date, etc.)
    - Skips jobs already marked as 'Scheduled'
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Call the import function
        result = sb.rpc('import_new_jobs').execute()
        
        # Result.data should contain our JSONB response
        if result.data:
            return result.data
        else:
            return {"success": True, "message": "Processing complete"}
            
    except Exception as e:
        error_str = str(e)
        
        # The Supabase client throws an error but the function actually succeeded
        # Extract the actual result from the error message
        if '"success":' in error_str:
            import json
            import re
            # Find JSON object in the error string
            match = re.search(r"b'(\{.*\})'", error_str)
            if match:
                try:
                    json_str = match.group(1).replace('\\"', '"')
                    return json.loads(json_str)
                except:
                    pass
            
            # Try another pattern
            match = re.search(r'\{[^{}]*"success"[^{}]*"message"[^{}]*\}', error_str)
            if match:
                try:
                    return json.loads(match.group())
                except:
                    pass
        
        return {"success": False, "error": error_str}
    
# ============================================
# SINGLE JOB MANAGEMENT
# ============================================

@app.post("/api/add-single-job")
async def add_single_job(job: SingleJob):
    """
    Add a single job directly to production
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Prepare job data
        job_data = {
            "work_order": job.work_order,
            "site_name": job.site_name,
            "site_city": job.site_city,
            "site_state": job.site_state,
            "due_date": job.due_date,
            "sow_1": job.sow_1,
            "site_address": job.site_address,
            "latitude": job.latitude,
            "longitude": job.longitude,
            "duration": job.duration,
            "jp_priority": job.jp_priority,
            "jp_status": "Ready",
            "tech_count": 0  # Will be calculated by trigger
        }
        
        # Insert directly to job_pool
        result = sb.table('job_pool').insert(job_data).execute()
        
        # Trigger should handle eligibility calculation
        # If not, we can call it manually:
        # sb.rpc('update_single_job_eligibility', {'job_id': job.work_order}).execute()
        
        return {
            "success": True,
            "work_order": job.work_order,
            "message": f"Job {job.work_order} added successfully"
        }
        
    except Exception as e:
        print(f"Add job error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/remove-jobs")
async def remove_jobs(request: RemoveJobRequest):
    """
    Remove/cancel jobs from the system (archives them first)
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        archived_count = 0
        
        # Archive each job before deleting
        for work_order in request.work_orders:
            try:
                # Get job data from job_pool
                job_result = sb.table('job_pool').select('*').eq('work_order', work_order).execute()
                
                if job_result.data:
                    job_data = job_result.data[0]
                    
                    # Map job_pool columns to job_archive columns
                    archive_data = {
                        'work_order': job_data['work_order'],
                        'site_name': job_data.get('site_name'),
                        'site_id': job_data.get('site_id'),
                        'address': job_data.get('site_address'),  # job_pool: site_address -> job_archive: address
                        'site_city': job_data.get('site_city'),
                        'site_state': job_data.get('site_state'),
                        'site_zip': None,  # Not in job_pool
                        'site_latitude': job_data.get('latitude'),  # job_pool: latitude -> job_archive: site_latitude
                        'site_longitude': job_data.get('longitude'),  # job_pool: longitude -> job_archive: site_longitude
                        'due_date': job_data.get('due_date'),
                        'sow_1': job_data.get('sow_1'),
                        'sow_2': None,  # Not in job_pool
                        'jp_status': job_data.get('jp_status'),
                        'eligible_technicians': None,  # Not in job_pool
                        'archived_date': datetime.now().isoformat(),
                        'archive_reason': request.reason if hasattr(request, 'reason') else 'Removed via data manager',
                        'archived_by': 'system'
                    }
                    
                    # Insert into job_archive
                    sb.table('job_archive').insert(archive_data).execute()
                    archived_count += 1
                    
            except Exception as archive_error:
                print(f"Error archiving job {work_order}: {archive_error}")
                # Continue to delete even if archive fails
        
        # Remove from scheduled_jobs if they exist there
        sb.table('scheduled_jobs').delete().in_('work_order', request.work_orders).execute()
        
        # Remove from job_pool
        result = sb.table('job_pool').delete().in_('work_order', request.work_orders).execute()
        
        return {
            "success": True,
            "jobs_removed": len(request.work_orders),
            "jobs_archived": archived_count,
            "message": f"Archived {archived_count} and removed {len(request.work_orders)} jobs"
        }
        
    except Exception as e:
        print(f"Remove jobs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# RECALCULATION AND MAINTENANCE
# ============================================

@app.post("/api/recalculate-eligibility")
async def recalculate_eligibility():
    """
    Recalculate tech eligibility for all jobs
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Call the recalculation function
        result = sb.rpc('populate_tech_eligibility').execute()
        
        # Get counts
        eligibility_count = sb.table('job_technician_eligibility').select('*', count='exact').execute()
        jobs_with_techs = sb.table('job_pool').select('work_order', count='exact').gt('tech_count', 0).execute()
        
        return {
            "success": True,
            "jobs_updated": jobs_with_techs.count if hasattr(jobs_with_techs, 'count') else 0,
            "eligibility_records": eligibility_count.count if hasattr(eligibility_count, 'count') else 0,
            "message": "Eligibility recalculated successfully"
        }
        
    except Exception as e:
        print(f"Recalculation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================
# DATABASE STATUS
# ============================================

@app.post("/api/job/update")
async def update_job_field(request: dict):
    """Update a single field in a job"""
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        work_order = request.get('work_order')
        field = request.get('field')
        value = request.get('value')
        
        # Update the job
        result = sb.table('job_pool').update({field: value}).eq('work_order', work_order).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "success": True,
            "message": f"Updated {field} for job {work_order}"
        }
        
    except Exception as e:
        print(f"Update job error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/database-status")
async def get_database_status():
    """
    Get current database statistics
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Get various counts
        total_jobs = sb.table('job_pool').select('work_order', count='exact').execute()
        scheduled_jobs = sb.table('scheduled_jobs').select('work_order', count='exact').execute()
        
        # Get unscheduled count (jobs in job_pool not in scheduled_jobs)
        unscheduled = sb.table('job_pool').select('work_order', count='exact').eq('jp_status', 'Call').execute()
        
        # Get overdue jobs
        today = date.today().isoformat()
        overdue = sb.table('job_pool').select('work_order', count='exact').lt('due_date', today).neq('jp_status', 'Scheduled').execute()
        
        # Active technicians
        active_techs = sb.table('technicians').select('technician_id', count='exact').is_('active', True).execute()
        
        # Problem jobs (no eligible techs)
        problem_jobs = sb.table('job_pool').select('work_order', count='exact').eq('tech_count', 0).execute()
        
        # Staging count
        staging_count = sb.table('stg_job_pool').select('work_order', count='exact').execute()
        
        return {
            "total_jobs": total_jobs.count if hasattr(total_jobs, 'count') else 0,
            "scheduled_jobs": scheduled_jobs.count if hasattr(scheduled_jobs, 'count') else 0,
            "unscheduled_jobs": unscheduled.count if hasattr(unscheduled, 'count') else 0,
            "overdue_jobs": overdue.count if hasattr(overdue, 'count') else 0,
            "active_techs": active_techs.count if hasattr(active_techs, 'count') else 0,
            "problem_jobs": problem_jobs.count if hasattr(problem_jobs, 'count') else 0,
            "staging_jobs": staging_count.count if hasattr(staging_count, 'count') else 0,
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Status error: {e}")
        return {
            "total_jobs": 0,
            "scheduled_jobs": 0,
            "unscheduled_jobs": 0,
            "overdue_jobs": 0,
            "active_techs": 0,
            "problem_jobs": 0,
            "staging_jobs": 0,
            "error": str(e)
        }

@app.get("/api/staging-preview")
async def preview_staging():
    """
    Preview what's in the staging table
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Get first 10 rows from staging
        preview = sb.table('stg_job_pool').select('*').limit(10).execute()
        
        # Get total count
        count_result = sb.table('stg_job_pool').select('work_order', count='exact').execute()
        
        return {
            "total_count": count_result.count if hasattr(count_result, 'count') else 0,
            "preview_rows": preview.data,
            "columns": list(preview.data[0].keys()) if preview.data else []
        }
        
    except Exception as e:
        print(f"Preview error: {e}")
        return {
            "total_count": 0,
            "preview_rows": [],
            "columns": [],
            "error": str(e)
        }

# ============================================
# VALIDATION HELPERS
# ============================================

@app.post("/api/validate-jobs")
async def validate_jobs():
    """
    Run validation checks on current job pool
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        issues = []
        
        # Check for jobs with no eligible techs
        no_techs = sb.table('job_pool').select('work_order, site_name, sow_1').eq('tech_count', 0).execute()
        if no_techs.data:
            for job in no_techs.data:
                issues.append({
                    "type": "error",
                    "job": job['work_order'],
                    "message": f"No eligible techs for {job['site_name']} (SOW: {job['sow_1']})"
                })
        
        # Check for missing regions
        no_region = sb.table('job_pool').select('work_order, site_name').or_('region.is.null,region.eq.NULL').execute()
        if no_region.data:
            for job in no_region.data:
                issues.append({
                    "type": "warning",
                    "job": job['work_order'],
                    "message": f"No region assigned for {job['site_name']}"
                })
        
        # Check for overdue unscheduled jobs
        today = date.today().isoformat()
        overdue = sb.table('job_pool').select('work_order, site_name, due_date').lt('due_date', today).is_('jp_status', 'Ready').execute()
        if overdue.data:
            for job in overdue.data:
                issues.append({
                    "type": "error",
                    "job": job['work_order'],
                    "message": f"{job['site_name']} is overdue (due: {job['due_date']})"
                })
        
        return {
            "issues_found": len(issues),
            "issues": issues[:50],  # Limit to first 50 issues
            "summary": {
                "errors": len([i for i in issues if i["type"] == "error"]),
                "warnings": len([i for i in issues if i["type"] == "warning"])
            }
        }
        
    except Exception as e:
        print(f"Validation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/api/test-supabase")
async def test_supabase():
    """Test if Supabase connection works"""
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Try a simple query
        result = sb.table('stg_job_pool').select('work_order').limit(1).execute()
        
        return {
            "success": True,
            "message": "Supabase connected",
            "data": result.data
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
@app.get("/api/test-rpc")
async def test_rpc():
    """Test if we can call ANY RPC function"""
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Try to call a function that we know exists
        result = sb.rpc('import_new_jobs').execute()
        
        return {"success": True, "result": "Function called"}
    except Exception as e:
        return {
            "success": False, 
            "error": str(e),
            "details": {
                "message": str(e),
                "type": type(e).__name__
            }
        }
@app.get("/api/test-functions")
async def test_functions():
    """Test each function individually"""
    from supabase_client import supabase_client
    sb = supabase_client()
    
    results = {}
    
    # Test 1: Can we call assign_regions?
    try:
        sb.rpc('assign_regions_to_jobs').execute()
        results['assign_regions'] = "SUCCESS"
    except Exception as e:
        results['assign_regions'] = f"FAILED: {str(e)}"
    
    # Test 2: Can we call populate_eligibility?
    try:
        sb.rpc('populate_tech_eligibility').execute()
        results['populate_eligibility'] = "SUCCESS"
    except Exception as e:
        results['populate_eligibility'] = f"FAILED: {str(e)}"
    
    # Test 3: Can we call import_new_jobs?
    try:
        sb.rpc('import_new_jobs').execute()
        results['import_new_jobs'] = "SUCCESS"
    except Exception as e:
        results['import_new_jobs'] = f"FAILED: {str(e)}"
    
    return results

# ============================================================================
# TECHNICIAN CRUD OPERATIONS
# ============================================================================

@app.get("/api/technicians/all")
def get_all_technicians():
    """Get all technicians (already exists, but ensure it includes all fields)"""
    techs = sb_select("technicians")
    
    for tech in techs:
        # Ensure all fields are present
        tech.setdefault('qualified_tests', '')
        tech.setdefault('states_allowed', '')
        tech.setdefault('states_excluded', '')
        tech.setdefault('home_location', '')
        tech.setdefault('max_weekly_hours', 40)
        tech.setdefault('max_daily_hours', 10)
        tech.setdefault('active', True)
    
    return {"count": len(techs), "technicians": techs}

@app.post("/api/technicians/add")
def add_technician(tech: TechnicianModel):
    """Add a new technician"""
    
    # Check if technician_id already exists
    existing = sb_select("technicians", filters=[
        ("technician_id", "eq", tech.technician_id)
    ])
    
    if existing:
        raise HTTPException(400, f"Technician with ID {tech.technician_id} already exists")
    
    # Check if name already exists
    existing_name = sb_select("technicians", filters=[
        ("name", "eq", tech.name)
    ])
    
    if existing_name:
        raise HTTPException(400, f"Technician with name '{tech.name}' already exists")
    
    # Prepare data
    tech_data = tech.dict()
    
    # Add geom field (PostGIS point)
    tech_data['home_geom'] = f"POINT({tech.home_longitude} {tech.home_latitude})"
    
    try:
        # Insert into technicians table
        sb_insert("technicians", [tech_data])
        
        # Recalculate eligibility for all jobs
        recalculate_eligibility_for_tech(tech.technician_id)
        
        return {
            "success": True,
            "message": f"Technician {tech.name} added successfully",
            "technician_id": tech.technician_id
        }
    
    except Exception as e:
        raise HTTPException(500, f"Failed to add technician: {str(e)}")

@app.post("/api/technicians/update")
def update_technician(tech: TechnicianModel):
    """Update existing technician"""
    
    # Check if exists
    existing = sb_select("technicians", filters=[
        ("technician_id", "eq", tech.technician_id)
    ])
    
    if not existing:
        raise HTTPException(404, f"Technician with ID {tech.technician_id} not found")
    
    # Prepare update data
    tech_data = tech.dict()
    tech_data.pop('technician_id', None)  # Don't update ID
    
    # Update geom field
    tech_data['home_geom'] = f"POINT({tech.home_longitude} {tech.home_latitude})"
    
    try:
        # Update technician
        sb_update(
            "technicians",
            {"technician_id": tech.technician_id},
            tech_data
        )
        
        # Recalculate eligibility (qualifications or states may have changed)
        recalculate_eligibility_for_tech(tech.technician_id)
        
        return {
            "success": True,
            "message": f"Technician {tech.name} updated successfully"
        }
    
    except Exception as e:
        raise HTTPException(500, f"Failed to update technician: {str(e)}")

@app.post("/api/technicians/toggle-active")
def toggle_technician_active(req: ToggleActiveRequest):
    """Activate or deactivate a technician"""
    
    try:
        sb_update(
            "technicians",
            {"technician_id": req.technician_id},
            {"active": req.active}
        )
        
        return {
            "success": True,
            "message": f"Technician {'activated' if req.active else 'deactivated'}"
        }
    
    except Exception as e:
        raise HTTPException(500, f"Failed to toggle status: {str(e)}")

# ============================================================================
# ELIGIBILITY RECALCULATION
# ============================================================================

def recalculate_eligibility_for_tech(tech_id: int):
    """
    Recalculate job eligibility for a specific technician.
    Called after updating qualifications or states.
    """
    
    # Get tech details
    tech = sb_select("technicians", filters=[
        ("technician_id", "eq", tech_id)
    ])
    
    if not tech:
        return
    
    tech = tech[0]
    
    # Get tech's qualifications and states
    tech_quals = set((tech.get('qualified_tests') or '').split(','))
    tech_states = set((tech.get('states_allowed') or '').split(','))
    
    # Get all jobs
    jobs = sb_select("job_pool", filters=[
        ("jp_status", "neq", "Completed")
    ])
    
    # Delete existing eligibility for this tech
    sb = supabase_client()
    sb.table("job_technician_eligibility").delete().eq("technician_id", tech_id).execute()
    
    # Calculate new eligibility
    eligible_jobs = []
    
    for job in jobs:
        job_state = job.get('site_state', '')
        job_sows = set((job.get('sow_1') or '').split(','))
        
        # Check state match
        if job_state not in tech_states:
            continue
        
        # Check SOW match (tech must have at least one of the job's SOWs)
        if not job_sows.intersection(tech_quals):
            continue
        
        # Eligible!
        eligible_jobs.append({
            "work_order": job['work_order'],
            "technician_id": tech_id
        })
    
    # Insert new eligibility records
    if eligible_jobs:
        sb_insert("job_technician_eligibility", eligible_jobs)
    
    print(f" Recalculated eligibility for Tech {tech_id}: {len(eligible_jobs)} eligible jobs")

# ============================================================================
# TIME OFF MANAGEMENT
# ============================================================================

@app.get("/api/timeoff/get")
def get_technician_time_off(
    technician_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get time off requests for a technician"""
    
    filters = [("technician_id", "eq", technician_id)]
    
    if start_date:
        filters.append(("start_date", "gte", start_date))
    
    if end_date:
        filters.append(("end_date", "lte", end_date))
    
    time_off = sb_select("time_off_requests", filters=filters)
    
    # Flatten date ranges to individual dates
    expanded = []
    for entry in time_off:
        from datetime import datetime, timedelta
        
        start = datetime.strptime(entry['start_date'], '%Y-%m-%d').date()
        end = datetime.strptime(entry['end_date'], '%Y-%m-%d').date()
        
        current = start
        while current <= end:
            expanded.append({
                "date": str(current),
                "hours_per_day": float(entry.get('hours_per_day', 0)),
                "reason": entry.get('reason', '')
            })
            current += timedelta(days=1)
    
    return {
        "technician_id": technician_id,
        "time_off": expanded
    }

@app.post("/api/timeoff/save")
def save_time_off(req: SaveTimeOffRequest):
    """
    Save time off entries for a technician.
    This will replace existing entries for the same dates.
    """
    
    if not req.time_off:
        return {"success": True, "message": "No time off to save"}
    
    tech_id = req.time_off[0].technician_id
    
    try:
        # Delete existing entries for these dates
        sb = supabase_client()
        
        for entry in req.time_off:
            # Delete existing entry for this date
            sb.table("time_off_requests").delete()\
                .eq("technician_id", tech_id)\
                .eq("start_date", entry.date)\
                .eq("end_date", entry.date)\
                .execute()
            
            # Insert new entry
            sb_insert("time_off_requests", [{
                "technician_id": tech_id,
                "start_date": entry.date,
                "end_date": entry.date,
                "hours_per_day": float(entry.hours_per_day),
                "reason": entry.reason or "Time off",
                "approved": True  # Auto-approve for now
            }])
        
        return {
            "success": True,
            "message": f"Saved {len(req.time_off)} time off entries"
        }
    
    except Exception as e:
        raise HTTPException(500, f"Failed to save time off: {str(e)}")

# ============================================================================
# HELPER FUNCTION FOR SCHEDULING
# ============================================================================

def check_tech_available(tech_id: int, date_str: str) -> dict:
    """
    Check if a technician is available on a specific date.
    Returns: {
        "available": bool,
        "hours_available": float,
        "reason": str (if not available)
    }
    """
    
    # Check if tech is active
    tech = sb_select("technicians", filters=[
        ("technician_id", "eq", tech_id)
    ])
    
    if not tech or not tech[0].get('active', True):
        return {
            "available": False,
            "hours_available": 0,
            "reason": "Technician inactive"
        }
    
    # Check time off
    time_off = sb_select("time_off_requests", filters=[
        ("technician_id", "eq", tech_id),
        ("start_date", "lte", date_str),
        ("end_date", "gte", date_str)
    ])
    
    if time_off:
        entry = time_off[0]
        # FIXED: hours_per_day stores HOURS AVAILABLE (not hours off)
        # 0 = full day off, 4 = 4 hours available, 8 = full day available
        hours_available = float(entry.get('hours_per_day', 0))
        
        if hours_available <= 0:
            return {
                "available": False,
                "hours_available": 0,
                "reason": entry.get('reason', 'Time off')
            }
        
        return {
            "available": True,
            "hours_available": hours_available,
            "reason": f"Partial day: {hours_available}h available"
        }
    
    # Fully available
    return {
        "available": True,
        "hours_available": float(tech[0].get('max_daily_hours', 10)),
        "reason": None
    }

# ============================================================================
# ENDPOINT TO GET TECH AVAILABILITY FOR WEEK (FOR UI)
# ============================================================================

@app.get("/api/technicians/availability")
def get_tech_availability(tech_id: int, week_start: str):
    """
    Get availability for a tech for a specific week.
    Used by the scheduler UI to show which days are blocked.
    """
    
    from datetime import datetime, timedelta
    
    week_start_date = datetime.strptime(week_start, '%Y-%m-%d').date()
    
    availability = []
    
    for i in range(7):
        date = week_start_date + timedelta(days=i)
        date_str = str(date)
        
        avail = check_tech_available(tech_id, date_str)
        
        availability.append({
            "date": date_str,
            "day_name": date.strftime('%A'),
            **avail
        })
    
    return {
        "technician_id": tech_id,
        "week_start": week_start,
        "availability": availability
    }
# ============================================
# DATA MANAGER ENDPOINTS
# ============================================

@app.get("/api/job/{work_order}")
async def get_single_job(work_order: int):
    """
    Get a single job by work order number
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        result = sb.table('job_pool').select('*').eq('work_order', work_order).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return result.data[0]
        
    except Exception as e:
        print(f"Get job error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs/all")
async def get_all_jobs(
    work_order: Optional[List[int]] = Query(None),
    due_date_start: Optional[str] = None,
    due_date_end: Optional[str] = None,
    limit: int = 1000
):
    """
    Get jobs from job_pool with optional filtering
    - work_order: List of specific work orders to fetch
    - due_date_start/end: Date range filtering  
    - limit: Max results (default 1000)
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        query = sb.table('job_pool').select('*')
        
        # Priority: If specific work orders requested, only get those
        if work_order and len(work_order) > 0:
            query = query.in_('work_order', work_order)
        else:
            # Otherwise apply date filters
            if due_date_start:
                query = query.gte('due_date', due_date_start)
            if due_date_end:
                query = query.lte('due_date', due_date_end)
            
            # Apply limit and order
            query = query.order('due_date').limit(limit)
        
        result = query.execute()
        return result.data
        
    except Exception as e:
        print(f"Get all jobs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class ArchiveJobRequest(BaseModel):
    work_order: int
    reason: str

@app.post("/api/archive-job")
async def archive_job(request: ArchiveJobRequest):
    """
    Archive a job (move to job_archive table)
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # First get the job data
        job_result = sb.table('job_pool').select('*').eq('work_order', request.work_order).execute()
        
        if not job_result.data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job_data = job_result.data[0]
        
        # Map job_pool columns to job_archive columns
        # job_archive uses different column names and has extra fields
        archive_data = {
            'work_order': job_data['work_order'],
            'site_name': job_data.get('site_name'),
            'site_id': job_data.get('site_id'),
            'address': job_data.get('site_address'),  # job_pool: site_address -> job_archive: address
            'site_city': job_data.get('site_city'),
            'site_state': job_data.get('site_state'),
            'site_zip': None,  # Not in job_pool
            'site_latitude': job_data.get('latitude'),  # job_pool: latitude -> job_archive: site_latitude
            'site_longitude': job_data.get('longitude'),  # job_pool: longitude -> job_archive: site_longitude
            'due_date': job_data.get('due_date'),
            'sow_1': job_data.get('sow_1'),
            'sow_2': None,  # Not in job_pool
            'jp_status': job_data.get('jp_status'),
            'eligible_technicians': None,  # Not in job_pool (could query job_technician_eligibility if needed)
            'archived_date': datetime.now().isoformat(),
            'archive_reason': request.reason,
            'archived_by': 'system'  # You can update this with actual user
        }
        
        # Insert into job_archive table
        sb.table('job_archive').insert(archive_data).execute()
        
        # Remove from scheduled_jobs if exists
        sb.table('scheduled_jobs').delete().eq('work_order', request.work_order).execute()
        
        # Remove from job_pool
        sb.table('job_pool').delete().eq('work_order', request.work_order).execute()
        
        return {
            "success": True,
            "message": f"Job {request.work_order} archived successfully"
        }
        
    except Exception as e:
        print(f"Archive job error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class ScheduleItem(BaseModel):
    work_order: int
    technician_id: int
    date: str  # YYYY-MM-DD format
    is_primary: int = 1

class BulkScheduleRequest(BaseModel):
    schedules: List[ScheduleItem]

@app.post("/api/bulk-schedule")
async def bulk_schedule_jobs(request: BulkScheduleRequest):
    """
    Bulk schedule jobs from Excel import
    Moves jobs from job_pool to scheduled_jobs
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        scheduled_count = 0
        skipped_count = 0
        errors = []
        
        for item in request.schedules:
            try:
                # Get job details from job_pool
                job_result = sb.table('job_pool').select('*').eq('work_order', item.work_order).execute()
                
                if not job_result.data:
                    errors.append(f"WO {item.work_order} not found in job_pool")
                    skipped_count += 1
                    continue
                
                job_data = job_result.data[0]
                
                # Check if already scheduled
                existing = sb.table('scheduled_jobs').select('work_order').eq('work_order', item.work_order).execute()
                if existing.data:
                    errors.append(f"WO {item.work_order} already scheduled")
                    skipped_count += 1
                    continue
                
                # Prepare scheduled_job data
                scheduled_job = {
                    'work_order': item.work_order,
                    'technician_id': item.technician_id,
                    'date': item.date,
                    'site_name': job_data.get('site_name'),
                    'site_city': job_data.get('site_city'),
                    'site_state': job_data.get('site_state'),
                    'latitude': job_data.get('latitude'),
                    'longitude': job_data.get('longitude'),
                    'sow_1': job_data.get('sow_1'),
                    'due_date': job_data.get('due_date'),
                    'duration': float(job_data.get('duration', 2.0)),
                    'site_id': job_data.get('site_id'),
                    'is_night_job': job_data.get('night_test', False)
                }
                
                # Insert into scheduled_jobs
                sb.table('scheduled_jobs').insert(scheduled_job).execute()
                
                # Update job_pool status
                sb.table('job_pool').update({
                    'jp_status': 'Scheduled',
                    'updated_at': datetime.now().isoformat()
                }).eq('work_order', item.work_order).execute()
                
                scheduled_count += 1
                
            except Exception as e:
                errors.append(f"WO {item.work_order}: {str(e)[:100]}")
                skipped_count += 1
        
        return {
            "success": True,
            "scheduled_count": scheduled_count,
            "skipped_count": skipped_count,
            "total_processed": len(request.schedules),
            "errors": errors[:500] if errors else None,  # Return first 50 errors
            "message": f"Successfully scheduled {scheduled_count} of {len(request.schedules)} jobs"
        }
        
    except Exception as e:
        print(f"Bulk schedule error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/clear-scheduled-jobs")
async def clear_scheduled_jobs():
    """
    Clear all scheduled jobs (useful for reimporting)
    WARNING: This will delete ALL scheduled jobs!
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # First, get count
        count_result = sb.table('scheduled_jobs').select('work_order', count='exact').execute()
        count = count_result.count if hasattr(count_result, 'count') else 0
        
        if count == 0:
            return {
                "success": True,
                "message": "No scheduled jobs to clear"
            }
        
        # Clear scheduled_jobs table
        sb.table('scheduled_jobs').delete().neq('work_order', 0).execute()  # Delete all
        
        # Reset all job_pool statuses
        sb.table('job_pool').update({
            'jp_status': 'Call',
            'updated_at': datetime.now().isoformat()
        }).eq('jp_status', 'Scheduled').execute()
        
        return {
            "success": True,
            "cleared_count": count,
            "message": f"Cleared {count} scheduled jobs and reset job_pool statuses"
        }
        
    except Exception as e:
        print(f"Clear scheduled jobs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/schedule/fillin")
async def api_schedule_fillin(request: dict):
    try:
        tech_id = int(request['tech_id'])
        week_start = request['week_start']
        sow_filter = request.get('sow_filter')
        target_weekly_hours = float(request.get('target_weekly_hours', 40))
        
        week_date = datetime.strptime(week_start, '%Y-%m-%d').date()
        
        result = schedule_week_fillin(
            tech_id=tech_id,
            week_start=week_date,
            sow_filter=sow_filter,
            target_weekly_hours=target_weekly_hours
        )
        
        return result
        
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }   


@app.post("/api/schedule/historical")
async def api_schedule_historical(request: dict):
    """
    Generate schedule suggestions based on historical patterns.
    Looks at job_history to find route patterns and suggests groupings.
    """
    try:
        from scheduler_historical import match_jobs_to_history
        
        tech_id = int(request['tech_id'])
        week_start = request['week_start']
        
        week_date = datetime.strptime(week_start, '%Y-%m-%d').date()
        
        result = match_jobs_to_history(
            tech_id=tech_id,
            week_start=week_date
        )
        
        return result
        
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }


@app.get("/api/schedule/historical-routes")
async def api_get_historical_routes(
    tech_id: int = Query(...),
    week_start: str = Query(...)
):
    """
    Get historical route patterns for display/comparison.
    Shows what was done in previous years for the same time period.
    """
    try:
        from scheduler_historical import get_historical_routes_for_display
        
        week_date = datetime.strptime(week_start, '%Y-%m-%d').date()
        
        result = get_historical_routes_for_display(
            tech_id=tech_id,
            week_start=week_date
        )
        
        return result
        
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }


@app.get("/api/schedule-import-status")
async def get_schedule_import_status():
    """
    Get status of scheduled vs unscheduled jobs
    Useful after import to verify
    """
    try:
        from supabase_client import supabase_client
        sb = supabase_client()
        
        # Get counts
        total_jobs = sb.table('job_pool').select('work_order', count='exact').execute()
        scheduled = sb.table('scheduled_jobs').select('work_order', count='exact').execute()
        unscheduled = sb.table('job_pool').select('work_order', count='exact').eq('jp_status', 'Call').execute()
        
        # Get date range of scheduled jobs
        date_range = sb.table('scheduled_jobs').select('date').order('date').execute()
        
        min_date = None
        max_date = None
        if date_range.data:
            min_date = min(job['date'] for job in date_range.data if job['date'])
            max_date = max(job['date'] for job in date_range.data if job['date'])
        
        return {
            "total_jobs": total_jobs.count if hasattr(total_jobs, 'count') else 0,
            "scheduled_count": scheduled.count if hasattr(scheduled, 'count') else 0,
            "unscheduled_count": unscheduled.count if hasattr(unscheduled, 'count') else 0,
            "schedule_date_range": {
                "earliest": min_date,
                "latest": max_date
            },
            "import_ready": True
        }
        
    except Exception as e:
        print(f"Import status error: {e}")
        return {
            "error": str(e),
            "import_ready": False
        }


# ============================================================================
# RUN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
