# scheduler_api_unified.py - CLEAN UNIFIED API
import os
import threading
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
from route_template_builder import (
    get_last_month_routes,
    find_historically_paired_sites,
    match_sites_to_current_jobs,
    get_nearby_annuals,
    build_pool_from_template
)
_db_semaphore = threading.Semaphore(10)

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
            "schedule": f"{BASE_URL}/api/schedule/week-all"
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
class SiteVisitWindowResponse(BaseModel):
    site_id: int
    site_name: Optional[str]
    visit_cycle: Optional[str]
    last_visit_date: Optional[str]
    last_visit_source: Optional[str]
    earliest_schedule: Optional[str]
    optimal_target: Optional[str]
    latest_schedule: Optional[str]
    days_since_last_visit: Optional[int]
    window_status: Optional[str]
    scheduling_recommendation: Optional[str]
class BatchSiteIdsRequest(BaseModel):
    site_ids: List[int]
class UpdateVisitCycleRequest(BaseModel):
    site_id: int
    visit_cycle: str  # 'monthly', 'quarterly', 'annual', 'on-demand', or null

@app.get("/api/sites/visit-window/{site_id}")
def get_site_visit_window(site_id: int):
    """
    Get the visit window for a specific site.
    Returns scheduling window information based on last visit.
    """
    import time as time_module
    max_retries = 3
    retry_delay = 0.1
    
    with _db_semaphore:
        for attempt in range(max_retries):
            try:
                sb = supabase_client()
                result = sb.rpc('get_site_visit_window', {'p_site_id': site_id}).execute()
                
                if not result.data:
                    site = sb.table('sites').select('*').eq('site_id', site_id).execute()
                    if site.data:
                        return {
                            "site_id": site_id,
                            "site_name": site.data[0].get('site_name'),
                            "visit_cycle": None,
                            "window_status": "not_tracked",
                            "scheduling_recommendation": "Site not set up for recurring visits"
                        }
                    raise HTTPException(404, f"Site {site_id} not found")
                
                return result.data[0]
                
            except HTTPException:
                raise
            except Exception as e:
                error_str = str(e)
                if "10035" in error_str or "non-blocking socket" in error_str.lower():
                    if attempt < max_retries - 1:
                        time_module.sleep(retry_delay * (attempt + 1))
                        continue
                print(f"Error getting site visit window: {e}")
                raise HTTPException(500, str(e))


@app.get("/api/sites/visit-windows")
def get_all_site_visit_windows(
    window_status: Optional[str] = Query(None, description="Filter by status: too_soon, optimal, urgent, overdue, unknown"),
    within_days: int = Query(30, description="Show sites due within this many days"),
    include_overdue: bool = Query(True, description="Include overdue sites")
):
    """
    Get visit windows for all recurring sites.
    Useful for planning and identifying sites that need attention.
    """
    try:
        sb = supabase_client()
        
        # First, refresh all windows
        sb.rpc('update_site_visit_windows').execute()
        
        # Build query
        query = sb.table('site_visit_windows').select('*')
        
        if window_status:
            query = query.eq('window_status', window_status)
        
        # Order by urgency
        query = query.order('latest_schedule')
        
        result = query.execute()
        
        # Filter by within_days if not filtering by status
        if not window_status and result.data:
            from datetime import datetime, timedelta
            cutoff_date = (datetime.now().date() + timedelta(days=within_days)).isoformat()
            filtered = []
            for row in result.data:
                # Include if within date range OR overdue/urgent
                if row.get('latest_schedule') and row['latest_schedule'] <= cutoff_date:
                    filtered.append(row)
                elif include_overdue and row.get('window_status') in ('overdue', 'urgent', 'unknown'):
                    filtered.append(row)
            result.data = filtered
        
        return {
            "success": True,
            "count": len(result.data),
            "windows": result.data
        }
        
    except Exception as e:
        print(f"Error getting site visit windows: {e}")
        raise HTTPException(500, str(e))


@app.get("/api/sites/needing-visits")
def get_sites_needing_visits(
    within_days: int = Query(14, description="Days to look ahead"),
    include_overdue: bool = Query(True)
):
    """
    Get list of sites that need to be scheduled soon.
    Calls the database function for efficient querying.
    """
    try:
        sb = supabase_client()
        
        result = sb.rpc('get_sites_needing_visits', {
            'p_within_days': within_days,
            'p_include_overdue': include_overdue
        }).execute()
        
        return {
            "success": True,
            "count": len(result.data) if result.data else 0,
            "sites": result.data or []
        }
        
    except Exception as e:
        print(f"Error getting sites needing visits: {e}")
        raise HTTPException(500, str(e))

@app.post("/api/sites/visit-windows-batch")
def get_site_visit_windows_batch(request: BatchSiteIdsRequest):
    """
    Get visit windows for multiple sites in a single call.
    Accepts up to 100 site IDs per request.
    """
    try:
        if len(request.site_ids) > 100:
            raise HTTPException(400, "Maximum 100 site IDs per request")
        
        if not request.site_ids:
            return {"success": True, "windows": {}}
        
        sb = supabase_client()
        results = {}
        
        for site_id in request.site_ids:
            try:
                result = sb.rpc('get_site_visit_window', {'p_site_id': site_id}).execute()
                if result.data:
                    results[site_id] = result.data[0]
                else:
                    results[site_id] = {
                        "site_id": site_id,
                        "visit_cycle": None,
                        "window_status": "not_tracked"
                    }
            except Exception as e:
                results[site_id] = {
                    "site_id": site_id,
                    "error": str(e),
                    "window_status": "error"
                }
        
        return {
            "success": True,
            "count": len(results),
            "windows": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting batch site visit windows: {e}")
        raise HTTPException(500, str(e))

@app.post("/api/sites/visit-cycle")
def update_site_visit_cycle(request: UpdateVisitCycleRequest):
    """
    Set or update the visit cycle for a site.
    Valid values: 'monthly', 'quarterly', 'annual', 'on-demand', null
    """
    try:
        sb = supabase_client()
        
        # Validate visit_cycle value
        valid_cycles = ['monthly', 'quarterly', 'annual', 'on-demand', None, '']
        if request.visit_cycle not in valid_cycles:
            raise HTTPException(400, f"Invalid visit_cycle. Must be one of: {valid_cycles}")
        
        # Update the sites table
        cycle_value = request.visit_cycle if request.visit_cycle else None
        result = sb.table('sites')\
            .update({'visit_cycle': cycle_value})\
            .eq('site_id', request.site_id)\
            .execute()
        
        if not result.data:
            raise HTTPException(404, f"Site {request.site_id} not found")
        
        # Refresh the visit window for this site
        sb.rpc('update_site_visit_windows', {'p_site_ids': [request.site_id]}).execute()
        
        return {
            "success": True,
            "site_id": request.site_id,
            "visit_cycle": cycle_value,
            "message": f"Visit cycle updated to {cycle_value or 'not tracked'}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating site visit cycle: {e}")
        raise HTTPException(500, str(e))


@app.post("/api/sites/refresh-windows")
def refresh_site_visit_windows(site_ids: Optional[List[int]] = None):
    """
    Manually refresh visit windows for specific sites or all sites.
    Useful after bulk imports or manual data corrections.
    """
    try:
        sb = supabase_client()
        
        if site_ids:
            result = sb.rpc('update_site_visit_windows', {'p_site_ids': site_ids}).execute()
        else:
            result = sb.rpc('update_site_visit_windows').execute()
        
        return {
            "success": True,
            "result": result.data[0] if result.data else {"sites_updated": 0, "sites_skipped": 0}
        }
        
    except Exception as e:
        print(f"Error refreshing windows: {e}")
        raise HTTPException(500, str(e))


@app.get("/api/sites/visit-windows/bulk")
def get_bulk_visit_windows(site_ids: List[int] = Query(...)):
    """
    Get visit windows for multiple specific sites at once.
    Useful for map popups when loading multiple markers.
    """
    try:
        sb = supabase_client()
        
        if not site_ids:
            return {"success": True, "windows": {}}
        
        # Refresh windows for these sites
        sb.rpc('update_site_visit_windows', {'p_site_ids': site_ids}).execute()
        
        # Get the windows
        result = sb.table('site_visit_windows')\
            .select('*')\
            .in_('site_id', site_ids)\
            .execute()
        
        # Return as dictionary keyed by site_id for easy frontend lookup
        windows_dict = {row['site_id']: row for row in (result.data or [])}
        
        return {
            "success": True,
            "windows": windows_dict
        }
        
    except Exception as e:
        print(f"Error getting bulk windows: {e}")
        raise HTTPException(500, str(e))


# ============================================================================
# HELPER: Add visit window info to job queries
# ============================================================================
# You can modify existing job endpoints to include visit window info
# Example modification for get_unscheduled_jobs:

def enrich_jobs_with_visit_windows(jobs: List[Dict], sb) -> List[Dict]:
    """
    Helper function to add visit window info to a list of jobs.
    Call this after fetching jobs to add window data.
    """
    if not jobs:
        return jobs
    
    # Get unique site_ids
    site_ids = list(set(j.get('site_id') for j in jobs if j.get('site_id')))
    
    if not site_ids:
        return jobs
    
    # Fetch windows for these sites
    try:
        result = sb.table('site_visit_windows')\
            .select('site_id, visit_cycle, last_visit_date, window_status, earliest_schedule, latest_schedule, days_since_last_visit')\
            .in_('site_id', site_ids)\
            .execute()
        
        # Create lookup dict
        windows = {row['site_id']: row for row in (result.data or [])}
        
        # Enrich jobs
        for job in jobs:
            site_id = job.get('site_id')
            if site_id and site_id in windows:
                window = windows[site_id]
                job['visit_window'] = {
                    'visit_cycle': window.get('visit_cycle'),
                    'last_visit_date': window.get('last_visit_date'),
                    'window_status': window.get('window_status'),
                    'earliest_schedule': window.get('earliest_schedule'),
                    'latest_schedule': window.get('latest_schedule'),
                    'days_since_last_visit': window.get('days_since_last_visit')
                }
            else:
                job['visit_window'] = None
                
    except Exception as e:
        print(f"Warning: Could not enrich jobs with visit windows: {e}")
        # Don't fail - just return jobs without window info
    
    return jobs

class AssignJobRequest(BaseModel):
    work_order: int
    technician_id: int
    date: str  # YYYY-MM-DD
    start_time: Optional[str] = None

frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/static", StaticFiles(directory=frontend_path), name="static")

frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")

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

@app.get("/schedule-viewer", response_class=HTMLResponse)
def serve_schedule_viewer():
    html_path = os.path.join(frontend_dir, "schedule-viewer.html")
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()
    raise HTTPException(404, "schedule-viewer.html not found")


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
    """Get all unscheduled jobs with eligibility info and visit windows"""
    
    from datetime import datetime, timedelta
    
    print(f"\n DEBUG get_unscheduled_jobs:")
    print(f"  start_date received: {start_date}")
    print(f"  end_date received: {end_date}")
    
    # Build filters list
    filters = [("jp_status", "in", ["Call", "Waiting to Schedule"])]
    
    # Add date filters if provided
    if start_date:
        filters.append(("due_date", "gte", start_date))
        print(f"   Added start filter: due_date >= {start_date}")
    if end_date:
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        next_day = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
        filters.append(("due_date", "lt", next_day))
        print(f"   Added end filter: due_date < {next_day}")
    print(f"  Final filters: {filters}")
    
    # Get jobs with filters
    jobs = sb_select("job_pool", filters=filters)
    print(f"   Jobs returned: {len(jobs)}")
    
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
    
    # === BATCH FETCH VISIT WINDOWS (1 query instead of 500+) ===
    site_ids = list(set(j.get('site_id') for j in jobs if j.get('site_id')))
    window_lookup = {}
    if site_ids:
        try:
            sb = supabase_client()
            windows = sb.table('site_visit_windows').select('*').in_('site_id', site_ids).execute()
            window_lookup = {w['site_id']: w for w in (windows.data or [])}
            print(f"   Fetched {len(window_lookup)} visit windows in batch")
        except Exception as e:
            print(f"   Warning: Could not fetch visit windows: {e}")
    
    # === BATCH FETCH ELIGIBILITY (1 query instead of 500+) ===
    work_orders = [j["work_order"] for j in jobs]
    eligibility_lookup = {}
    if work_orders:
        try:
            all_elig = sb_select("job_technician_eligibility", filters=[
                ("work_order", "in", work_orders)
            ])
            for e in all_elig:
                wo = e["work_order"]
                if wo not in eligibility_lookup:
                    eligibility_lookup[wo] = []
                eligibility_lookup[wo].append(e["technician_id"])
            print(f"   Fetched eligibility for {len(eligibility_lookup)} jobs in batch")
        except Exception as e:
            print(f"   Warning: Could not fetch eligibility: {e}")
    
    # Add metadata to each job
    for job in jobs:
        # Attach eligibility
        wo = job["work_order"]
        elig_techs = eligibility_lookup.get(wo, [])
        job["eligible_tech_count"] = len(elig_techs)
        job["eligible_tech_ids"] = elig_techs
        
        # Attach visit window
        sid = job.get('site_id')
        if sid and sid in window_lookup:
            w = window_lookup[sid]
            job['visit_window'] = {
                'last_visit_date': w.get('last_visit_date'),
                'earliest_schedule': w.get('earliest_schedule'),
                'optimal_target': w.get('optimal_target'),
                'latest_schedule': w.get('latest_schedule'),
                'window_status': w.get('window_status'),
                'visit_cycle': w.get('visit_cycle')
            }
        else:
            job['visit_window'] = None
        
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
        pri = job.get("jp_priority", "Unknown")
        summary["by_priority"][pri] = summary["by_priority"].get(pri, 0) + 1
        
        reg = job.get("site_state", "Unknown")
        summary["by_region"][reg] = summary["by_region"].get(reg, 0) + 1
        
        urg = job.get("urgency", "normal")
        summary["by_urgency"][urg] = summary["by_urgency"].get(urg, 0) + 1
    
    print(f"   Returning {len(jobs)} jobs to frontend\n")
    return {
        "count": len(jobs),
        "jobs": jobs,
        "summary": summary
    }
# ----------------------------------------------------------------------------
# SCHEDULE OPERATIONS
# ----------------------------------------------------------------------------

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
    month_end: str,
    sow_filter: Optional[str] = None
):
    """Get all eligible jobs in a region for a technician"""
    sb = supabase_client()
    
    result = sb.rpc(
        'get_all_jobs_in_region',
        {
            'p_tech_id': tech_id,
            'p_region_name': region,
            'p_month_start': month_start,
            'p_month_end': month_end,
            'p_sow_filter': sow_filter
        }
    ).execute()
    
    jobs = result.data or []
    print(f"  /api/jobs/region: tech={tech_id}, region={region}, found {len(jobs)} jobs")
    
    return {
        "jobs": jobs,
        "count": len(jobs),
        "tech_id": tech_id,
        "region": region
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
# ============================================================================
# ROUTE TEMPLATE BUILDER ENDPOINTS
# Add these to scheduler_api.py after the existing historical routes endpoints
# ============================================================================

@app.get("/api/route-templates/last-month")
def api_get_last_month_routes(reference_date: str = None):
    """
    Get routes from last month grouped by tech + week.
    These serve as templates for building job pools.
    
    Args:
        reference_date: Optional. The date you're scheduling FOR (YYYY-MM-DD).
                       Defaults to today. System looks ~4 weeks back.
    
    Returns:
        List of route templates with site IDs, regions, and totals.
    """
    try:
        from route_template_builder import get_last_month_routes
        return get_last_month_routes(reference_date)
    except Exception as e:
        print(f"Error getting last month routes: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.get("/api/route-templates/{route_id}/historical-pairings")
def api_get_historical_pairings(
    route_id: str,
    years_back: int = 3,
    min_overlap: int = 3,
    week_flexibility: int = 1
):
    """
    Find sites that were historically done with the given route's sites.
    
    Args:
        route_id: The route template ID (e.g., "5_2024_W50")
        years_back: How many years to search (default 3)
        min_overlap: Minimum site overlap to consider a match (default 3)
        week_flexibility: +/- weeks to search around target week (default 1)
    """
    try:
        from route_template_builder import get_last_month_routes, find_historically_paired_sites
        
        # Get the route to find its site_ids and week_number
        routes_data = get_last_month_routes()
        route = None
        for r in routes_data.get('routes', []):
            if r['route_id'] == route_id:
                route = r
                break
        
        if not route:
            raise HTTPException(404, f"Route {route_id} not found")
        
        return find_historically_paired_sites(
            route['site_ids'],
            route['week_number'],
            years_back=years_back,
            min_overlap=min_overlap,
            week_flexibility=week_flexibility
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting historical pairings: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


class BuildPoolRequest(BaseModel):
    route_id: str
    reference_date: Optional[str] = None  # YYYY-MM-DD - the date we're scheduling FOR
    due_date_end: Optional[str] = None    # YYYY-MM-DD - only include jobs due on or before this date
    priority_within_days: int = 10
    max_annual_distance: float = 50
    years_back: int = 3
    min_historical_overlap: int = 3


@app.post("/api/route-templates/build-pool")
def api_build_pool_from_template(request: BuildPoolRequest):
    """
    Build a complete job pool from a route template.
    
    Combines:
    1. Current MOI jobs for template sites
    2. Historically paired annuals (with current work orders)
    3. Nearby annuals due within the specified window
    
    Returns categorized pool ready for scheduling.
    """
    try:
        from route_template_builder import build_pool_from_template
        
        return build_pool_from_template(
            route_id=request.route_id,
            reference_date=request.reference_date,
            due_date_end=request.due_date_end,
            priority_within_days=request.priority_within_days,
            max_annual_distance=request.max_annual_distance,
            years_back=request.years_back,
            min_historical_overlap=request.min_historical_overlap
        )
    except Exception as e:
        print(f"Error building pool from template: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.get("/api/route-templates/nearby-annuals")
def api_get_nearby_annuals(
    site_ids: str,  # Comma-separated list
    due_within_days: int = 30,
    priority_within_days: int = 10,
    max_distance: float = 50
):
    """
    Get annual jobs near a set of sites.
    Useful for manually adding annuals to a pool.
    
    Args:
        site_ids: Comma-separated list of site IDs
        due_within_days: Include jobs due within this window
        priority_within_days: Flag jobs due within this as priority
        max_distance: Max distance in miles from route center
    """
    try:
        from route_template_builder import get_nearby_annuals
        
        site_id_list = [int(s.strip()) for s in site_ids.split(',') if s.strip()]
        
        if not site_id_list:
            raise HTTPException(400, "No valid site IDs provided")
        
        return get_nearby_annuals(
            site_id_list,
            due_within_days=due_within_days,
            priority_within_days=priority_within_days,
            max_distance_miles=max_distance
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting nearby annuals: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


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
        
        # Import haversine from scheduler_utils
        from scheduler_utils import haversine
        
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


@app.get("/api/technicians/availability-batch")
def get_all_techs_availability_batch(week_start: str):
    """
    Get availability for ALL active technicians in one call.
    Returns nested dict: { availability: { tech_id: { day_name: {...} } } }
    
    This is much faster than calling /api/technicians/availability for each tech.
    """
    try:
        from datetime import datetime, timedelta
        
        start_date = datetime.strptime(week_start, "%Y-%m-%d").date()
        end_date = start_date + timedelta(days=4)  # Mon-Fri
        
        # Get all active technicians
        techs = sb_select("technicians", filters=[("active", "eq", True)])
        if not techs:
            return {"availability": {}}
        
        tech_ids = [t['technician_id'] for t in techs]
        
        # Get all time off requests for these techs in date range - ONE query instead of 12!
        sb = supabase_client()
        time_off_result = sb.table('time_off_requests')\
            .select('*')\
            .in_('technician_id', tech_ids)\
            .lte('start_date', str(end_date))\
            .gte('end_date', str(start_date))\
            .execute()
        
        # Build availability map
        availability = {}
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        
        for tech in techs:
            tech_id = tech['technician_id']
            availability[tech_id] = {}
            max_hours = float(tech.get('max_daily_hours', 10))
            
            for i, day_name in enumerate(days):
                check_date = start_date + timedelta(days=i)
                
                # Check if tech has time off on this day
                time_off = None
                for to in (time_off_result.data or []):
                    if to['technician_id'] == tech_id:
                        to_start = datetime.strptime(to['start_date'], "%Y-%m-%d").date()
                        to_end = datetime.strptime(to['end_date'], "%Y-%m-%d").date()
                        if to_start <= check_date <= to_end:
                            time_off = to
                            break
                
                if time_off:
                    # hours_per_day stores HOURS AVAILABLE (not hours off)
                    # 0 = full day off, 4 = 4 hours available, 8 = full day available
                    hours_available = float(time_off.get('hours_per_day', 0))
                    
                    if hours_available <= 0:
                        availability[tech_id][day_name] = {
                            'available': False,
                            'hours_available': 0,
                            'reason': time_off.get('reason', 'Time off')
                        }
                    else:
                        availability[tech_id][day_name] = {
                            'available': True,
                            'hours_available': hours_available,
                            'reason': f"Partial day: {hours_available}h available"
                        }
                else:
                    availability[tech_id][day_name] = {
                        'available': True,
                        'hours_available': max_hours,
                        'reason': None
                    }
        
        return {"availability": availability}
        
    except Exception as e:
        print(f"Error in availability-batch: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


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


# ============================================================================
# RUN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
