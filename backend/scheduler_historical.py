"""
HISTORICAL PATTERN SCHEDULER
Analyzes job_history to find route patterns and suggest schedules
"""
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass

from supabase_client import supabase_client


@dataclass
class HistoricalRoute:
    """A group of sites that were done together historically"""
    site_ids: List[int]
    site_names: List[str]
    region: str
    total_duration: float
    historical_dates: List[date]  # When this route was done in history
    avg_week_of_year: float  # Average week number when done
    confidence: float  # How consistently these were grouped (0-1)


@dataclass 
class SuggestedJob:
    """A job suggested for scheduling"""
    work_order: int
    site_id: int
    site_name: str
    region: str
    duration: float
    due_date: date
    jp_priority: str
    sow_1: str
    latitude: float
    longitude: float
    historical_note: str  # "Done with X, Y, Z last year"
    suggested_day: str  # Monday, Tuesday, etc.
    confidence: float


def get_historical_patterns(
    region: str,
    month: int,
    day_window: int = 15
) -> Dict[int, List[Dict]]:
    """
    Get historical job patterns for a region around a specific time of year.
    
    Returns dict: {site_id: [list of historical occurrences]}
    """
    sb = supabase_client()
    
    patterns = defaultdict(list)
    
    # Look at same month across all years in history
    for year in [2023, 2024, 2025]:
        # Build date range around the target month
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)
        
        result = sb.table('job_history')\
            .select('*')\
            .eq('region', region)\
            .gte('scheduled_date', str(start_date))\
            .lte('scheduled_date', str(end_date))\
            .order('scheduled_date')\
            .execute()
        
        if result.data:
            for row in result.data:
                patterns[row['site_id']].append({
                    'scheduled_date': row['scheduled_date'],
                    'technician_id': row['technician_id'],
                    'duration': row['duration'],
                    'year': year
                })
    
    return dict(patterns)


def find_route_groups(
    region: str,
    target_date: date,
    lookback_days: int = 10
) -> List[HistoricalRoute]:
    """
    Find groups of sites that were historically done together.
    
    Logic:
    - Look at jobs done on the same day in history
    - Weight by recency: last year 50%, 2 years 30%, 3 years 20%
    - Sites done together multiple times = high confidence grouping
    """
    sb = supabase_client()
    
    routes = []
    
    # Get target week/day of year for comparison
    target_week = target_date.isocalendar()[1]
    target_month = target_date.month
    
    # Pull history for this time of year across all years
    all_daily_groups = []
    
    for year_offset, weight in [(1, 0.50), (2, 0.30), (3, 0.20)]:
        history_year = target_date.year - year_offset
        
        # Look at 2 weeks around the target week
        history_start = date(history_year, target_month, 1) - timedelta(days=7)
        history_end = date(history_year, target_month, 28) + timedelta(days=7)
        
        result = sb.table('job_history')\
            .select('*')\
            .eq('region', region)\
            .gte('scheduled_date', str(history_start))\
            .lte('scheduled_date', str(history_end))\
            .order('scheduled_date')\
            .execute()
        
        if result.data:
            # Group by date
            by_date = defaultdict(list)
            for row in result.data:
                by_date[row['scheduled_date']].append(row)
            
            for sched_date, jobs in by_date.items():
                if len(jobs) >= 1:
                    all_daily_groups.append({
                        'date': sched_date,
                        'year': history_year,
                        'weight': weight,
                        'site_ids': [j['site_id'] for j in jobs],
                        'site_names': [j['site_name'] for j in jobs],
                        'total_duration': sum(j['duration'] or 2 for j in jobs)
                    })
    
    # Now find sites that appear together frequently
    site_cooccurrence = defaultdict(lambda: defaultdict(float))
    
    for group in all_daily_groups:
        site_ids = group['site_ids']
        weight = group['weight']
        
        # Record co-occurrence for each pair
        for i, site_a in enumerate(site_ids):
            for site_b in site_ids[i+1:]:
                site_cooccurrence[site_a][site_b] += weight
                site_cooccurrence[site_b][site_a] += weight
    
    # Build route clusters from high co-occurrence
    # For now, just return the daily groups as potential routes
    seen_sites = set()
    
    for group in sorted(all_daily_groups, key=lambda x: -x['weight']):
        # Skip if we've already included these sites
        new_sites = [s for s in group['site_ids'] if s not in seen_sites]
        if not new_sites:
            continue
        
        routes.append(HistoricalRoute(
            site_ids=group['site_ids'],
            site_names=group['site_names'],
            region=region,
            total_duration=group['total_duration'],
            historical_dates=[datetime.strptime(group['date'], '%Y-%m-%d').date()],
            avg_week_of_year=datetime.strptime(group['date'], '%Y-%m-%d').isocalendar()[1],
            confidence=group['weight']
        ))
        
        seen_sites.update(group['site_ids'])
    
    return routes


def match_jobs_to_history(
    tech_id: int,
    week_start: date
) -> Dict:
    """
    Main function: Match current job_pool to historical patterns.
    
    Returns suggested schedule with historical context.
    """
    sb = supabase_client()
    
    # Get tech info
    tech_result = sb.table('technicians').select('*').eq('technician_id', tech_id).execute()
    if not tech_result.data:
        return {"error": f"Tech {tech_id} not found"}
    tech = tech_result.data[0]
    
    week_end = week_start + timedelta(days=4)
    month_start = date(week_start.year, week_start.month, 1)
    if week_start.month == 12:
        month_end = date(week_start.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(week_start.year, week_start.month + 1, 1) - timedelta(days=1)
    
    # Get eligible jobs for this tech
    jobs_result = sb.rpc(
        'get_jobs_for_scheduling',
        {
            'p_tech_id': tech_id,
            'p_month_start': str(month_start),
            'p_month_end': str(month_end)
        }
    ).execute()
    
    if not jobs_result.data:
        return {
            "success": True,
            "tech_id": tech_id,
            "tech_name": tech['name'],
            "week_start": str(week_start),
            "message": "No eligible jobs found",
            "suggested_schedule": {}
        }
    
    eligible_jobs = {j['work_order']: j for j in jobs_result.data}
    site_to_jobs = defaultdict(list)
    for job in jobs_result.data:
        if job.get('site_id'):
            site_to_jobs[job['site_id']].append(job)
    
    # Get regions represented in eligible jobs
    regions = set(j['region'] for j in jobs_result.data if j.get('region'))
    
    # For each region, find historical patterns
    all_suggestions = []
    
    for region in regions:
        routes = find_route_groups(region, week_start)
        
        for route in routes:
            # Find matching jobs in current pool
            matching_jobs = []
            for site_id in route.site_ids:
                if site_id in site_to_jobs:
                    for job in site_to_jobs[site_id]:
                        matching_jobs.append({
                            'work_order': job['work_order'],
                            'site_id': site_id,
                            'site_name': job['site_name'],
                            'region': region,
                            'duration': job.get('duration', 2.0),
                            'due_date': job['due_date'],
                            'jp_priority': job.get('jp_priority', 'Standard'),
                            'sow_1': job.get('sow_1', ''),
                            'latitude': job.get('latitude', 0),
                            'longitude': job.get('longitude', 0),
                            'historical_note': f"Grouped with {len(route.site_names)-1} other sites in history",
                            'route_sites': route.site_names,
                            'confidence': route.confidence
                        })
            
            if matching_jobs:
                all_suggestions.extend(matching_jobs)
    
    # Also add jobs without historical matches
    matched_work_orders = {s['work_order'] for s in all_suggestions}
    for wo, job in eligible_jobs.items():
        if wo not in matched_work_orders:
            all_suggestions.append({
                'work_order': wo,
                'site_id': job.get('site_id'),
                'site_name': job['site_name'],
                'region': job.get('region', 'Unknown'),
                'duration': job.get('duration', 2.0),
                'due_date': job['due_date'],
                'jp_priority': job.get('jp_priority', 'Standard'),
                'sow_1': job.get('sow_1', ''),
                'latitude': job.get('latitude', 0),
                'longitude': job.get('longitude', 0),
                'historical_note': 'No historical pattern found',
                'route_sites': [],
                'confidence': 0
            })
    
    # Sort by due date, then by confidence
    all_suggestions.sort(key=lambda x: (x['due_date'], -x['confidence']))
    
    # Distribute into days based on due dates and capacity
    schedule = {
        'Monday': {'date': str(week_start), 'jobs': [], 'total_hours': 0},
        'Tuesday': {'date': str(week_start + timedelta(days=1)), 'jobs': [], 'total_hours': 0},
        'Wednesday': {'date': str(week_start + timedelta(days=2)), 'jobs': [], 'total_hours': 0},
        'Thursday': {'date': str(week_start + timedelta(days=3)), 'jobs': [], 'total_hours': 0},
        'Friday': {'date': str(week_start + timedelta(days=4)), 'jobs': [], 'total_hours': 0},
    }
    
    max_hours = 10.0
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    
    for suggestion in all_suggestions:
        # Find best day based on due date
        due = datetime.strptime(suggestion['due_date'], '%Y-%m-%d').date() if isinstance(suggestion['due_date'], str) else suggestion['due_date']
        
        # Prefer earlier days for earlier due dates
        if due <= week_start:
            preferred_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        elif due <= week_start + timedelta(days=2):
            preferred_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        elif due <= week_start + timedelta(days=4):
            preferred_days = ['Wednesday', 'Thursday', 'Friday', 'Monday', 'Tuesday']
        else:
            preferred_days = ['Thursday', 'Friday', 'Wednesday', 'Tuesday', 'Monday']
        
        # Find first day with capacity
        placed = False
        for day in preferred_days:
            if schedule[day]['total_hours'] + suggestion['duration'] <= max_hours:
                schedule[day]['jobs'].append(suggestion)
                schedule[day]['total_hours'] += suggestion['duration']
                placed = True
                break
        
        if not placed:
            # Put in overflow / couldn't fit
            pass
    
    return {
        "success": True,
        "tech_id": tech_id,
        "tech_name": tech['name'],
        "week_start": str(week_start),
        "suggested_schedule": schedule,
        "total_jobs": len(all_suggestions),
        "jobs_with_history": len([s for s in all_suggestions if s['confidence'] > 0])
    }


def get_historical_routes_for_display(
    tech_id: int,
    week_start: date
) -> Dict:
    """
    Get historical route patterns for UI display.
    Shows what routes were done in previous years for comparison.
    """
    sb = supabase_client()
    
    target_month = week_start.month
    target_week = week_start.isocalendar()[1]
    
    historical_by_year = {}
    
    for year_offset in [1, 2, 3]:
        history_year = week_start.year - year_offset
        
        # Get history for same week-ish
        history_start = date(history_year, target_month, 1)
        if target_month == 12:
            history_end = date(history_year + 1, 1, 1) - timedelta(days=1)
        else:
            history_end = date(history_year, target_month + 1, 1) - timedelta(days=1)
        
        result = sb.table('job_history')\
            .select('*')\
            .gte('scheduled_date', str(history_start))\
            .lte('scheduled_date', str(history_end))\
            .order('scheduled_date')\
            .order('technician_id')\
            .execute()
        
        if result.data:
            # Group by date and tech
            by_date_tech = defaultdict(list)
            for row in result.data:
                key = (row['scheduled_date'], row['technician_id'])
                by_date_tech[key].append({
                    'site_name': row['site_name'],
                    'site_id': row['site_id'],
                    'region': row['region'],
                    'duration': row['duration']
                })
            
            historical_by_year[history_year] = [
                {
                    'date': k[0],
                    'tech_id': k[1],
                    'jobs': v,
                    'total_hours': sum(j['duration'] or 2 for j in v)
                }
                for k, v in by_date_tech.items()
            ]
    
    return {
        "target_week": str(week_start),
        "historical_patterns": historical_by_year
    }
