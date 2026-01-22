"""
ROUTE TEMPLATE BUILDER
Builds job pools from last month's routes + historically paired annuals

Workflow:
1. Get last month's routes (grouped by tech + week)
2. For each route, find historically paired annuals (3+ site overlap, +/- 1 week, 3 years back)
3. Match sites to current work orders in job_pool
4. Return enriched pool for scheduling
"""
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass
from math import cos, radians, sin, asin, sqrt
from supabase_client import supabase_client


@dataclass
class RouteTemplate:
    """A route from last month that can be used as a template"""
    route_id: str  # e.g., "tech_5_week_50"
    technician_id: int
    technician_name: str
    week_number: int
    week_start: str  # YYYY-MM-DD
    week_end: str
    year: int
    month: int
    site_ids: List[int]
    site_names: List[str]
    regions: List[str]
    total_jobs: int
    total_hours: float


def get_week_number(d: date) -> int:
    """Get ISO week number for a date"""
    return d.isocalendar()[1]


def get_week_start_end(year: int, week_number: int) -> Tuple[date, date]:
    """Get the Monday and Friday of a given ISO week"""
    # Find Jan 4 (always in week 1) and work from there
    jan4 = date(year, 1, 4)
    week1_monday = jan4 - timedelta(days=jan4.weekday())
    target_monday = week1_monday + timedelta(weeks=week_number - 1)
    target_friday = target_monday + timedelta(days=4)
    return target_monday, target_friday


def get_last_month_routes(reference_date: str = None) -> Dict:
    """
    Get routes from approximately 4 weeks ago (last month's equivalent week).
    Groups jobs by technician + week to identify route patterns.
    
    Args:
        reference_date: The date we're scheduling FOR (YYYY-MM-DD). 
                       We'll look ~4 weeks back from this date.
    
    Returns:
        Dict with routes grouped by tech and week
    """
    sb = supabase_client()
    
    # Parse reference date or use today
    if reference_date:
        ref_date = datetime.strptime(reference_date, '%Y-%m-%d').date()
    else:
        ref_date = date.today()
    
    # Look back ~4 weeks (28-35 days to catch the equivalent week)
    lookback_start = ref_date - timedelta(days=35)
    lookback_end = ref_date - timedelta(days=21)
    
    print(f"Looking for routes between {lookback_start} and {lookback_end}")
    
    # Get scheduled jobs for that period
    # Query scheduled_jobs table which has actual scheduled work
    result = sb.table('scheduled_jobs')\
        .select('work_order, site_id, site_name, date, technician_id, sow_1, duration, latitude, longitude, site_city, site_state')\
        .gte('date', str(lookback_start))\
        .lte('date', str(lookback_end))\
        .order('date')\
        .execute()
    
    if not result.data:
        # Fallback: try job_history if scheduled_jobs is empty
        print("No data in scheduled_jobs, trying job_history...")
        result = sb.table('job_history')\
            .select('*')\
            .gte('scheduled_date', str(lookback_start))\
            .lte('scheduled_date', str(lookback_end))\
            .order('scheduled_date')\
            .execute()
        
        if result.data:
            # Rename scheduled_date to date for consistency
            for row in result.data:
                row['date'] = row.get('scheduled_date')
    
    if not result.data:
        return {
            "success": True,
            "routes": [],
            "message": "No historical data found for last month",
            "lookback_range": {
                "start": str(lookback_start),
                "end": str(lookback_end)
            }
        }
    
    # Get regions for site_ids from sites table
    site_ids = list(set(j.get('site_id') for j in result.data if j.get('site_id')))
    site_regions = {}
    if site_ids:
        sites_result = sb.table('sites')\
            .select('site_id, region')\
            .in_('site_id', site_ids)\
            .execute()
        if sites_result.data:
            site_regions = {s['site_id']: s.get('region') for s in sites_result.data}
    
    # Add region to each job
    for job in result.data:
        if not job.get('region') and job.get('site_id'):
            job['region'] = site_regions.get(job['site_id'])
    
    # Get technician names
    tech_result = sb.table('technicians').select('technician_id, name').execute()
    tech_names = {t['technician_id']: t['name'] for t in (tech_result.data or [])}
    
    # Group by technician + week
    routes_by_tech_week = defaultdict(lambda: {
        'jobs': [],
        'site_ids': set(),
        'site_names': [],
        'regions': set(),
        'dates': set()
    })
    
    for job in result.data:
        tech_id = job.get('technician_id')
        # Use 'date' field (from scheduled_jobs) or 'scheduled_date' (from job_history)
        date_str = job.get('date') or job.get('scheduled_date')
        if not date_str:
            continue
        sched_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        week_num = get_week_number(sched_date)
        year = sched_date.year
        
        key = f"{tech_id}_{year}_W{week_num}"
        
        routes_by_tech_week[key]['jobs'].append(job)
        if job.get('site_id'):
            routes_by_tech_week[key]['site_ids'].add(job['site_id'])
        routes_by_tech_week[key]['site_names'].append(job.get('site_name', 'Unknown'))
        if job.get('region'):
            routes_by_tech_week[key]['regions'].add(job['region'])
        routes_by_tech_week[key]['dates'].add(date_str)
    
    # Convert to list of RouteTemplate objects
    routes = []
    for key, data in routes_by_tech_week.items():
        parts = key.split('_')
        tech_id = int(parts[0])
        year = int(parts[1])
        week_num = int(parts[2].replace('W', ''))
        
        # Get week boundaries
        week_start, week_end = get_week_start_end(year, week_num)
        
        # Calculate total hours
        total_hours = sum(j.get('duration', 2) or 2 for j in data['jobs'])
        
        routes.append({
            'route_id': key,
            'technician_id': tech_id,
            'technician_name': tech_names.get(tech_id, f'Tech {tech_id}'),
            'week_number': week_num,
            'week_start': str(week_start),
            'week_end': str(week_end),
            'year': year,
            'month': week_start.month,
            'site_ids': list(data['site_ids']),
            'site_names': list(set(data['site_names'])),  # Dedupe
            'regions': list(data['regions']),
            'total_jobs': len(data['jobs']),
            'total_hours': total_hours,
            'dates_worked': sorted(list(data['dates']))
        })
    
    # Sort by total_jobs descending (biggest routes first)
    routes.sort(key=lambda r: -r['total_jobs'])
    
    return {
        "success": True,
        "routes": routes,
        "total_routes": len(routes),
        "lookback_range": {
            "start": str(lookback_start),
            "end": str(lookback_end)
        }
    }


def find_historically_paired_sites(
    site_ids: List[int],
    reference_week: int,
    years_back: int = 3,
    min_overlap: int = 3,
    week_flexibility: int = 1
) -> Dict:
    """
    Find sites that were historically done with the given site cluster.
    
    Args:
        site_ids: The MOI sites from the selected route template
        reference_week: ISO week number to search around
        years_back: How many years of history to search
        min_overlap: Minimum number of sites that must match to consider it the same route
        week_flexibility: +/- weeks to search (1 = check weeks 49, 50, 51 for week 50)
    
    Returns:
        Dict with historically paired sites and their frequency
    """
    sb = supabase_client()
    
    if not site_ids:
        return {"success": False, "error": "No site IDs provided"}
    
    current_year = date.today().year
    
    # Build list of weeks to check
    weeks_to_check = []
    for week_offset in range(-week_flexibility, week_flexibility + 1):
        weeks_to_check.append(reference_week + week_offset)
    
    # Handle week number wraparound (week 0 -> 52, week 53 -> 1)
    weeks_to_check = [w if 1 <= w <= 52 else (w % 52) or 52 for w in weeks_to_check]
    
    print(f"Searching for historical pairings in weeks {weeks_to_check} across {years_back} years")
    
    # Find all historical weeks that have significant overlap with our sites
    matching_weeks = []
    paired_sites = defaultdict(lambda: {'count': 0, 'site_name': None, 'sow_1': None, 'region': None})
    
    for year_offset in range(1, years_back + 1):
        history_year = current_year - year_offset
        
        for week_num in weeks_to_check:
            # Get the date range for this week
            try:
                week_start, week_end = get_week_start_end(history_year, week_num)
            except:
                continue
            
            # Query jobs from this week
            result = sb.table('job_history')\
                .select('*')\
                .gte('scheduled_date', str(week_start))\
                .lte('scheduled_date', str(week_end))\
                .execute()
            
            if not result.data:
                continue
            
            # Group by technician for this week
            by_tech = defaultdict(list)
            for job in result.data:
                tech_id = job.get('technician_id')
                by_tech[tech_id].append(job)
            
            # Check each tech's week for overlap with our site_ids
            for tech_id, tech_jobs in by_tech.items():
                tech_site_ids = set(j.get('site_id') for j in tech_jobs if j.get('site_id'))
                
                # Count overlap
                overlap = tech_site_ids.intersection(set(site_ids))
                overlap_count = len(overlap)
                
                if overlap_count >= min_overlap:
                    # This is a matching route! Record the OTHER sites
                    matching_weeks.append({
                        'year': history_year,
                        'week': week_num,
                        'tech_id': tech_id,
                        'overlap_count': overlap_count,
                        'total_jobs': len(tech_jobs)
                    })
                    
                    # Find sites that are NOT in our original cluster
                    for job in tech_jobs:
                        job_site_id = job.get('site_id')
                        if job_site_id and job_site_id not in site_ids:
                            # This is a historically paired site!
                            paired_sites[job_site_id]['count'] += 1
                            paired_sites[job_site_id]['site_name'] = job.get('site_name')
                            paired_sites[job_site_id]['sow_1'] = job.get('sow_1')
                            paired_sites[job_site_id]['region'] = job.get('region')
    
    # Convert to list and sort by frequency
    paired_list = [
        {
            'site_id': site_id,
            'site_name': data['site_name'],
            'sow_1': data['sow_1'],
            'region': data['region'],
            'times_paired': data['count']
        }
        for site_id, data in paired_sites.items()
    ]
    paired_list.sort(key=lambda x: -x['times_paired'])
    
    return {
        "success": True,
        "reference_week": reference_week,
        "weeks_searched": weeks_to_check,
        "years_searched": list(range(current_year - years_back, current_year)),
        "matching_historical_weeks": matching_weeks,
        "historically_paired_sites": paired_list,
        "total_paired_sites": len(paired_list)
    }


def match_sites_to_current_jobs(site_ids: List[int], due_date_end: str = None) -> Dict:
    """
    Find current work orders in job_pool for the given site IDs.
    
    Returns jobs that are available for scheduling (not already scheduled).
    
    Args:
        site_ids: List of site IDs to find jobs for
        due_date_end: Optional. Only include jobs due on or before this date (YYYY-MM-DD)
    """
    sb = supabase_client()
    
    if not site_ids:
        return {"success": False, "error": "No site IDs provided"}
    
    # Query job_pool for these sites
    # Only get jobs that are ready to schedule (Call or Waiting to Schedule)
    query = sb.table('job_pool')\
        .select('*')\
        .in_('site_id', site_ids)\
        .in_('jp_status', ['Call', 'Waiting to Schedule'])
    
    # Apply due date filter if provided
    if due_date_end:
        query = query.lte('due_date', due_date_end)
    
    result = query.execute()
    
    found_jobs = result.data or []
    
    # Track which sites have jobs vs missing
    found_site_ids = set(j.get('site_id') for j in found_jobs if j.get('site_id'))
    missing_site_ids = set(site_ids) - found_site_ids
    
    # Get site info for missing ones
    missing_sites = []
    if missing_site_ids:
        sites_result = sb.table('sites')\
            .select('site_id, site_name, region')\
            .in_('site_id', list(missing_site_ids))\
            .execute()
        missing_sites = sites_result.data or []
    
    return {
        "success": True,
        "jobs": found_jobs,
        "jobs_found": len(found_jobs),
        "missing_sites": missing_sites,
        "missing_count": len(missing_site_ids)
    }


def get_nearby_annuals(
    site_ids: List[int],
    due_date_end: str = None,
    priority_within_days: int = 10,
    max_distance_miles: float = 50
) -> Dict:
    """
    Find annual jobs that are:
    1. Due on or before due_date_end
    2. Geographically near the given sites
    
    Uses PostGIS for efficient proximity search.
    
    Args:
        site_ids: List of site IDs to calculate center point from
        due_date_end: Only include jobs due on or before this date (YYYY-MM-DD)
        priority_within_days: Flag jobs due within this many days as priority
        max_distance_miles: Max distance from route center
    """
    sb = supabase_client()
    
    if not site_ids:
        return {"success": False, "error": "No site IDs provided"}
    
    # Get the center point of the route (average of site locations)
    sites_result = sb.table('sites')\
        .select('latitude, longitude')\
        .in_('site_id', site_ids)\
        .execute()
    
    if not sites_result.data:
        return {"success": False, "error": "Could not find site locations"}
    
    # Calculate centroid
    lats = [s['latitude'] for s in sites_result.data if s.get('latitude')]
    lons = [s['longitude'] for s in sites_result.data if s.get('longitude')]
    
    if not lats or not lons:
        return {"success": False, "error": "No valid coordinates for sites"}
    
    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)
    
    # Calculate date range
    today = date.today()
    priority_cutoff = today + timedelta(days=priority_within_days)
    
    # Use provided due_date_end or default to 30 days from today
    if due_date_end:
        due_cutoff = due_date_end
    else:
        due_cutoff = str(today + timedelta(days=30))
    
    # Query for annual jobs near the centroid
    # Using a bounding box first, then calculate actual distance
    # Rough conversion: 1 degree lat ~ 69 miles, 1 degree lon ~ varies by latitude
    lat_range = max_distance_miles / 69
    lon_range = max_distance_miles / (69 * abs(cos(center_lat * 3.14159 / 180)) or 1)
    
    result = sb.table('job_pool')\
        .select('*')\
        .in_('jp_status', ['Call', 'Waiting to Schedule'])\
        .lte('due_date', due_cutoff)\
        .gte('latitude', center_lat - lat_range)\
        .lte('latitude', center_lat + lat_range)\
        .gte('longitude', center_lon - lon_range)\
        .lte('longitude', center_lon + lon_range)\
        .execute()
    
    if not result.data:
        return {
            "success": True,
            "jobs": [],
            "center": {"lat": center_lat, "lon": center_lon}
        }
    
    # Filter out MOI jobs (we only want annuals) and jobs already in our site list
    # Also calculate actual distance and priority flag
    
    def haversine(lat1, lon1, lat2, lon2):
        """Calculate distance in miles between two points"""
        R = 3959  # Earth radius in miles
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        return 2 * R * asin(sqrt(a))
    
    annuals = []
    for job in result.data:
        # Skip if it's an MOI job
        sow = job.get('sow_1', '') or ''
        if 'MOI' in sow.upper():
            continue
        
        # Skip if it's one of the template sites
        if job.get('site_id') in site_ids:
            continue
        
        # Calculate actual distance
        job_lat = job.get('latitude')
        job_lon = job.get('longitude')
        if job_lat and job_lon:
            distance = haversine(center_lat, center_lon, float(job_lat), float(job_lon))
            
            if distance <= max_distance_miles:
                # Check if priority (due within priority window)
                due_date = job.get('due_date')
                is_priority = False
                if due_date:
                    due_dt = datetime.strptime(due_date, '%Y-%m-%d').date()
                    is_priority = due_dt <= priority_cutoff
                
                annuals.append({
                    **job,
                    'distance_from_route_center': round(distance, 1),
                    'is_priority': is_priority
                })
    
    # Sort: priority first, then by distance
    annuals.sort(key=lambda x: (not x['is_priority'], x['distance_from_route_center']))
    
    return {
        "success": True,
        "jobs": annuals,
        "total_found": len(annuals),
        "priority_count": len([j for j in annuals if j['is_priority']]),
        "center": {"lat": center_lat, "lon": center_lon},
        "search_radius_miles": max_distance_miles
    }


def build_pool_from_template(
    route_id: str,
    reference_date: str = None,
    due_date_end: str = None,
    priority_within_days: int = 10,
    max_annual_distance: float = 50,
    years_back: int = 3,
    min_historical_overlap: int = 3
) -> Dict:
    """
    Main function: Build a complete job pool from a route template.
    
    Combines:
    1. Current MOI jobs for template sites
    2. Historically paired annuals (if they have current work orders)
    3. Nearby annuals due soon
    
    Args:
        route_id: The route template ID (e.g., "tech_5_2024_W50")
        reference_date: The date we're scheduling FOR (YYYY-MM-DD) - needed to find the right routes
        due_date_end: Only include jobs due on or before this date (YYYY-MM-DD)
        priority_within_days: Flag annuals due within this many days as priority
        max_annual_distance: Max distance in miles for nearby annuals
        years_back: How many years to search for historical pairings
        min_historical_overlap: Minimum site overlap to consider a historical match
    
    Returns:
        Dict with categorized job pool
    """
    # First, get the route template details (pass reference_date to look at the same date range)
    routes_data = get_last_month_routes(reference_date)
    
    if not routes_data.get('success') or not routes_data.get('routes'):
        return {"success": False, "error": "Could not load route templates"}
    
    # Find the specific route
    route = None
    for r in routes_data['routes']:
        if r['route_id'] == route_id:
            route = r
            break
    
    if not route:
        return {"success": False, "error": f"Route {route_id} not found"}
    
    template_site_ids = route['site_ids']
    week_number = route['week_number']
    
    print(f"Building pool from route {route_id}")
    print(f"Template has {len(template_site_ids)} sites: {route['site_names'][:5]}...")
    if due_date_end:
        print(f"Filtering jobs due on or before: {due_date_end}")
    
    # Step 1: Get current MOI jobs for template sites
    moi_jobs_result = match_sites_to_current_jobs(template_site_ids, due_date_end=due_date_end)
    moi_jobs = moi_jobs_result.get('jobs', [])
    missing_moi_sites = moi_jobs_result.get('missing_sites', [])
    
    print(f"Found {len(moi_jobs)} current MOI jobs, {len(missing_moi_sites)} sites missing work orders")
    
    # Step 2: Find historically paired sites
    historical_result = find_historically_paired_sites(
        template_site_ids,
        week_number,
        years_back=years_back,
        min_overlap=min_historical_overlap
    )
    historically_paired = historical_result.get('historically_paired_sites', [])
    
    print(f"Found {len(historically_paired)} historically paired sites")
    
    # Get current jobs for historically paired sites
    historical_site_ids = [s['site_id'] for s in historically_paired if s.get('site_id')]
    historical_jobs_result = match_sites_to_current_jobs(historical_site_ids, due_date_end=due_date_end)
    historical_jobs = historical_jobs_result.get('jobs', [])
    
    # Add pairing frequency to jobs
    pairing_counts = {s['site_id']: s['times_paired'] for s in historically_paired}
    for job in historical_jobs:
        job['times_paired_historically'] = pairing_counts.get(job.get('site_id'), 0)
    
    print(f"Found {len(historical_jobs)} current jobs for historically paired sites")
    
    # Step 3: Get nearby annuals
    all_pool_site_ids = template_site_ids + historical_site_ids
    nearby_result = get_nearby_annuals(
        all_pool_site_ids,
        due_date_end=due_date_end,
        priority_within_days=priority_within_days,
        max_distance_miles=max_annual_distance
    )
    nearby_annuals = nearby_result.get('jobs', [])
    
    # Filter out any jobs already in our pool
    existing_work_orders = set(j['work_order'] for j in moi_jobs + historical_jobs)
    nearby_annuals = [j for j in nearby_annuals if j['work_order'] not in existing_work_orders]
    
    print(f"Found {len(nearby_annuals)} nearby annuals")
    
    # Compile final pool
    return {
        "success": True,
        "route_template": route,
        "pool": {
            "moi_jobs": moi_jobs,
            "moi_missing_sites": missing_moi_sites,
            "historical_jobs": historical_jobs,
            "nearby_annuals": nearby_annuals
        },
        "summary": {
            "total_moi_jobs": len(moi_jobs),
            "total_historical_jobs": len(historical_jobs),
            "total_nearby_annuals": len(nearby_annuals),
            "total_pool_size": len(moi_jobs) + len(historical_jobs) + len(nearby_annuals),
            "missing_moi_sites": len(missing_moi_sites),
            "priority_annuals": len([j for j in nearby_annuals if j.get('is_priority')])
        },
        "search_params": {
            "due_date_end": due_date_end,
            "priority_within_days": priority_within_days,
            "max_annual_distance": max_annual_distance,
            "years_back": years_back,
            "min_historical_overlap": min_historical_overlap
        }
    }



