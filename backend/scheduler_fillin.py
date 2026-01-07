"""
SCHEDULER FILL-IN MODE
Smart gap-filling with intelligent routing based on drive times
"""
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass

from supabase_client import supabase_client
from scheduler_utils import (
    Job, ScheduledJob, haversine, calculate_drive_time, 
    calculate_start_times, get_tech_home_location, 
    check_time_off, parse_time, estimate_job_end_time
)

# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class DayCapacity:
    """Represents available capacity on a day"""
    date: date
    day_name: str
    existing_jobs: List[ScheduledJob]
    hours_scheduled: float
    hours_available: float
    time_gaps: List[Tuple[str, str, float]]
    primary_region: Optional[str]
    last_job_location: Optional[Tuple[float, float]]

@dataclass
class FutureScheduledJob:
    """Track future scheduled jobs for routing decisions"""
    day_name: str
    location: Tuple[float, float]
    region: str
    drive_hours_from_home: float

# ============================================================================
# EXISTING SCHEDULE ANALYSIS
# ============================================================================

def get_existing_schedule(tech_id: int, week_start: date, week_end: date) -> Dict[str, List[ScheduledJob]]:
    """Get all jobs already scheduled for tech during the week"""
    global _job_regions_cache
    _job_regions_cache = {}  # Clear cache for each run
    sb = supabase_client()


    result = sb.table('scheduled_jobs')\
        .select('*, job_pool!inner(region)')\
        .eq('technician_id', tech_id)\
        .gte('date', str(week_start))\
        .lte('date', str(week_end))\
        .order('date', desc=False)\
        .order('start_time', desc=False)\
        .execute()
    
    schedule_by_day = {
        'Monday': [], 'Tuesday': [], 'Wednesday': [], 
        'Thursday': [], 'Friday': []
    }
    
    job_regions = {}
    
    if not result.data:
        return schedule_by_day
    
    for row in result.data:
        job_date = datetime.strptime(row['date'], '%Y-%m-%d').date()
        day_name = job_date.strftime('%A')
        
        region = row.get('job_pool', {}).get('region') if isinstance(row.get('job_pool'), dict) else None
        if region:
            job_regions[row['work_order']] = region
        
        scheduled_job = ScheduledJob(
            work_order=row['work_order'],
            site_name=row['site_name'],
            site_city=row.get('site_city', 'Unknown'),
            technician_id=row['technician_id'],
            date=row['date'],
            due_date=row.get('due_date'),
            duration=float(row.get('duration', 2.0)),
            sow_1=row.get('sow_1'),
            latitude=float(row.get('latitude', 0)),
            longitude=float(row.get('longitude', 0)),
            start_time=parse_time(row.get('start_time')),
            end_time=parse_time(row.get('end_time'))
        )
        
        schedule_by_day[day_name].append(scheduled_job) 
    
    # CRITICAL FIX: Copy local job_regions to global cache
    _job_regions_cache.update(job_regions)
   
    return schedule_by_day
_job_regions_cache = {}  # ADD THIS LINE AT MODULE LEVEL 

def analyze_day_capacity(
    day_date: date,
    day_name: str,
    existing_jobs: List[ScheduledJob],
    tech_home: Tuple[float, float],
    max_daily_hours: float = 12.0
) -> DayCapacity:
    """Analyze how much capacity is available on a day"""
    
    if not existing_jobs:
        return DayCapacity(
            date=day_date,
            day_name=day_name,
            existing_jobs=[],
            hours_scheduled=0,
            hours_available=max_daily_hours,
            time_gaps=[('07:00', '19:00', max_daily_hours)],
            primary_region=None,
            last_job_location=None
        )
    
    hours_scheduled = sum(job.duration for job in existing_jobs)
    
    drive_hours = 0
    for i in range(len(existing_jobs) - 1):
        current = existing_jobs[i]
        next_job = existing_jobs[i + 1]
        distance = haversine(current.latitude, current.longitude, 
                           next_job.latitude, next_job.longitude)
        drive_hours += calculate_drive_time(distance)
    
    if existing_jobs:
        first_job = existing_jobs[0]
        distance_from_home = haversine(tech_home[0], tech_home[1],
                                       first_job.latitude, first_job.longitude)
        drive_hours += calculate_drive_time(distance_from_home)
    
    total_scheduled = hours_scheduled + drive_hours
    hours_available = max(0, max_daily_hours - total_scheduled)
    
    regions = []
    for job in existing_jobs:
        if job.work_order in _job_regions_cache:
            regions.append(_job_regions_cache[job.work_order])
    
    primary_region = max(set(regions), key=regions.count) if regions else None
    
    last_job = existing_jobs[-1]
    last_location = (last_job.latitude, last_job.longitude)
    
    return DayCapacity(
        date=day_date,
        day_name=day_name,
        existing_jobs=existing_jobs,
        hours_scheduled=hours_scheduled,
        hours_available=hours_available,
        time_gaps=[],
        primary_region=primary_region,
        last_job_location=last_location
    )

# ============================================================================
# SMART ROUTING ANALYSIS
# ============================================================================

def analyze_future_jobs(
    existing_schedule: Dict[str, List[ScheduledJob]],
    tech_home: Tuple[float, float],
    weekdays: List[str]
) -> List[FutureScheduledJob]:
    """Analyze future scheduled jobs to make routing decisions"""
    future_jobs = []
    
    for day_name in weekdays:
        if existing_schedule[day_name]:
            # Get first job of the day (where tech needs to be)
            first_job = existing_schedule[day_name][0]
            location = (first_job.latitude, first_job.longitude)
            
            # Calculate drive time from home
            distance = haversine(tech_home[0], tech_home[1], 
                               location[0], location[1])
            drive_hours = calculate_drive_time(distance)
            
            # Get region
            region = _job_regions_cache.get(first_job.work_order, 'Unknown')
            
            future_jobs.append(FutureScheduledJob(
                day_name=day_name,
                location=location,
                region=region,
                drive_hours_from_home=drive_hours
            ))
    
    return future_jobs

def should_go_early_to_region(
    drive_hours: float,
    days_until: int,
    current_location: Tuple[float, float],
    target_location: Tuple[float, float]
) -> bool:
    """
    Determine if we should go to a region early.
    ALWAYS go toward future scheduled work - don't stay home doing unrelated jobs.
    """
    return True
# ============================================================================
# FRESHNESS CHECK FUNCTIONS
# ============================================================================

def get_site_freshness(sb, site_ids: list, schedule_date) -> Dict[int, int]:
    """
    Check how many days since each site was last visited.
    IMPORTANT: Calculates from schedule_date, not today.
    
    Args:
        sb: Supabase client
        site_ids: List of site IDs to check
        schedule_date: The date we're scheduling FOR (date object or string)
    
    Returns dict: {site_id: days_since_last_visit}
    """
    if not site_ids:
        return {}
    
    # Convert date to string if needed
    if hasattr(schedule_date, 'isoformat'):
        schedule_date_str = schedule_date.isoformat()
    else:
        schedule_date_str = str(schedule_date)
    
    result = sb.rpc(
        'get_site_freshness_batch',
        {
            'p_site_ids': site_ids,
            'p_schedule_date': schedule_date_str
        }
    ).execute()
    
    if not result.data:
        return {sid: 9999 for sid in site_ids}
    
    freshness = {}
    for row in result.data:
        freshness[row['site_id']] = row['days_since']
    
    for sid in site_ids:
        if sid not in freshness:
            freshness[sid] = 9999
    
    return freshness


def filter_jobs_by_freshness(jobs: list, freshness: Dict[int, int], min_days: int = 18) -> list:
    """
    Remove jobs done too recently. Recurring sites always pass.
    """
    filtered = []
    for job in jobs:
        is_recurring = getattr(job, 'is_recurring_site', False)
        if is_recurring:
            filtered.append(job)
        elif freshness.get(job.site_id, 9999) >= min_days:
            filtered.append(job)
        else:
            print(f"      â­ï¸ Skipping {job.site_name} - done {freshness.get(job.site_id)} days ago (from schedule date)")
    return filtered
# ============================================================================
# CORRIDOR SCHEDULING LOGIC
# ============================================================================

def schedule_corridor_jobs(
    corridor_jobs: list,
    start_location: Tuple[float, float],
    end_location: Tuple[float, float],
    available_hours: float,
    travel_time_to_destination: float,
    destination_job_duration: float,
    destination_can_be_bumped: bool
) -> dict:
    """
    Decide which corridor jobs to schedule.
    
    Returns:
        {
            'jobs_to_schedule': [...],
            'bump_destination': True/False,
            'reason': 'explanation'
        }
    """
    if not corridor_jobs:
        return {'jobs_to_schedule': [], 'bump_destination': False, 'reason': 'No corridor jobs'}
    
    # Time needed for destination
    time_for_dest = travel_time_to_destination + destination_job_duration
    time_available_for_corridor = available_hours - time_for_dest
    
    # Sort by distance from start (route order)
    def dist_from_start(job):
        return haversine(start_location[0], start_location[1], job.latitude, job.longitude)
    
    corridor_sorted = sorted(corridor_jobs, key=dist_from_start)
    
    # Find best cluster (jobs within 15 miles of each other)
    clusters = []
    current_cluster = []
    
    for job in corridor_sorted:
        if not current_cluster:
            current_cluster.append(job)
        else:
            last = current_cluster[-1]
            if haversine(last.latitude, last.longitude, job.latitude, job.longitude) <= 15:
                current_cluster.append(job)
            else:
                if current_cluster:
                    clusters.append(current_cluster)
                current_cluster = [job]
    if current_cluster:
        clusters.append(current_cluster)
    
    # Sort clusters by total work time (biggest first)
    clusters.sort(key=lambda c: sum(j.duration for j in c), reverse=True)
    
    # Try to fit best cluster + destination
    for cluster in clusters:
        cluster_time = sum(j.duration for j in cluster)
        
        # Estimate drive through cluster
        cluster_drive = 0
        prev = start_location
        for job in cluster:
            cluster_drive += calculate_drive_time(haversine(prev[0], prev[1], job.latitude, job.longitude))
            prev = (job.latitude, job.longitude)
        
        # Drive from cluster end to destination
        last_job = cluster[-1]
        to_dest = calculate_drive_time(haversine(last_job.latitude, last_job.longitude, 
                                                  end_location[0], end_location[1]))
        
        total = cluster_time + cluster_drive + to_dest + destination_job_duration
        
        if total <= available_hours:
            return {
                'jobs_to_schedule': cluster,
                'bump_destination': False,
                'reason': f'Cluster of {len(cluster)} jobs ({cluster_time:.1f}h) fits with destination'
            }
    
    # Can't fit cluster + destination together
    if destination_can_be_bumped:
        # Bump destination, take biggest cluster that fits alone
        for cluster in clusters:
            cluster_time = sum(j.duration for j in cluster)
            cluster_drive = 0
            prev = start_location
            for job in cluster:
                cluster_drive += calculate_drive_time(haversine(prev[0], prev[1], job.latitude, job.longitude))
                prev = (job.latitude, job.longitude)
            
            if cluster_time + cluster_drive <= available_hours:
                return {
                    'jobs_to_schedule': cluster,
                    'bump_destination': True,
                    'reason': f'Bumping destination, doing {len(cluster)} corridor jobs ({cluster_time:.1f}h)'
                }
    
    # Can't bump - fit what we can around destination
    jobs_that_fit = []
    time_used = 0
    prev = start_location
    
    for job in corridor_sorted:
        drive_to = calculate_drive_time(haversine(prev[0], prev[1], job.latitude, job.longitude))
        job_to_dest = calculate_drive_time(haversine(job.latitude, job.longitude, 
                                                      end_location[0], end_location[1]))
        
        total_if_added = time_used + drive_to + job.duration + job_to_dest + destination_job_duration
        
        if total_if_added <= available_hours:
            jobs_that_fit.append(job)
            time_used += drive_to + job.duration
            prev = (job.latitude, job.longitude)
    
    return {
        'jobs_to_schedule': jobs_that_fit,
        'bump_destination': False,
        'reason': f'Fitted {len(jobs_that_fit)} jobs around fixed destination'
    }
# ============================================================================
# JOB FINDING FUNCTIONS
# ============================================================================

def find_jobs_along_corridor(
    sb,
    tech_id: int,
    start_location: Tuple[float, float],
    end_location: Tuple[float, float],
    schedule_date, 
    corridor_width: float = 30.0,
    already_scheduled: Set[int] = None,
) -> List[Job]:
    """Find jobs along the route between two points - filtered by tech eligibility at DB level"""
    
    if already_scheduled is None:
        already_scheduled = set()
    
    result = sb.rpc(
        'find_jobs_along_route',
        {
            'start_lat': float(start_location[0]),
            'start_lon': float(start_location[1]),
            'end_lat': float(end_location[0]),
            'end_lon': float(end_location[1]),
            'corridor_miles': float(corridor_width),
            'max_results': 50,
            'p_tech_id': tech_id,
            'p_schedule_date': str(schedule_date) if schedule_date else None
        }
    ).execute()
    
    if not result.data:
        return []
    
    # Database already filtered by tech eligibility - just convert to Job objects
    jobs = []
    for row in result.data:
        if row['work_order'] in already_scheduled:
            continue
        
        jobs.append(Job(
            work_order=row['work_order'],
            site_id=0,  # Not returned by function
            site_name=row['site_name'],
            site_city=row['site_city'],
            latitude=float(row['latitude']),
            longitude=float(row['longitude']),
            sow_1=row['sow_1'],
            due_date=row['due_date'],
            jp_priority=row.get('jp_priority', 'Standard'),
            duration=float(row.get('duration', 2.0)),
            is_recurring_site=False,
            is_night=False,
            night_test=row.get('night_test', False),
            days_til_due=0,  # Can calculate if needed
            priority_rank=5,
            distance_from_tech_home=row.get('distance_from_start_miles', 0)
        ))
    
    if jobs:
        site_ids = [j.site_id for j in jobs]
        freshness = get_site_freshness(sb, site_ids, schedule_date)
        jobs = filter_jobs_by_freshness(jobs, freshness, min_days=18)
    
    return jobs

def find_jobs_in_region(
    sb,
    tech_id: int,
    region: str,
    already_scheduled: Set[int] = None,
    month_start: date = None,
    month_end: date = None
) -> List[Job]:
    """Find all available jobs in a specific region - uses DB function with tech eligibility"""
    
    if already_scheduled is None:
        already_scheduled = set()
    
    # Use the database function that handles eligibility filtering
    result = sb.rpc(
        'get_all_jobs_in_region',
        {
            'p_tech_id': tech_id,
            'p_region_name': region,
            'p_month_start': str(month_start) if month_start else None,
            'p_month_end': str(month_end) if month_end else None,
            'p_sow_filter': None
        }
    ).execute()
    
    if not result.data:
        return []
    
    jobs = []
    for row in result.data:
        if row['work_order'] in already_scheduled:
            continue
        
        jobs.append(Job(
            work_order=row['work_order'],
            site_id=row.get('site_id', 0),
            site_name=row['site_name'],
            site_city=row['site_city'],
            latitude=float(row['latitude']),
            longitude=float(row['longitude']),
            sow_1=row['sow_1'],
            due_date=row['due_date'],
            jp_priority=row.get('jp_priority', 'Standard'),
            duration=float(row.get('duration', 2.0)),
            is_recurring_site=row.get('is_recurring_site', False),
            is_night=row.get('is_night', False),
            night_test=row.get('night_test', False),
            days_til_due=row.get('days_til_due', 0),
            priority_rank=row.get('priority_rank', 5),
            distance_from_tech_home=row.get('distance_from_tech_home', 0)
        ))
    
    return jobs

def find_nearest_job_any_region(
    sb,
    tech_id: int,
    from_location: Tuple[float, float],
    already_scheduled: Set[int],
    max_distance: float = 300.0,
    schedule_date: date = None
) -> Optional[Tuple[Job, str]]:
    """Find the nearest job from any region within max distance - filtered by tech eligibility at DB level"""
    
    result = sb.rpc(
        'find_nearby_jobs',
        {
            'center_lat': from_location[0],
            'center_lon': from_location[1],
            'radius_miles': max_distance,
            'max_results': 50,
            'p_tech_id': tech_id,
            'p_schedule_date': str(schedule_date) if schedule_date else None
        }
    ).execute()
    
    if not result.data:
        return None
    
    # Database already filtered by tech eligibility - find first not already scheduled
    for row in result.data:
        if row['work_order'] in already_scheduled:
            continue
        
        job = Job(
            work_order=row['work_order'],
            site_id=0,
            site_name=row['site_name'],
            site_city=row['site_city'],
            latitude=float(row['latitude']),
            longitude=float(row['longitude']),
            sow_1=row['sow_1'],
            due_date=row['due_date'],
            jp_priority=row.get('jp_priority', 'Standard'),
            duration=float(row.get('duration', 2.0)),
            is_recurring_site=row.get('is_recurring_site', False),
            is_night=False,
            night_test=row.get('night_test', False),
            days_til_due=row.get('days_til_due', 0),
            priority_rank=5,
            distance_from_tech_home=row['distance_miles']
        )
        return job, row['region']
    
    return None

# ============================================================================

# ============================================================================
# GREEDY GEOGRAPHIC FILL - GPS BASED
# ============================================================================

def fill_day_greedy_geographic(
    sb,
    tech_id: int,
    capacity: DayCapacity,
    start_location: Tuple[float, float],
    current_region: str,
    already_scheduled: Set[int],
    max_work_hours: float = 10.5,
    month_start: date = None,
    month_end: date = None,
    schedule_date: date = None
) -> Tuple[List[Job], float, float, str, Tuple[float, float]]:
    """
    Fill a day using region-based search with nearest-neighbor within region.
    Exhausts current region before switching to next nearest region.
    Returns: (jobs, work_hours, drive_hours, final_region, final_location)
    """
    scheduled_today = []
    current_location = start_location
    work_hours = 0
    drive_hours = 0
    
    # Get jobs in current region
    region_jobs = find_jobs_in_region(sb, tech_id, current_region, already_scheduled, month_start, month_end)
    
    # Apply freshness filter
    if region_jobs:
        site_ids = [j.site_id for j in region_jobs]
        freshness = get_site_freshness(sb, site_ids, schedule_date)
        region_jobs = filter_jobs_by_freshness(region_jobs, freshness, min_days=18)
        print(f"      Found {len(region_jobs)} jobs in {current_region}")

    while work_hours < max_work_hours and capacity.hours_available > (work_hours + drive_hours):
        remaining_hours = capacity.hours_available - work_hours - drive_hours
        
        if remaining_hours < 0.5:
            break
        
        # Find nearest job in current region that fits
        best_job = None
        best_distance = float('inf')
        
        for job in region_jobs:
            if job.work_order in already_scheduled:
                continue
            distance = haversine(current_location[0], current_location[1],
                               job.latitude, job.longitude)
            drive_time = calculate_drive_time(distance)
            
            # Check if it fits
            if drive_time + job.duration <= remaining_hours:
                if distance < best_distance:
                    best_distance = distance
                    best_job = job
        
        if best_job:
            drive_time = calculate_drive_time(best_distance)
            scheduled_today.append(best_job)
            work_hours += best_job.duration
            drive_hours += drive_time
            current_location = (best_job.latitude, best_job.longitude)
            already_scheduled.add(best_job.work_order)
            region_jobs.remove(best_job)
            print(f"      Added: {best_job.site_name} - {best_job.duration}h, {best_distance:.1f} mi")
        else:
            # No job fits in current region, find nearest job in ANY region
            result = find_nearest_job_any_region(sb, tech_id, current_location, 
                                                already_scheduled, max_distance=100,
                                                schedule_date=schedule_date)
            
            if result:
                next_job, new_region = result
                distance = haversine(current_location[0], current_location[1],
                                   next_job.latitude, next_job.longitude)
                drive_time = calculate_drive_time(distance)
                
                if drive_time + next_job.duration <= remaining_hours:
                    print(f"      Region {current_region} exhausted, switching to {new_region}")
                    scheduled_today.append(next_job)
                    work_hours += next_job.duration
                    drive_hours += drive_time
                    current_location = (next_job.latitude, next_job.longitude)
                    already_scheduled.add(next_job.work_order)
                    current_region = new_region
                    
                    # Load new region's jobs
                    region_jobs = find_jobs_in_region(sb, tech_id, new_region, already_scheduled, month_start, month_end)
                    if region_jobs:
                        site_ids = [j.site_id for j in region_jobs]
                        freshness = get_site_freshness(sb, site_ids, schedule_date)
                        region_jobs = filter_jobs_by_freshness(region_jobs, freshness, min_days=18)
                    print(f"      Added: {next_job.site_name} - {next_job.duration}h, {distance:.1f} mi")
                else:
                    break
            else:
                print(f"      No more jobs within 100 miles")
                break
    
    return scheduled_today, work_hours, drive_hours, current_region, current_location

# ============================================================================
# MAIN SCHEDULER
# ============================================================================

def schedule_week_fillin(
    tech_id: int,
    week_start: date,
    sow_filter: Optional[str] = None,
    target_weekly_hours: float = 40
) -> Dict:
    """Main fill-in scheduler with smart routing"""
    
    sb = supabase_client()
    
    # Get tech details
    tech_result = sb.table('technicians').select('*').eq('technician_id', tech_id).execute()
    if not tech_result.data:
        return {"error": f"Technician {tech_id} not found"}
    
    tech = tech_result.data[0]
    tech_home = get_tech_home_location(tech)
    max_daily_hours = tech.get('max_daily_hours', 12)
    
    week_end = week_start + timedelta(days=4)
    
    # Calculate month boundaries for date filtering
    month_start = date(week_start.year, week_start.month, 1)
    if week_start.month == 12:
        month_end = date(week_start.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(week_start.year, week_start.month + 1, 1) - timedelta(days=1)
    
    print(f"\n{'='*80}")
    print(f"SMART FILL-IN SCHEDULER - Tech {tech_id} ({tech['name']})")
    print(f"Week: {week_start} to {week_end}")
    print(f"{'='*80}\n")
    
    # Get existing schedule
    print("ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã¢â‚¬Â¹ Reading existing schedule...")
    existing_schedule = get_existing_schedule(tech_id, week_start, week_end)
    
    # Analyze capacity
    print("\nÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã…Â  Analyzing daily capacity...")
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    day_capacities = {}
    
    for day_num, day_name in enumerate(weekdays):
        day_date = week_start + timedelta(days=day_num)
        
        capacity = analyze_day_capacity(
            day_date=day_date,
            day_name=day_name,
            existing_jobs=existing_schedule[day_name],
            tech_home=tech_home,
            max_daily_hours=max_daily_hours
        )
        
        day_capacities[day_name] = capacity
        
        print(f"  {day_name} ({day_date}):")
        print(f"    Existing: {len(capacity.existing_jobs)} jobs, {capacity.hours_scheduled:.1f}h scheduled")
        print(f"    Available: {capacity.hours_available:.1f}h")
        if capacity.primary_region:
            print(f"    Region: {capacity.primary_region}")
    
    # Analyze future scheduled jobs
    print("\nÃƒÂ°Ã…Â¸Ã¢â‚¬â€Ã‚ÂºÃƒÂ¯Ã‚Â¸Ã‚Â  Analyzing routing strategy...")
    future_jobs = analyze_future_jobs(existing_schedule, tech_home, weekdays)
    
    for fj in future_jobs:
        print(f"  {fj.day_name}: {fj.region} ({fj.drive_hours_from_home:.1f}h from home)")
    
    # Schedule each day
    print("\nÃƒÂ°Ã…Â¸Ã¢â‚¬ÂÃ‚Â§ Filling schedule gaps...")
    new_schedule = {}
    already_scheduled = set()
    current_location = tech_home
    current_region = None
    hotel_stays = {}
    
    for day_num, day_name in enumerate(weekdays):
        day_date = week_start + timedelta(days=day_num)
        capacity = day_capacities[day_name]
        
        print(f"\n  {day_name} ({day_date}):")
        
        # Track corridor jobs scheduled for this day
        corridor_jobs_scheduled = []
        corridor_work_hours = 0
        corridor_drive_hours = 0
        
        if capacity.hours_available <= 1.0:
            print(f"    â¸ Day is full - no capacity to add jobs")
            # Even for full days, need to check hotel stay and update location for next day
            if capacity.existing_jobs and capacity.last_job_location:
                distance_to_home = haversine(
                    capacity.last_job_location[0], capacity.last_job_location[1],
                    tech_home[0], tech_home[1]
                )
                
                if day_name != 'Friday' and distance_to_home > 90:
                    hotel_stays[day_name] = True
                    current_location = capacity.last_job_location
                    current_region = capacity.primary_region
                    print(f"    Hotel stay required ({distance_to_home:.1f} miles from home)")
                else:
                    current_location = tech_home
                    hotel_stays[day_name] = False
            
            new_schedule[day_name] = {
                "date": str(day_date),
                "existing_jobs": len(capacity.existing_jobs),
                "new_jobs": [],
                "work_hours": 0,
                "drive_hours": 0,
                "total_hours": 0,
                "hotel_stay": hotel_stays.get(day_name, False)
            }
            continue
        
        # If this day has existing jobs, use them
        if capacity.existing_jobs:
            current_location = capacity.last_job_location
            current_region = capacity.primary_region
            print(f"    ðŸ“‹ Starting from existing job location in {current_region}")
        else:
            # Check if we should go early to a future job
            next_scheduled = None
            for future_day_num in range(day_num + 1, 5):
                future_capacity = day_capacities[weekdays[future_day_num]]
                if future_capacity.existing_jobs:
                    next_scheduled = future_capacity
                    days_until = future_day_num - day_num
                    break
            
            if next_scheduled:
                distance_to_future = haversine(
                    current_location[0], current_location[1],
                    next_scheduled.last_job_location[0], next_scheduled.last_job_location[1]
                )
                drive_hours_to_future = calculate_drive_time(distance_to_future)
                
                print(f"    ðŸŽ¯ Next scheduled job: {weekdays[future_day_num]} in {next_scheduled.primary_region}")
                print(f"       Distance: {distance_to_future:.1f} miles ({drive_hours_to_future:.1f}h drive)")
                
                # SMART DECISION: Should we go early?
                if should_go_early_to_region(drive_hours_to_future, days_until,
                             current_location, next_scheduled.last_job_location):
                    print(f"    ðŸš— Going early to {next_scheduled.primary_region} region (long drive)")
                    
                    # Find and filter corridor jobs
                    corridor_jobs = find_jobs_along_corridor(
                        sb, tech_id, current_location, next_scheduled.last_job_location,
                        schedule_date=day_date,
                        corridor_width=30, already_scheduled=already_scheduled
                    )
                    
                    if corridor_jobs:
                        print(f"    ðŸ“‹ Found {len(corridor_jobs)} corridor jobs")
                        
                        # Calculate if destination can be bumped
                        dest_can_bump = True
                        if next_scheduled.existing_jobs:
                            first_dest_job = next_scheduled.existing_jobs[0]
                            if hasattr(first_dest_job, 'due_date') and first_dest_job.due_date:
                                try:
                                    due = datetime.fromisoformat(str(first_dest_job.due_date)).date() if isinstance(first_dest_job.due_date, str) else first_dest_job.due_date
                                    dest_can_bump = (due - day_date).days > 1
                                except:
                                    dest_can_bump = True
                        
                        # Estimate destination work time
                        dest_duration = sum(j.duration for j in next_scheduled.existing_jobs) if next_scheduled.existing_jobs else 4.0
                        
                        # Decide what to do with corridor jobs
                        corridor_result = schedule_corridor_jobs(
                            corridor_jobs=corridor_jobs,
                            start_location=current_location,
                            end_location=next_scheduled.last_job_location,
                            available_hours=capacity.hours_available,
                            travel_time_to_destination=drive_hours_to_future,
                            destination_job_duration=dest_duration,
                            destination_can_be_bumped=dest_can_bump
                        )
                        
                        print(f"    ðŸ“ {corridor_result['reason']}")
                        
                        if corridor_result['jobs_to_schedule']:
                            # *** FIX: Actually collect corridor jobs for output ***
                            corridor_jobs_scheduled = corridor_result['jobs_to_schedule']
                            
                            # Calculate corridor time
                            prev_loc = current_location
                            for job in corridor_jobs_scheduled:
                                drive_dist = haversine(prev_loc[0], prev_loc[1], job.latitude, job.longitude)
                                corridor_drive_hours += calculate_drive_time(drive_dist)
                                corridor_work_hours += job.duration
                                already_scheduled.add(job.work_order)
                                prev_loc = (job.latitude, job.longitude)
                                print(f"      âœ… Corridor job: {job.site_name} ({job.duration}h)")
                            
                            # Update location to end of corridor
                            last_corridor = corridor_jobs_scheduled[-1]
                            current_location = (last_corridor.latitude, last_corridor.longitude)
                            current_region = _job_regions_cache.get(last_corridor.work_order, next_scheduled.primary_region)
                            
                            if corridor_result['bump_destination']:
                                print(f"    âš ï¸ NOTE: Destination should be bumped to next day (not yet implemented)")
                    else:
                        print(f"    No corridor jobs, going directly to {next_scheduled.primary_region}")
                        # GO TO THE DESTINATION - don't stay at home!
                        current_location = next_scheduled.last_job_location
                        current_region = next_scheduled.primary_region
        
        # Fill the day using greedy geographic
        if not current_region:
            # Find nearest job from current location to determine region
            result = find_nearest_job_any_region(sb, tech_id, current_location, 
                                                already_scheduled, max_distance=300,
                                                schedule_date=week_start)
            if result:
                _, current_region = result
                print(f"     Starting in region {current_region}")
            else:
                print(f"     No available jobs found")
                # Still need to output corridor jobs if we found any!
                if corridor_jobs_scheduled:
                    new_schedule[day_name] = {
                        "date": str(day_date),
                        "existing_jobs": len(capacity.existing_jobs),
                        "new_jobs": [
                            {
                                "work_order": j.work_order,
                                "site_name": j.site_name,
                                "sow": j.sow_1,
                                "duration": j.duration,
                                "priority": j.jp_priority,
                                "is_recurring": j.is_recurring_site if hasattr(j, 'is_recurring_site') else False
                            }
                            for j in corridor_jobs_scheduled
                        ],
                        "work_hours": corridor_work_hours,
                        "drive_hours": corridor_drive_hours,
                        "total_hours": corridor_work_hours + corridor_drive_hours,
                        "hotel_stay": False
                    }
                continue
        
        # Reduce available capacity by corridor jobs already scheduled
        adjusted_capacity = DayCapacity(
            date=capacity.date,
            day_name=capacity.day_name,
            existing_jobs=capacity.existing_jobs,
            hours_scheduled=capacity.hours_scheduled + corridor_work_hours + corridor_drive_hours,
            hours_available=capacity.hours_available - corridor_work_hours - corridor_drive_hours,
            time_gaps=capacity.time_gaps,
            primary_region=capacity.primary_region,
            last_job_location=current_location
        )
        
        # Fill using greedy geographic approach
        new_jobs, work_hours, drive_hours, end_region, end_location = fill_day_greedy_geographic(
            sb, tech_id, adjusted_capacity, current_location, current_region, 
            already_scheduled, max_work_hours=10.5,
            month_start=month_start, month_end=month_end, schedule_date=week_start
        )
        
        # *** FIX: Combine corridor jobs + greedy jobs ***
        all_new_jobs = corridor_jobs_scheduled + new_jobs
        total_work_hours = corridor_work_hours + work_hours
        total_drive_hours = corridor_drive_hours + drive_hours
        
        print(f"     Added {len(all_new_jobs)} jobs ({total_work_hours:.1f}h work + {total_drive_hours:.1f}h drive)")
        if corridor_jobs_scheduled:
            print(f"       (includes {len(corridor_jobs_scheduled)} corridor jobs)")
        
        # Check hotel stay
        distance_to_home = haversine(end_location[0], end_location[1],
                                    tech_home[0], tech_home[1])
        
        if day_name != 'Friday' and distance_to_home > 90:
            hotel_stays[day_name] = True
            current_location = end_location
            current_region = end_region
            print(f"     Hotel stay required ({distance_to_home:.1f} miles from home)")
        else:
            current_location = tech_home
            current_region = None  # Reset - will find nearest region to home
            hotel_stays[day_name] = False
        
        new_schedule[day_name] = {
            "date": str(day_date),
            "existing_jobs": len(capacity.existing_jobs),
            "new_jobs": [
                {
                    "work_order": j.work_order,
                    "site_name": j.site_name,
                    "sow": j.sow_1,
                    "duration": j.duration,
                    "priority": j.jp_priority,
                    "is_recurring": j.is_recurring_site if hasattr(j, 'is_recurring_site') else False
                }
                for j in all_new_jobs  # *** FIX: Use combined list ***
            ],
            "work_hours": total_work_hours,
            "drive_hours": total_drive_hours,
            "total_hours": total_work_hours + total_drive_hours,
            "hotel_stay": hotel_stays.get(day_name, False)
        }
    
    print(f"\n{'='*80}")
    print("FILL-IN COMPLETE")
    print(f"{'='*80}")
    
    return {
        "success": True,
        "tech_id": tech_id,
        "tech_name": tech['name'],
        "week_start": str(week_start),
        "schedule": new_schedule
    }

