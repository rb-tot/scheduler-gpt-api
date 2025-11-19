"""
SCHEDULER V5 - GEOGRAPHIC-FIRST APPROACH
Smart regional scheduling with route optimization
"""
import os
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from math import radians, cos, sin, asin, sqrt

from supabase_client import supabase_client

# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class Job:
    work_order: int
    site_id: int
    site_name: str
    site_city: str
    latitude: float
    longitude: float
    sow_1: str
    due_date: date
    jp_priority: str
    duration: float
    is_recurring_site: bool
    is_night: bool
    night_test: bool
    days_til_due: int
    priority_rank: int
    distance_from_tech_home: float
    start_time: Optional[str] = None

@dataclass
class DaySchedule:
    date: date
    day_name: str
    jobs: List[Job]
    work_hours: float
    drive_hours: float
    total_hours: float
    starts_from_home: bool
    ends_at_home: bool
    hotel_location: Optional[str] = None
    last_location: Optional[Tuple[float, float]] = None

# ============================================================================
# DISTANCE CALCULATIONS
# ============================================================================

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two points in miles using Haversine formula
    """
    R = 3958.8  # Earth radius in miles
    
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    return R * c

def calculate_drive_time(distance_miles: float, avg_speed: float = 55) -> float:
    """
    Calculate drive time in hours
    Default: 55 mph average speed
    """
    return distance_miles / avg_speed
def calculate_start_times(daily_jobs: List[Job], start_location: Tuple[float, float]) -> None:
    """
    Calculate start times for each job in the day.
    
    Rules:
    - Night jobs (NT in SOW): Must be on-site by 9:00 PM
    - Regular jobs: Start at 7:00 AM, then sequence by travel + work time
    
    Modifies jobs in-place to add start_time field
    """
    from datetime import datetime, timedelta
    
    if not daily_jobs:
        return
    
    # Check if first job is a night job
    first_job = daily_jobs[0]
    is_night_job = 'NT' in (first_job.sow_1 or '')
    
    if is_night_job:
        # Night job - work backwards from 9 PM on-site time
        onsite_time = datetime.strptime('21:00', '%H:%M')  # 9 PM
        
        # Calculate drive time to first job
        distance_to_first = haversine(
            start_location[0], start_location[1],
            first_job.latitude, first_job.longitude
        )
        drive_time_hours = distance_to_first / 55  # 55 mph average
        drive_time_minutes = int(drive_time_hours * 60)
        
        # Start time = on-site time - drive time
        start_time = onsite_time - timedelta(minutes=drive_time_minutes)
        first_job.start_time = start_time.strftime('%H:%M')
        
        current_time = onsite_time
        
    else:
        # Regular job - start at 7 AM
        current_time = datetime.strptime('07:00', '%H:%M')
        
        # Calculate drive to first job
        distance_to_first = haversine(
            start_location[0], start_location[1],
            first_job.latitude, first_job.longitude
        )
        drive_time_hours = distance_to_first / 55
        drive_time_minutes = int(drive_time_hours * 60)
        
        first_job.start_time = current_time.strftime('%H:%M')
        
        # Arrival time = start + drive
        current_time = current_time + timedelta(minutes=drive_time_minutes)
    
    # Calculate subsequent job start times
    for i in range(len(daily_jobs) - 1):
        current_job = daily_jobs[i]
        next_job = daily_jobs[i + 1]
        
        # Add work time for current job
        work_minutes = int(current_job.duration * 60)
        current_time = current_time + timedelta(minutes=work_minutes)
        
        # Add drive time to next job
        distance = haversine(
            current_job.latitude, current_job.longitude,
            next_job.latitude, next_job.longitude
        )
        drive_minutes = int((distance / 55) * 60)
        current_time = current_time + timedelta(minutes=drive_minutes)
        
        # Check if next job is night job
        if 'NT' in (next_job.sow_1 or ''):
            # Override - must start so we're on-site by 9 PM
            onsite_time = datetime.strptime('21:00', '%H:%M')
            distance_to_next = haversine(
                current_job.latitude, current_job.longitude,
                next_job.latitude, next_job.longitude
            )
            drive_to_next = int((distance_to_next / 55) * 60)
            next_job.start_time = (onsite_time - timedelta(minutes=drive_to_next)).strftime('%H:%M')
            current_time = onsite_time
        else:
            next_job.start_time = current_time.strftime('%H:%M')


# ============================================================================
# REGION ANALYSIS
# ============================================================================

def analyze_regions_for_tech(
    tech_id: int,
    month_start: date,
    month_end: date,
    sow_filter: Optional[str] = None
) -> List[Dict]:
    """
    Get region analysis from database
    Returns list of regions with job counts, distances, efficiency scores
    """
    sb = supabase_client()
    
    result = sb.rpc(
        'analyze_regions_for_tech',
        {
            'p_tech_id': tech_id,
            'p_month_start': str(month_start),
            'p_month_end': str(month_end),
            'p_sow_filter': sow_filter
        }
    ).execute()
    
    return result.data if result.data else []

def get_all_jobs_in_region(
    tech_id: int,
    region_name: str,
    month_start: date,
    month_end: date,
    sow_filter: Optional[str] = None
) -> List[Job]:
    """
    Get ALL jobs in a region for a tech
    Returns jobs sorted by priority
    """
    sb = supabase_client()
    
    result = sb.rpc(
        'get_all_jobs_in_region',
        {
            'p_tech_id': tech_id,
            'p_region_name': region_name,
            'p_month_start': str(month_start),
            'p_month_end': str(month_end),
            'p_sow_filter': sow_filter
        }
    ).execute()
    
    if not result.data:
        return []
    
    # Convert to Job objects
    jobs = []
    for row in result.data:
        jobs.append(Job(
            work_order=row['work_order'],
            site_id=row['site_id'],
            site_name=row['site_name'],
            site_city=row['site_city'],
            latitude=row['latitude'],
            longitude=row['longitude'],
            sow_1=row['sow_1'],
            due_date=datetime.fromisoformat(row['due_date']).date(),
            jp_priority=row['jp_priority'],
            duration=float(row['duration']),
            is_recurring_site=row['is_recurring_site'],
            is_night=row['is_night'] or False,
            night_test=row['night_test'] or False,
            days_til_due=row['days_til_due'] or 999,
            priority_rank=row['priority_rank'],
            distance_from_tech_home=float(row['distance_from_tech_home'])
        ))
    
    return jobs

# ============================================================================
# ROUTE OPTIMIZATION (Nearest-Neighbor Algorithm)
# ============================================================================

def build_daily_route(
    jobs: List[Job],
    start_location: Tuple[float, float],
    max_daily_hours: float = 12,
    max_work_hours: float = 10.5
) -> Tuple[List[Job], float, float, Tuple[float, float]]:
    """
    Build one day's route using nearest-neighbor algorithm
    
    Returns:
        - List of jobs scheduled today
        - Total work hours
        - Total drive hours  
        - Last location (lat, lon)
    """
    scheduled_today = []
    remaining_jobs = jobs.copy()
    current_location = start_location
    
    work_hours = 0
    drive_hours = 0
    
    while remaining_jobs and work_hours < max_work_hours:
        # Find nearest unscheduled job
        nearest_job = min(
            remaining_jobs,
            key=lambda j: haversine(
                current_location[0], current_location[1],
                j.latitude, j.longitude
            )
        )
        
        # Calculate drive time to this job
        distance = haversine(
            current_location[0], current_location[1],
            nearest_job.latitude, nearest_job.longitude
        )
        drive_time = calculate_drive_time(distance)
        
        # Check if job fits today (work + drive must be < max_daily_hours)
        if work_hours + drive_hours + drive_time + nearest_job.duration <= max_daily_hours:
            # Schedule this job
            scheduled_today.append(nearest_job)
            work_hours += nearest_job.duration
            drive_hours += drive_time
            current_location = (nearest_job.latitude, nearest_job.longitude)
            remaining_jobs.remove(nearest_job)
        else:
            # Can't fit any more jobs today
            break
    
    return scheduled_today, work_hours, drive_hours, current_location

# ============================================================================
# WEEK SCHEDULING - GEOGRAPHIC-FIRST APPROACH
# ============================================================================

def schedule_week_geographic(
    tech_id: int,
    region_names: List[str],
    week_start: date,
    sow_filter: Optional[str] = None,
    target_weekly_hours: float = 40
) -> Dict:
    """
    Main scheduler: Geographic-first approach
    
    Strategy:
    1. Analyze all specified regions
    2. Pick best region to focus on this week
    3. Schedule ALL jobs in that region (including all Monthly O&M)
    4. If capacity remains, add jobs from nearby regions
    5. Optimize route using nearest-neighbor
    
    Returns full week schedule with drive times and hotel stays
    """
    
    # Get tech details
    sb = supabase_client()
    tech_result = sb.table('technicians').select('*').eq('technician_id', tech_id).execute()
    
    if not tech_result.data:
        return {"error": f"Technician {tech_id} not found"}
    
    tech = tech_result.data[0]
    tech_home = (tech['home_latitude'], tech['home_longitude'])
    max_daily_hours = tech.get('max_daily_hours', 14)
    max_weekly_hours = tech.get('max_weekly_hours', 50)
    
    # Calculate month boundaries for region analysis
    month_start = date(week_start.year, week_start.month, 1)
    if week_start.month == 12:
        month_end = date(week_start.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(week_start.year, week_start.month + 1, 1) - timedelta(days=1)
    
    # STEP 1: Analyze regions
    print(f"\nðŸ“Š Analyzing regions for Tech {tech_id}...")
    all_regions = analyze_regions_for_tech(tech_id, month_start, month_end, sow_filter)
    
    # Filter to user-selected regions if specified
    if region_names:
        all_regions = [r for r in all_regions if r['region_name'] in region_names]
    
    if not all_regions:
        return {
            "error": "No jobs found in specified regions",
            "regions_checked": region_names
        }
    
    # STEP 2: Score regions and pick best one
    print(f"\nðŸŽ¯ Scoring regions...")
    for region in all_regions:
        score = 0
        
        # Critical: Priority jobs boost score significantly
        score += region['priority_job_count'] * 100
        
        # Recurring jobs must be done
        score += region['recurring_count'] * 50
        
        # Remote regions: Only go if we can fill 2+ days
        if region['requires_hotel']:
            if region['total_work_hours'] >= 16:  # 2 full days of work
                score += 50
            else:
                score -= 200  # Don't go for just a few hours
        
        # Efficiency: jobs per mile
        if region['distance_from_home'] > 0:
            score += (region['job_count'] / region['distance_from_home']) * 10
        
        # Urgency: favor regions with jobs due soon
        if region['avg_days_til_due'] and region['avg_days_til_due'] < 14:
            score += 30
        
        region['score'] = round(score, 2)
        print(f"  {region['region_name']}: {region['job_count']} jobs, "
              f"distance {region['distance_from_home']:.0f}mi, score {region['score']}")
    
    # Pick highest scoring region
    primary_region = max(all_regions, key=lambda r: r['score'])
    print(f"\nâœ… Selected region: {primary_region['region_name']} "
          f"(score: {primary_region['score']})")
    
    # STEP 3: Get ALL jobs in primary region
    print(f"\nðŸ“‹ Loading ALL jobs in {primary_region['region_name']}...")
    jobs = get_all_jobs_in_region(
        tech_id=tech_id,
        region_name=primary_region['region_name'],
        month_start=month_start,
        month_end=month_end,
        sow_filter=sow_filter
    )
    
    print(f"  Found {len(jobs)} jobs:")
    print(f"    - Priority: {sum(1 for j in jobs if j.priority_rank <= 2)}")
    print(f"    - Monthly O&M: {sum(1 for j in jobs if j.jp_priority == 'Monthly O&M')}")
    print(f"    - Other: {sum(1 for j in jobs if j.priority_rank > 3)}")
    
    # STEP 4: Build week schedule using route optimization
    print(f"\nðŸ—ºï¸  Building optimized routes...")
    week_schedule = {}
    current_location = tech_home
    remaining_jobs = jobs.copy()
    total_week_hours = 0
    
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    
    for day_num in range(5):
        day_date = week_start + timedelta(days=day_num)
        day_name = weekdays[day_num]
        
        print(f"\n  {day_name} ({day_date}):")
         
        # Update for time off 11/14/2025 # Check for time off on this day
        time_off_result = sb.table('time_off_requests')\
            .select('*')\
            .eq('technician_id', tech_id)\
            .eq('approved', True)\
            .lte('start_date', day_date.isoformat())\
            .gte('end_date', day_date.isoformat())\
            .execute()
        
        # Adjust available hours based on time off
        adjusted_daily_hours = max_daily_hours
        time_off_reason = None
        
        if time_off_result.data:
            time_off = time_off_result.data[0]
            hours_off = float(time_off.get('hours_per_day', 4))  # Default to 4 hours (half day)
            adjusted_daily_hours = hours_off
            time_off_reason = time_off.get('reason', 'Time off')
            
            if adjusted_daily_hours <= 0:
                # Full day off - skip this day
                print(f"    ⛔ FULL DAY OFF: {time_off_reason}")
                week_schedule[day_name.lower()] = DaySchedule(
                    date=day_date,
                    day_name=day_name,
                    jobs=[],
                    work_hours=0,
                    drive_hours=0,
                    total_hours=0,
                    starts_from_home=True,
                    ends_at_home=True,
                    hotel_location=None,
                    last_location=tech_home
                )
                continue  # Skip to next day
            else:
                # Partial day off
                print(f"    ⏰ PARTIAL TIME OFF: {hours_off}h off ({time_off_reason}), {adjusted_daily_hours}h available")
        
        # Build route for this day (using adjusted hours)
        daily_jobs, work_hours, drive_hours, end_location = build_daily_route(
            jobs=remaining_jobs,
            start_location=current_location,
            max_daily_hours=adjusted_daily_hours  # Use adjusted hours instead of max_daily_hours
        )    
        # Build route for this day
        daily_jobs, work_hours, drive_hours, end_location = build_daily_route(
            jobs=remaining_jobs,
            start_location=current_location,
            max_daily_hours=adjusted_daily_hours
        )
        calculate_start_times(daily_jobs, current_location)
        # BEFORE scheduling jobs for the day, add this:
        if day_name == 'Monday' or current_location == tech_home:
            # Starting from home - need to add drive to first job
            if daily_jobs:
                first_job = daily_jobs[0]
                distance_to_first = haversine(
                    tech_home[0], tech_home[1],
                    first_job.latitude, first_job.longitude
                )
                initial_drive_time = calculate_drive_time(distance_to_first)
                drive_hours += initial_drive_time
                print(f"    Adding {initial_drive_time:.1f}h drive from home to first job")
        # Remove scheduled jobs from remaining
        for job in daily_jobs:
            if job in remaining_jobs:
                remaining_jobs.remove(job)
        
        # Calculate distance back to home
        distance_to_home = haversine(
            end_location[0], end_location[1],
            tech_home[0], tech_home[1]
        )
        drive_time_home = calculate_drive_time(distance_to_home)
        
        # Determine hotel stay
        if day_name == 'Friday':
            # Always go home on Friday - no weekend hotels
            hotel_stay = False
            ends_at_home = True
        else:
            hotel_stay = distance_to_home > 90
            ends_at_home = not hotel_stay
        
        # If staying at hotel, don't add drive time home
        # If going home, add drive time
        total_drive_hours = drive_hours if hotel_stay else drive_hours + drive_time_home
        total_hours = work_hours + total_drive_hours
        
        week_schedule[day_name.lower()] = DaySchedule(
            date=day_date,
            day_name=day_name,
            jobs=daily_jobs,
            work_hours=round(work_hours, 2),
            drive_hours=round(total_drive_hours, 2),
            total_hours=round(total_hours, 2),
            starts_from_home=(current_location == tech_home),
            ends_at_home=ends_at_home,
            hotel_location=f"{daily_jobs[-1].site_city if daily_jobs else 'Unknown'}" if hotel_stay else None,
            last_location=end_location if hotel_stay else tech_home
        )
        
        print(f"    Jobs: {len(daily_jobs)}, Work: {work_hours:.1f}h, "
              f"Drive: {total_drive_hours:.1f}h, Total: {total_hours:.1f}h")
        if hotel_stay:
            print(f"    ðŸ¨ Hotel stay in {week_schedule[day_name.lower()].hotel_location}")
        
        # Next day starts from hotel or home
        current_location = end_location if hotel_stay else tech_home
        total_week_hours += total_hours
    
    # STEP 5: Check if we met target hours
    print(f"\nðŸ“Š Week total: {total_week_hours:.1f} hours (target: {target_weekly_hours})")
    
    warnings = []
    suggestions = []
    
    if total_week_hours < target_weekly_hours:
        hours_short = target_weekly_hours - total_week_hours
        warnings.append(f"Scheduled {total_week_hours:.1f} hours (target: {target_weekly_hours})")
        
        # Get adjacent regions
        adjacent = sb.rpc(
            'get_adjacent_regions',
            {'p_region_name': primary_region['region_name']}
        ).execute()
        
        if adjacent.data:
            nearby_regions = [r['adjacent_region'] for r in adjacent.data[:3]]
            suggestions.append(f"Consider adding nearby regions: {', '.join(nearby_regions)}")
    
    if remaining_jobs:
        warnings.append(f"{len(remaining_jobs)} jobs not scheduled this week")
        suggestions.append(f"These jobs can be scheduled in subsequent weeks")
    
    # Format response
    return {
        "success": True,
        "tech_id": tech_id,
        "tech_name": tech['name'],
        "region_focus": primary_region['region_name'],
        "week_start": str(week_start),
        "total_hours": round(total_week_hours, 2),
        "jobs_scheduled": len(jobs) - len(remaining_jobs),
        "jobs_remaining": len(remaining_jobs),
        "schedule": {
            day: {
                "date": str(sched.date),
                "day_name": sched.day_name,
                "jobs": [
                    {
                        "work_order": j.work_order,
                        "site_name": j.site_name,
                        "sow": j.sow_1,
                        "duration": j.duration,
                        "priority": j.jp_priority,
                        "start_time": j.start_time
                    } for j in sched.jobs
                ],
                "work_hours": sched.work_hours,
                "drive_hours": sched.drive_hours,
                "total_hours": sched.total_hours,
                "starts_from_home": sched.starts_from_home,
                "ends_at_home": sched.ends_at_home,
                "hotel_stay": sched.hotel_location is not None,
                "hotel_location": sched.hotel_location
            }
            for day, sched in week_schedule.items()
        },
        "warnings": warnings,
        "suggestions": suggestions,
        "region_analysis": all_regions
    }

# ============================================================================
# MAIN ENTRY POINT (for testing)
# ============================================================================

if __name__ == "__main__":
    # Test the scheduler
    result = schedule_week_geographic(
        tech_id=7,
        region_names=['CO_Denver_Metro', 'CO_NoCo'],
        week_start=date(2025, 9, 8),
        sow_filter=None,  # or 'NT' to filter by SOW
        target_weekly_hours=40
    )
    
    import json
    print("\n" + "="*80)
    print("SCHEDULE RESULT:")
    print("="*80)
    print(json.dumps(result, indent=2))
