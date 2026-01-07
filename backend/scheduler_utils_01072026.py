"""
SCHEDULER UTILITIES
Shared functions for all scheduler versions
"""
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt
from typing import List, Tuple, Optional
from dataclasses import dataclass

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
    due_date: str  # Can be date or string
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
class ScheduledJob:
    """Represents a job already in scheduled_jobs table"""
    work_order: int
    site_name: str
    site_city: str
    technician_id: int
    date: str
    due_date: str
    duration: float
    sow_1: str
    latitude: float
    longitude: float
    start_time: Optional[str] = None
    end_time: Optional[str] = None

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

# ============================================================================
# TIME CALCULATIONS
# ============================================================================

def calculate_start_times(daily_jobs: List[Job], start_location: Tuple[float, float]) -> None:
    """
    Calculate start times for each job in the day.
    
    Rules:
    - Night jobs (NT in SOW): Must be on-site by 9:00 PM
    - Regular jobs: Start at 7:00 AM, then sequence by travel + work time
    
    Modifies jobs in-place to add start_time field
    """
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

def estimate_job_end_time(start_time_str: str, duration_hours: float, 
                          travel_time_to_next: float = 0) -> str:
    """
    Estimate when a job ends (including travel to next job)
    Returns time in HH:MM format
    """
    start = datetime.strptime(start_time_str, '%H:%M')
    work_minutes = int(duration_hours * 60)
    travel_minutes = int(travel_time_to_next * 60)
    
    end = start + timedelta(minutes=work_minutes + travel_minutes)
    return end.strftime('%H:%M')

def calculate_time_gap(end_time_str: str, next_start_time_str: str) -> float:
    """
    Calculate hours between end of one job and start of next
    Returns hours as float
    """
    end = datetime.strptime(end_time_str, '%H:%M')
    next_start = datetime.strptime(next_start_time_str, '%H:%M')
    
    # Handle next day wraparound
    if next_start < end:
        next_start += timedelta(days=1)
    
    gap = (next_start - end).total_seconds() / 3600
    return gap

def parse_time(time_value) -> Optional[str]:
    """
    Parse various time formats to HH:MM string
    Handles: datetime, time, string formats
    """
    if time_value is None:
        return None
    
    if isinstance(time_value, str):
        # Already a string, try to parse it
        try:
            dt = datetime.fromisoformat(time_value.replace('Z', '+00:00'))
            return dt.strftime('%H:%M')
        except:
            # Might already be HH:MM format
            if ':' in time_value:
                return time_value[:5]  # Take just HH:MM
    
    if hasattr(time_value, 'hour'):  # datetime or time object
        return time_value.strftime('%H:%M')
    
    return None

# ============================================================================
# TECH UTILITIES
# ============================================================================

def get_tech_home_location(tech_data: dict) -> Tuple[float, float]:
    """Extract tech home coordinates from tech data"""
    return (tech_data['home_latitude'], tech_data['home_longitude'])

def check_time_off(supabase_client, tech_id: int, check_date: str) -> Tuple[bool, float, str]:
    """
    Check if tech has time off on a specific date
    
    Returns:
        - is_time_off: bool
        - hours_available: float (0 if full day off, reduced hours if partial)
        - reason: str
    """
    result = supabase_client.table('time_off_requests')\
        .select('*')\
        .eq('technician_id', tech_id)\
        .eq('approved', True)\
        .lte('start_date', check_date)\
        .gte('end_date', check_date)\
        .execute()
    
    if not result.data:
        return False, 12.0, ""  # No time off, full day available (12h default)
    
    time_off = result.data[0]
    hours_off = float(time_off.get('hours_per_day', 4))
    reason = time_off.get('reason', 'Time off')
    
    # Calculate available hours (assume 12h max day - hours off)
    hours_available = max(0, 12 - hours_off)
    
    return True, hours_available, reason
