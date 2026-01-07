"""
TEST HARNESS: Compare Greedy Routing vs AI Optimizer

This script lets you test the AI Route Optimizer against your current 
greedy nearest-neighbor approach using REAL data from your database.

HOW TO USE:
1. Run this script: python test_route_optimizer.py
2. It will fetch jobs from database for a specific tech/region/week
3. It runs BOTH algorithms on the same jobs
4. Shows you a side-by-side comparison

WHAT TO LOOK FOR:
- Does AI optimizer schedule more jobs?
- Does it reduce drive time?
- Does it maintain business rules? (This version won't - that's what we'll fix next)
"""

import sys
import os
from datetime import date, timedelta
from typing import List, Tuple
import json

# Import your existing scheduler
from scheduler_v5_geographic import (
    Job,
    get_all_jobs_in_region,
    build_daily_route,
    haversine,
    calculate_drive_time
)

# Import the AI optimizer
from AI_Route_Optimizer import SmartRouter

# ============================================================================
# COMPARISON FUNCTIONS
# ============================================================================

def convert_jobs_for_ai_optimizer(jobs: List[Job]) -> List[dict]:
    """
    Convert your Job objects to the format the AI optimizer expects
    """
    job_dicts = []
    for j in jobs:
        job_dicts.append({
            'work_order': j.work_order,
            'latitude': j.latitude,
            'longitude': j.longitude,
            'duration': j.duration,
            'jp_priority': j.jp_priority,
            'site_name': j.site_name,
            'sow_1': j.sow_1,
            'days_til_due': j.days_til_due
        })
    return job_dicts

def analyze_route(jobs: List[Job], start_location: Tuple[float, float]) -> dict:
    """
    Analyze a route to show detailed metrics
    """
    if not jobs:
        return {
            'job_count': 0,
            'work_hours': 0,
            'drive_hours': 0,
            'total_hours': 0,
            'total_miles': 0,
            'avg_job_distance': 0,
            'backtracking_score': 0
        }
    
    total_miles = 0
    total_work = 0
    current_lat, current_lon = start_location
    
    # Track if we backtrack (go farther from home then closer, then farther again)
    distances_from_start = []
    
    for job in jobs:
        # Calculate drive to this job
        dist = haversine(current_lat, current_lon, job.latitude, job.longitude)
        total_miles += dist
        total_work += job.duration
        
        # Track distance from start
        dist_from_start = haversine(start_location[0], start_location[1], 
                                     job.latitude, job.longitude)
        distances_from_start.append(dist_from_start)
        
        current_lat, current_lon = job.latitude, job.longitude
    
    # Calculate backtracking score (how many times we go farther, then closer, then farther)
    backtrack_count = 0
    for i in range(len(distances_from_start) - 2):
        if distances_from_start[i] < distances_from_start[i+1] > distances_from_start[i+2]:
            backtrack_count += 1
    
    drive_hours = total_miles / 55  # 55 mph average
    
    return {
        'job_count': len(jobs),
        'work_hours': round(total_work, 2),
        'drive_hours': round(drive_hours, 2),
        'total_hours': round(total_work + drive_hours, 2),
        'total_miles': round(total_miles, 1),
        'avg_job_distance': round(total_miles / len(jobs), 1) if jobs else 0,
        'backtracking_score': backtrack_count
    }

def compare_routes(
    tech_id: int,
    region_name: str,
    test_date: date,
    start_location: Tuple[float, float],
    max_daily_hours: float = 12.0
):
    """
    Main comparison function - runs both algorithms and shows results
    """
    
    print("="*80)
    print(f"🧪 ROUTE OPTIMIZER TEST")
    print("="*80)
    print(f"Tech ID: {tech_id}")
    print(f"Region: {region_name}")
    print(f"Test Date: {test_date}")
    print(f"Max Daily Hours: {max_daily_hours}")
    print(f"Start Location: {start_location}")
    print()
    
    # STEP 1: Fetch jobs from database
    print("📥 Fetching jobs from database...")
    month_start = date(test_date.year, test_date.month, 1)
    if test_date.month == 12:
        month_end = date(test_date.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(test_date.year, test_date.month + 1, 1) - timedelta(days=1)
    
    jobs = get_all_jobs_in_region(
        tech_id=tech_id,
        region_name=region_name,
        month_start=month_start,
        month_end=month_end,
        sow_filter=None
    )
    
    if not jobs:
        print("❌ No jobs found! Check your tech_id and region_name.")
        return
    
    print(f"✅ Found {len(jobs)} total jobs in region")
    print(f"   - Urgent: {sum(1 for j in jobs if j.jp_priority == 'Urgent')}")
    print(f"   - Monthly O&M: {sum(1 for j in jobs if j.jp_priority == 'Monthly O&M')}")
    print(f"   - Priority Rank 1-2: {sum(1 for j in jobs if j.priority_rank <= 2)}")
    print()
    
    # STEP 2: Run GREEDY algorithm (your current approach)
    print("🔄 Running GREEDY algorithm (nearest-neighbor)...")
    greedy_jobs, greedy_work, greedy_drive, greedy_end = build_daily_route(
        jobs=jobs.copy(),
        start_location=start_location,
        max_daily_hours=max_daily_hours
    )
    greedy_metrics = analyze_route(greedy_jobs, start_location)
    print(f"✅ Greedy scheduled {len(greedy_jobs)} jobs")
    print()
    
    # STEP 3: Run AI OPTIMIZER (Simulated Annealing)
    print("🤖 Running AI OPTIMIZER (Simulated Annealing)...")
    # Pass the Job objects directly - the AI optimizer handles conversion internally
    router = SmartRouter(avg_speed_mph=55)
    ai_jobs_result, ai_work, ai_drive, ai_end = router.build_route(
        available_jobs=jobs.copy(),  # Pass Job objects, not dicts
        start_location=start_location,
        max_daily_hours=max_daily_hours
    )
    
    # The optimizer might return dicts - convert back to Job objects if needed
    if ai_jobs_result and isinstance(ai_jobs_result[0], dict):
        # Map returned dicts back to original Job objects by work_order
        ai_jobs = []
        for job_dict in ai_jobs_result:
            wo = job_dict.get('work_order')
            matching_job = next((j for j in jobs if j.work_order == wo), None)
            if matching_job:
                ai_jobs.append(matching_job)
    else:
        ai_jobs = ai_jobs_result
    
    ai_metrics = analyze_route(ai_jobs, start_location)
    print(f"✅ AI Optimizer scheduled {len(ai_jobs)} jobs")
    print()
    
    # STEP 4: COMPARISON
    print("="*80)
    print("📊 SIDE-BY-SIDE COMPARISON")
    print("="*80)
    print()
    
    print(f"{'Metric':<30} {'Greedy':<20} {'AI Optimizer':<20} {'Difference':<15}")
    print("-"*85)
    
    metrics_to_compare = [
        ('Jobs Scheduled', 'job_count', ''),
        ('Work Hours', 'work_hours', 'h'),
        ('Drive Hours', 'drive_hours', 'h'),
        ('Total Hours', 'total_hours', 'h'),
        ('Total Miles', 'total_miles', 'mi'),
        ('Avg Distance/Job', 'avg_job_distance', 'mi'),
        ('Backtracking Events', 'backtracking_score', '')
    ]
    
    for label, key, unit in metrics_to_compare:
        greedy_val = greedy_metrics[key]
        ai_val = ai_metrics[key]
        diff = ai_val - greedy_val
        
        # Format the difference with a sign
        if key in ['drive_hours', 'total_miles', 'backtracking_score', 'avg_job_distance']:
            # Lower is better for these
            diff_str = f"{diff:+.1f} {unit}" if diff != 0 else "Same"
            if diff < 0:
                diff_str += " ✅"  # AI is better
            elif diff > 0:
                diff_str += " ⚠️"  # Greedy was better
        else:
            # Higher is better for these
            diff_str = f"{diff:+.1f} {unit}" if diff != 0 else "Same"
            if diff > 0:
                diff_str += " ✅"  # AI is better
            elif diff < 0:
                diff_str += " ⚠️"  # Greedy was better
        
        print(f"{label:<30} {greedy_val}{unit:<19} {ai_val}{unit:<19} {diff_str:<15}")
    
    print()
    print("="*80)
    
    # STEP 5: Calculate efficiency improvements
    if greedy_metrics['drive_hours'] > 0:
        drive_reduction_pct = ((greedy_metrics['drive_hours'] - ai_metrics['drive_hours']) / 
                               greedy_metrics['drive_hours'] * 100)
        print(f"💡 Drive Time Change: {drive_reduction_pct:+.1f}%")
    
    if greedy_metrics['job_count'] > 0:
        job_increase_pct = ((ai_metrics['job_count'] - greedy_metrics['job_count']) / 
                           greedy_metrics['job_count'] * 100)
        print(f"💡 Jobs Scheduled Change: {job_increase_pct:+.1f}%")
    
    print()
    
    # STEP 6: Show actual route sequences (first 10 jobs)
    print("="*80)
    print("🗺️  ROUTE SEQUENCES (First 10 Jobs)")
    print("="*80)
    print()
    
    print("GREEDY Route:")
    for i, job in enumerate(greedy_jobs[:10], 1):
        print(f"  {i}. {job.site_name} ({job.site_city}) - {job.sow_1} - {job.duration}h")
    if len(greedy_jobs) > 10:
        print(f"  ... and {len(greedy_jobs) - 10} more jobs")
    print()
    
    print("AI OPTIMIZER Route:")
    for i, job in enumerate(ai_jobs[:10], 1):
        print(f"  {i}. {job.site_name} ({job.site_city}) - {job.sow_1} - {job.duration}h")
    if len(ai_jobs) > 10:
        print(f"  ... and {len(ai_jobs) - 10} more jobs")
    print()
    
    # STEP 7: Save detailed results to file
    results = {
        'test_config': {
            'tech_id': tech_id,
            'region': region_name,
            'test_date': str(test_date),
            'start_location': start_location,
            'max_daily_hours': max_daily_hours,
            'total_jobs_available': len(jobs)
        },
        'greedy': {
            'metrics': greedy_metrics,
            'jobs': [
                {
                    'work_order': j.work_order,
                    'site_name': j.site_name,
                    'city': j.site_city,
                    'sow': j.sow_1,
                    'duration': j.duration,
                    'priority': j.jp_priority
                }
                for j in greedy_jobs
            ]
        },
        'ai_optimizer': {
            'metrics': ai_metrics,
            'jobs': [
                {
                    'work_order': j.work_order,
                    'site_name': j.site_name,
                    'city': j.site_city,
                    'sow': j.sow_1,
                    'duration': j.duration,
                    'priority': j.jp_priority
                }
                for j in ai_jobs
            ]
        }
    }
    
    output_file = f"route_comparison_{test_date.strftime('%Y%m%d')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"💾 Detailed results saved to: {output_file}")
    print()
    
    return results

# ============================================================================
# MAIN - Run the test
# ============================================================================

if __name__ == "__main__":
    print()
    print("╔════════════════════════════════════════════════════════════════════════════╗")
    print("║                    ROUTE OPTIMIZER TEST HARNESS                            ║")
    print("╔════════════════════════════════════════════════════════════════════════════╗")
    print()
    
    # ========================================================================
    # 🔧 CONFIGURE YOUR TEST HERE
    # ========================================================================
    
    # Which technician to test?
    TEST_TECH_ID = 416  # Change this to test different techs
    
    # Which region?
    TEST_REGION = 'WY_West'  # Change to any region in your database
    
    # What date to simulate? (Jobs will be pulled for this month)
    TEST_DATE = date(2025, 12, 1)  # Monday of a test week
    
    # Where does the tech start? (Get from database)
    # For now, using Tech 7's home location - we'll pull this automatically
    from supabase_client import supabase_client
    sb = supabase_client()
    tech_result = sb.table('technicians').select('*').eq('technician_id', TEST_TECH_ID).execute()
    
    if not tech_result.data:
        print(f"❌ Error: Technician {TEST_TECH_ID} not found in database!")
        sys.exit(1)
    
    tech = tech_result.data[0]
    START_LOCATION = (tech['home_latitude'], tech['home_longitude'])
    MAX_DAILY_HOURS = tech.get('max_daily_hours', 12)
    
    print(f"Testing with: {tech['name']}")
    print(f"Home Location: {START_LOCATION}")
    print()
    
    # ========================================================================
    # RUN THE COMPARISON
    # ========================================================================
    
    try:
        results = compare_routes(
            tech_id=TEST_TECH_ID,
            region_name=TEST_REGION,
            test_date=TEST_DATE,
            start_location=START_LOCATION,
            max_daily_hours=MAX_DAILY_HOURS
        )
        
        print("="*80)
        print("✅ Test Complete!")
        print("="*80)
        print()
        print("NEXT STEPS:")
        print("1. Review the comparison above")
        print("2. Check the JSON file for detailed route sequences")
        print("3. If AI optimizer looks promising, we'll add business rules next")
        print("4. If it's worse, we can tune the algorithm parameters")
        print()
        
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")
        import traceback
        traceback.print_exc()
        print()
        print("Common issues:")
        print("- Database connection failed")
        print("- No jobs found for that tech/region combination")
        print("- Missing imports or file paths")
