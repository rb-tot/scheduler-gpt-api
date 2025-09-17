
import pandas as pd
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2
from db_queries import job_pool_df as _jp, eligibility_df as _elig, technicians_df as _techs

# ===========================================================================
# Scheduler V4a (Supabase-backed) — SOW-week, fixed jobs, radius cap, seed prefs
# ===========================================================================

# Don't load on startup - will load when needed
job_pool_df = pd.DataFrame()
job_technician_eligibility_df = pd.DataFrame()
technicians_df = pd.DataFrame()
site_distance_matrix_df = pd.DataFrame()

# Ensure datetime types
if 'due_date' in job_pool_df.columns:
    job_pool_df['due_date'] = pd.to_datetime(job_pool_df['due_date'], errors='coerce')

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def haversine(lat1, lon1, lat2, lon2):
    """Distance in miles between two lat/lon points."""
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    c = 2*atan2(sqrt(a), sqrt(1-a))
    return R * c

def get_travel_time(from_lat, from_lon, to_lat, to_lon, from_site=None, to_site=None):
    """Return drive time in hours; 50 mph fallback if matrix pair not found."""
    try:
        if from_site and to_site and not site_distance_matrix_df.empty:
            row = site_distance_matrix_df[
                (site_distance_matrix_df.get('from_site_name') == from_site) &
                (site_distance_matrix_df.get('to_site_name') == to_site)
            ]
            if row is not None and not row.empty:
                return float(row['drive_time_hours'].iloc[0])
    except Exception:
        pass
    return haversine(from_lat, from_lon, to_lat, to_lon) / 50.0

# ---------------------------------------------------------------------------
# Core scheduler
# ---------------------------------------------------------------------------
def schedule_technician_week(
        tech_id: int,
        start_date: datetime,
        assigned_clusters=None,
        priority_work_orders=None,
        horizon_days: int = 21,
        target_sow_list=None,
        anchor_week_strategy: str = "exhaustive",
        allow_in_day_filler: bool = True,
        fixed_jobs=None,
        weekly_target_hours_min: int = 45,
        weekly_target_hours_max: int = 50,
        max_drive_minutes: int = 60,
        pre_night_day_job_cap: int = 4,
        radius_miles_cap: int | None = 250,
        seed_region: str | None = None,
        seed_cluster: int | None = None,
):
    """Return a one-week schedule list[{date, jobs, total_hours}] for a tech."""
    global job_pool_df, job_technician_eligibility_df, technicians_df
    
    # Load data if empty
    if job_pool_df.empty:
        job_pool_df = _jp()
    if job_technician_eligibility_df.empty:
        job_technician_eligibility_df = _elig()
    if technicians_df.empty:
        technicians_df = _techs()
    tech = technicians_df.loc[technicians_df['technician_id'] == tech_id].iloc[0]
    assigned_clusters    = assigned_clusters or []
    priority_work_orders = priority_work_orders or []
    target_sow_list      = target_sow_list or []
    fixed_jobs           = fixed_jobs or []

    week_end  = start_date + timedelta(days=6)
    month_end = (start_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    # Eligible WOs for this tech
    eligible_work_orders = job_technician_eligibility_df[
        job_technician_eligibility_df['technician_id'] == tech_id
    ]['work_order'].tolist()

    # Build WEEKLY working set
    in_scope_all = job_pool_df[
        (job_pool_df['work_order'].isin(eligible_work_orders)) &
        (job_pool_df['jp_status'] != 'Scheduled') &
        (job_pool_df['due_date'] <= month_end)
    ].copy()

    # Priority flag
    in_scope_all['is_priority'] = in_scope_all['work_order'].isin(priority_work_orders)

    # Horizon filter with overrides
    if horizon_days is not None:
        in_scope_all = in_scope_all[
            (in_scope_all['days_til_due'] <= horizon_days) |
            (in_scope_all['jp_priority'] == 'Monthly O&M') |
            (in_scope_all['night_test']) |
            (in_scope_all['is_priority'])
        ]

    # Optional restriction to assigned clusters with allowed overrides
    if assigned_clusters:
        mask_cluster  = in_scope_all['cluster_id'].isin(assigned_clusters)
        mask_sow      = in_scope_all['sow_1'].isin(target_sow_list) if target_sow_list else pd.Series(False, index=in_scope_all.index)
        mask_night    = in_scope_all['night_test']
        mask_priority = in_scope_all['is_priority']
        in_scope_all  = in_scope_all[mask_cluster | mask_sow | mask_night | mask_priority]

    # Sort for stability
    if 'zone_3' in in_scope_all.columns:
        in_scope_all = in_scope_all.sort_values(['days_til_due', 'zone_3'])
    else:
        in_scope_all = in_scope_all.sort_values(['days_til_due'])

    # Radius cap from tech home
    if radius_miles_cap is not None:
        def _miles_from_home(r):
            return haversine(tech['home_latitude'], tech['home_longitude'], r['latitude'], r['longitude'])
        in_scope_all = in_scope_all[in_scope_all.apply(_miles_from_home, axis=1) <= radius_miles_cap]

    # Seed region/cluster preference
    if seed_region is not None and 'region' in in_scope_all.columns:
        in_scope_all['region_pref'] = (in_scope_all['region'] == seed_region).astype(int)
    else:
        in_scope_all['region_pref'] = 0
    if seed_cluster is not None and 'cluster_id' in in_scope_all.columns:
        in_scope_all['cluster_pref'] = (in_scope_all['cluster_id'] == seed_cluster).astype(int)
    else:
        in_scope_all['cluster_pref'] = 0
    sort_cols = ['region_pref', 'cluster_pref', 'days_til_due']
    ascend    = [False, False, True]
    if 'zone_3' in in_scope_all.columns:
        sort_cols.append('zone_3'); ascend.append(True)
    in_scope_all = in_scope_all.sort_values(sort_cols, ascending=ascend)

    # Split SOW anchor vs filler
    if target_sow_list:
        anchor_pool = in_scope_all[in_scope_all['sow_1'].isin(target_sow_list)].copy()
        filler_pool = in_scope_all[~in_scope_all['sow_1'].isin(target_sow_list)].copy()
    else:
        anchor_pool = in_scope_all.iloc[0:0].copy()
        filler_pool = in_scope_all.copy()

    # Fixed jobs normalization
    fixed_by_date = {}
    for item in fixed_jobs:
        wo = int(item.get('wo'))
        dt = item.get('date')
        if isinstance(dt, str):
            try:
                dt = datetime.fromisoformat(dt).date()
            except Exception:
                dt = pd.to_datetime(dt, errors='coerce')
                dt = dt.date() if not pd.isna(dt) else None
        elif isinstance(dt, pd.Timestamp):
            dt = dt.date()
        elif isinstance(dt, datetime):
            dt = dt.date()
        elif hasattr(dt, 'date'):
            dt = dt.date()
        if dt is not None:
            fixed_by_date.setdefault(dt, []).append(wo)

    # Diagnostics collectors
    schedule              = []
    warnings_rows         = []
    daily_summary_rows    = []
    fixed_applied_rows    = []

    def log_warning(date, wo, reason, note=""):
        warnings_rows.append({
            'tech_id': tech_id,
            'date': date.strftime('%Y-%m-%d') if isinstance(date, (datetime, pd.Timestamp)) else str(date),
            'work_order': int(wo) if pd.notna(wo) else None,
            'reason': reason,
            'note': note
        })

    def remove_from_pools(work_order: int):
        nonlocal anchor_pool, filler_pool, in_scope_all
        anchor_pool = anchor_pool[anchor_pool['work_order'] != work_order]
        filler_pool = filler_pool[filler_pool['work_order'] != work_order]
        in_scope_all = in_scope_all[in_scope_all['work_order'] != work_order]

    # Helper filters
    def filter_for_date(pool_df: pd.DataFrame, d: datetime) -> pd.DataFrame:
        if pool_df.empty:
            return pool_df.copy()
        return pool_df[
            ((pool_df['is_recurring_site']) & (pool_df['due_date'] == d)) |
            ((~pool_df['is_recurring_site']) & (pool_df['due_date'] >= d))
        ].copy()

    def add_travel_and_capacity(df: pd.DataFrame, current_loc: dict, daily_hours: float,
                                max_hours: float, weekday: int) -> pd.DataFrame:
        out = df.copy()
        if out.empty:
            if 'travel_time' not in out.columns:
                out['travel_time'] = pd.Series(dtype='float64')
            if 'total_time' not in out.columns:
                out['total_time'] = pd.Series(dtype='float64')
            return out
        out['travel_time'] = out.apply(
            lambda r: get_travel_time(current_loc['lat'], current_loc['lon'],
                                      r['latitude'], r['longitude'],
                                      current_loc['site'], r['site_name']),
            axis=1
        )
        out['total_time'] = out['travel_time'] + out['duration']
        if weekday == 4:  # Friday constraints
            out = out[~out['site_name'].str.contains('King Soopers|City Market|Alta', na=False)]
        out = out[out['total_time'] + daily_hours <= max_hours]
        return out

    def pick_next(df: pd.DataFrame) -> pd.Series | None:
        if df.empty:
            return None
        sort_df = df.sort_values(
            by=['is_priority', 'days_til_due', 'travel_time', 'duration'],
            ascending=[False, True, True, True]
        )
        return sort_df.iloc[0]

    # Week state
    total_week_hours   = 0.0
    recovery_day       = False
    recovery_day_next  = False
    current_location   = {'lat': tech['home_latitude'], 'lon': tech['home_longitude'], 'site': None}
    current_zone_3     = None

    # Day loop (Mon–Fri)
    current_date = start_date
    weekly_hour_cap = min(float(tech['max_weekly_hours']), float(weekly_target_hours_max))
    while current_date <= week_end and total_week_hours < weekly_hour_cap:
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue

        daily_schedule = []
        daily_hours    = 0.0
        max_hours      = float(tech['max_daily_hours']) * (0.5 if recovery_day else 1.0)

        night_today_all = filter_for_date(in_scope_all[in_scope_all['night_test']], current_date)
        night_present_today = not night_today_all.empty

        pre_night_count      = 0
        night_job_scheduled  = False

        # Fixed jobs first (day), hold night-fixed for end
        fixed_day_jobs = []
        fixed_night_jobs = []
        key_date = current_date.date()
        if key_date in fixed_by_date:
            for wo in fixed_by_date[key_date]:
                row = job_pool_df[job_pool_df['work_order'] == wo]
                if row.empty:
                    log_warning(current_date, wo, 'fixed_job_missing', 'WO not found in job_pool')
                    continue
                r = row.iloc[0]
                (fixed_night_jobs if bool(r['night_test']) else fixed_day_jobs).append(r)

        for r in fixed_day_jobs:
            travel = get_travel_time(current_location['lat'], current_location['lon'],
                                     r['latitude'], r['longitude'],
                                     current_location['site'], r['site_name'])
            total_time = float(travel) + float(r['duration'])
            daily_schedule.append({
                'work_order':  int(r['work_order']),
                'site_name':   r['site_name'],
                'date':        current_date,
                'travel_time': float(travel),
                'job_time':    float(r['duration']),
                'total_time':  float(total_time),
                'night_job':   bool(r['night_test'])
            })
            daily_hours      += total_time
            total_week_hours += total_time
            current_location  = {'lat': r['latitude'], 'lon': r['longitude'], 'site': r['site_name']}
            current_zone_3    = r['zone_3'] if 'zone_3' in r else None
            remove_from_pools(int(r['work_order']))

        # Anchor SOW phase
        anchor_today = filter_for_date(anchor_pool, current_date) if not anchor_pool.empty else anchor_pool.copy()
        while daily_hours < max_hours and not anchor_today.empty:
            if night_present_today and pre_night_day_job_cap > 0 and pre_night_count >= pre_night_day_job_cap:
                break
            candidates = add_travel_and_capacity(anchor_today, current_location, daily_hours, max_hours, current_date.weekday())
            if candidates.empty:
                break
            non_night = candidates[~candidates['night_test']]
            if not non_night.empty:
                pick = non_night.sort_values(by=['is_priority','days_til_due','travel_time','duration'],
                                             ascending=[False,True,True,True]).iloc[0]
            else:
                break
            job_info = {
                'work_order':  int(pick['work_order']),
                'site_name':   pick['site_name'],
                'date':        current_date,
                'travel_time': float(pick['travel_time']),
                'job_time':    float(pick['duration']),
                'total_time':  float(pick['total_time']),
                'night_job':   bool(pick['night_test'])
            }
            daily_schedule.append(job_info)
            daily_hours      += job_info['total_time']
            total_week_hours += job_info['total_time']
            current_location = {'lat': pick['latitude'], 'lon': pick['longitude'], 'site': pick['site_name']}
            current_zone_3   = pick['zone_3'] if 'zone_3' in pick else None
            remove_from_pools(job_info['work_order'])
            anchor_today = anchor_today[anchor_today['work_order'] != job_info['work_order']]
            if night_present_today and not job_info['night_job']:
                pre_night_count += 1

        # In-day filler
        def get_filler_candidates(base_df: pd.DataFrame) -> pd.DataFrame:
            return add_travel_and_capacity(base_df, current_location, daily_hours, max_hours, current_date.weekday())

        if allow_in_day_filler and daily_hours < max_hours:
            if not (night_present_today and pre_night_day_job_cap > 0 and pre_night_count >= pre_night_day_job_cap):
                filler_today_all = filter_for_date(filler_pool, current_date)
                step = filler_today_all[filler_today_all['cluster_id'].isin(assigned_clusters)] if 'cluster_id' in filler_today_all.columns else filler_today_all.iloc[0:0].copy()
                step = get_filler_candidates(step)
                if 'travel_time' not in step.columns:
                    step['travel_time'] = pd.Series(dtype='float64')
                step = step[step['travel_time'] <= max_drive_minutes / 60.0]
                if step.empty:
                    step = get_filler_candidates(filler_today_all)
                    step = step[step['travel_time'] <= max_drive_minutes / 60.0]
                if step.empty and current_zone_3 is not None and 'zone_3' in filler_today_all.columns:
                    step = get_filler_candidates(filler_today_all[filler_today_all['zone_3'] == current_zone_3])
                if step.empty:
                    step = get_filler_candidates(filler_today_all)

                while daily_hours < max_hours and not step.empty:
                    if night_present_today and pre_night_day_job_cap > 0 and pre_night_count >= pre_night_day_job_cap:
                        break
                    pick = pick_next(step)
                    if pick is None:
                        break
                    job_info = {
                        'work_order':  int(pick['work_order']),
                        'site_name':   pick['site_name'],
                        'date':        current_date,
                        'travel_time': float(pick['travel_time']),
                        'job_time':    float(pick['duration']),
                        'total_time':  float(pick['total_time']),
                        'night_job':   bool(pick['night_test'])
                    }
                    daily_schedule.append(job_info)
                    daily_hours      += job_info['total_time']
                    total_week_hours += job_info['total_time']
                    current_location = {'lat': pick['latitude'], 'lon': pick['longitude'], 'site': pick['site_name']}
                    current_zone_3   = pick['zone_3'] if 'zone_3' in pick else None
                    remove_from_pools(job_info['work_order'])
                    filler_today_all = filter_for_date(filler_pool, current_date)
                    step = filler_today_all[filler_today_all['cluster_id'].isin(assigned_clusters)] if 'cluster_id' in filler_today_all.columns else filler_today_all.iloc[0:0].copy()
                    step = get_filler_candidates(step)
                    if 'travel_time' not in step.columns:
                        step['travel_time'] = pd.Series(dtype='float64')
                    step = step[step['travel_time'] <= max_drive_minutes / 60.0]
                    if step.empty:
                        step = get_filler_candidates(filler_today_all)
                        step = step[step['travel_time'] <= max_drive_minutes / 60.0]
                    if step.empty and current_zone_3 is not None and 'zone_3' in filler_today_all.columns:
                        step = get_filler_candidates(filler_today_all[filler_today_all['zone_3'] == current_zone_3])
                    if step.empty:
                        step = get_filler_candidates(filler_today_all)
                    if night_present_today and not job_info['night_job']:
                        pre_night_count += 1

        # Night job at end-of-day
        if fixed_night_jobs:
            r = fixed_night_jobs[0]
            travel = get_travel_time(current_location['lat'], current_location['lon'],
                                     r['latitude'], r['longitude'],
                                     current_location['site'], r['site_name'])
            total_time = float(travel) + float(r['duration'])
            daily_schedule.append({
                'work_order':  int(r['work_order']),
                'site_name':   r['site_name'],
                'date':        current_date,
                'travel_time': float(travel),
                'job_time':    float(r['duration']),
                'total_time':  float(total_time),
                'night_job':   True
            })
            daily_hours      += total_time
            total_week_hours += total_time
            current_location  = {'lat': r['latitude'], 'lon': r['longitude'], 'site': r['site_name']}
            current_zone_3    = r['zone_3'] if 'zone_3' in r else None
            remove_from_pools(int(r['work_order']))
            recovery_day_next = True
            night_job_scheduled = True
        elif night_present_today:
            night_today_all = add_travel_and_capacity(night_today_all, current_location, daily_hours, max_hours, current_date.weekday())
            if not night_today_all.empty:
                pick = pick_next(night_today_all)
                if pick is not None:
                    job_info = {
                        'work_order':  int(pick['work_order']),
                        'site_name':   pick['site_name'],
                        'date':        current_date,
                        'travel_time': float(pick['travel_time']),
                        'job_time':    float(pick['duration']),
                        'total_time':  float(pick['total_time']),
                        'night_job':   True
                    }
                    daily_schedule.append(job_info)
                    daily_hours      += job_info['total_time']
                    total_week_hours += job_info['total_time']
                    current_location = {'lat': pick['latitude'], 'lon': pick['longitude'], 'site': pick['site_name']}
                    current_zone_3   = pick['zone_3'] if 'zone_3' in pick else None
                    remove_from_pools(job_info['work_order'])
                    recovery_day_next = True
                    night_job_scheduled = True

        # Drive home
        if daily_schedule:
            last_lat = current_location['lat']
            last_lon = current_location['lon']
            dist_home = haversine(last_lat, last_lon, tech['home_latitude'], tech['home_longitude'])
            if dist_home <= 85 or current_date == week_end:
                return_time = dist_home / 50.0
                if daily_hours + return_time <= max_hours:
                    daily_hours      += return_time
                    total_week_hours += return_time
                    current_location  = {'lat': tech['home_latitude'], 'lon': tech['home_longitude'], 'site': None}
            schedule.append({'date': current_date, 'jobs': daily_schedule, 'total_hours': daily_hours})

        # Daily summary (diagnostics CSVs optional; disabled here)
        recovery_day = recovery_day_next
        recovery_day_next = False
        current_date += timedelta(days=1)

    # Fixed jobs outside week window
    for dtv, wos in fixed_by_date.items():
        if dtv < start_date.date() or dtv > week_end.date():
            day_date = datetime(dtv.year, dtv.month, dtv.day)
            daily_schedule = []
            current_location_out = {'lat': tech['home_latitude'], 'lon': tech['home_longitude'], 'site': None}
            for wo in wos:
                row = job_pool_df[job_pool_df['work_order'] == wo]
                if row.empty:
                    log_warning(day_date, wo, 'fixed_job_missing', 'WO not found in job_pool (outside week)')
                    continue
                r = row.iloc[0]
                travel = get_travel_time(current_location_out['lat'], current_location_out['lon'],
                                         r['latitude'], r['longitude'],
                                         current_location_out['site'], r['site_name'])
                total_time = float(travel) + float(r['duration'])
                daily_schedule.append({
                    'work_order':  int(r['work_order']),
                    'site_name':   r['site_name'],
                    'date':        day_date,
                    'travel_time': float(travel),
                    'job_time':    float(r['duration']),
                    'total_time':  float(total_time),
                    'night_job':   bool(r['night_test'])
                })
                current_location_out = {'lat': r['latitude'], 'lon': r['longitude'], 'site': r['site_name']}
                remove_from_pools(int(r['work_order']))
            if daily_schedule:
                schedule.append({'date': day_date, 'jobs': daily_schedule, 'total_hours': sum(j['total_time'] for j in daily_schedule)})

    schedule = sorted(schedule, key=lambda d: d['date'])
    return schedule

# ---------------------------------------------------------------------------
# Wrapper helpers
# ---------------------------------------------------------------------------
schedules = {}
scheduled_jobs_export = []

def assign_technician(tech_id, start_date,
                      assigned_clusters=None, priority_work_orders=None,
                      horizon_days=21,
                      target_sow_list=None,
                      anchor_week_strategy="exhaustive",
                      allow_in_day_filler=True,
                      fixed_jobs=None,
                      weekly_target_hours_min=45,
                      weekly_target_hours_max=50,
                      max_drive_minutes=60,
                      pre_night_day_job_cap=4,
                      radius_miles_cap=250,
                      seed_region=None,
                      seed_cluster=None,
                      commit=False):
    global scheduled_jobs_export
    scheduled_jobs_export = []

    tech_name = technicians_df.loc[technicians_df['technician_id'] == tech_id, 'name'].iloc[0]

    schedule = schedule_technician_week(
        tech_id=tech_id,
        start_date=start_date,
        assigned_clusters=assigned_clusters,
        priority_work_orders=priority_work_orders,
        horizon_days=horizon_days,
        target_sow_list=target_sow_list,
        anchor_week_strategy=anchor_week_strategy,
        allow_in_day_filler=allow_in_day_filler,
        fixed_jobs=fixed_jobs,
        weekly_target_hours_min=weekly_target_hours_min,
        weekly_target_hours_max=weekly_target_hours_max,
        max_drive_minutes=max_drive_minutes,
        pre_night_day_job_cap=pre_night_day_job_cap,
        radius_miles_cap=radius_miles_cap,
        seed_region=seed_region,
        seed_cluster=seed_cluster
    )

    schedules[tech_id] = schedule
    for day in schedule:
        for job in day['jobs']:
            # mark scheduled in main pool (local df copy)
            job_pool_df.loc[job_pool_df['work_order'] == job['work_order'], 'jp_status'] = 'Scheduled'
            scheduled_jobs_export.append({
                'work_order':         job['work_order'],
                'assigned_tech_id':   tech_id,
                'assigned_tech_name': tech_name,
                'start_date':         day['date'].strftime('%Y-%m-%d'),
                'duration':           job['job_time'],
                'site_name':          job['site_name'],
                'sow_1':              job_pool_df.loc[job_pool_df['work_order'] == job['work_order'], 'sow_1'].values[0],
                'site_city':          job_pool_df.loc[job_pool_df['work_order'] == job['work_order'], 'site_city'].values[0],
                'site_state':         job_pool_df.loc[job_pool_df['work_order'] == job['work_order'], 'site_state'].values[0],
                'due_date':           job_pool_df.loc[job_pool_df['work_order'] == job['work_order'], 'due_date'].values[0]
            })

        try:
            from db_writes import upsert_scheduled_jobs, mark_jobs_scheduled
            seen, batch = set(), []
            for r in scheduled_jobs_export:
                wo = int(r["work_order"])
                if wo in seen:
                    continue
                seen.add(wo)
                batch.append(r)
            upsert_scheduled_jobs(batch)          # writes to scheduled_jobs
            mark_jobs_scheduled(list(seen))       # flips job_pool.jp_status='Scheduled'
        except Exception as e:
            raise RuntimeError(f"supabase_write_failed: {type(e).__name__}: {e}")
    return schedule

def export_schedule():
    pd.DataFrame(scheduled_jobs_export).to_csv("scheduled_jobs.csv", index=False)
    job_pool_df.to_csv("job_pool_updated.csv", index=False)
    return {"scheduled_file": "scheduled_jobs.csv", "job_pool_file": "job_pool_updated.csv"}
