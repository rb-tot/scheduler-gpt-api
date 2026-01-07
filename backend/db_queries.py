# db_queries.py
from datetime import date
from typing import Any, Dict, List, Optional, Tuple
from supabase_client import sb_select

def get_job_pool(
    due_start: date,
    due_end: date,
    states: Optional[List[str]] = None,
    statuses: Tuple[str, ...] = ("Call", "Waitnig to Schedule"),
) -> List[Dict[str, Any]]:
    filters: List[Tuple[str, str, Any]] = [
        ("due_date", "gte", str(due_start)),
        ("due_date", "lte", str(due_end)),
        ("jp_status", "in", list(statuses)),
    ]
    if states:
        filters.append(("state", "in", states))
    return sb_select("job_pool", filters=filters)

def get_technicians(active_only: bool = True) -> List[Dict[str, Any]]:
    filters: Optional[List[Tuple[str, str, Any]]] = [("active", "eq", True)] if active_only else None
    return sb_select("technicians", filters=filters)

def get_job_eligibility_for_jobs(work_orders: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    filters = [("work_order", "in", work_orders)] if work_orders else None
    return sb_select("job_technician_eligibility", filters=filters)

def get_blackouts(tech_ids: List[int], start: date, end: date) -> List[Dict[str, Any]]:
    if not tech_ids:
        return []
    return sb_select(
        "blackouts",
        filters=[("technician_id", "in", tech_ids), ("date", "gte", str(start)), ("date", "lte", str(end))],
    )

def get_existing_schedule(tech_ids: List[int], start: date, end: date) -> List[Dict[str, Any]]:
    if not tech_ids:
        return []
    return sb_select(
        "scheduled_jobs",
        filters=[("technician_id", "in", tech_ids), ("date", "gte", str(start)), ("date", "lte", str(end))],
    )

def get_capacities(tech_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    filters = [("id", "in", tech_ids)] if tech_ids else None
    return sb_select("technicians", filters=filters, columns="id,max_daily_hours,max_weekly_hours,night_eligible")

import pandas as pd
from typing import Optional, List, Any

def _to_df(rows: Optional[List[dict]]) -> pd.DataFrame:
    return pd.DataFrame(rows or [])

def job_pool_df(
    due_start: Any = None,
    due_end: Any = None,
    states: Optional[List[str]] = None,
    statuses: Any = None  # None or "*" means no status filter
):
    #    Old code calls _jp() with no args. Support that by returning all rows.
   # If dates are provided, filter by due_date window and optional states/statuses.
    
    from supabase_client import sb_select  # local import to avoid cycles

    if due_start is None or due_end is None:
        rows = sb_select("job_pool")  # no filters, full table
        return _to_df(rows)

    ds = str(due_start)
    de = str(due_end)
    filters = [("due_date", "gte", ds), ("due_date", "lte", de)]
    if states:
        filters.append(("state", "in", list(states)))
    if statuses not in (None, "*"):
        if isinstance(statuses, (list, tuple)):
            stat_list = list(statuses)
        else:
            stat_list = [str(statuses)]
        filters.append(("jp_status", "in", stat_list))
    rows = sb_select("job_pool", filters=filters)
    return _to_df(rows)

def eligibility_df(work_orders: Optional[List[int]] = None):
    from supabase_client import sb_select
    filters = [("work_order", "in", list(work_orders))] if work_orders else None
    rows = sb_select("job_technician_eligibility", filters=filters)
    return _to_df(rows)

def technicians_df(active_only: bool = True):
    """
    Return technician DataFrame with legacy column names:
    - id -> technician_id
    - home_lat -> home_latitude
    - home_lng -> home_longitude
    """
    rows = get_technicians(active_only)
    df = _to_df(rows)

    rename_map = {
        "id": "technician_id",
        "home_lat": "home_latitude",
        "home_lng": "home_longitude",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    return df
