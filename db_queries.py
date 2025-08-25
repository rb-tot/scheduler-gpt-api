# db_queries.py
from datetime import date
from typing import Any, Dict, List, Optional, Tuple
from supabase_client import sb_select

def get_job_pool(
    due_start: date,
    due_end: date,
    states: Optional[List[str]] = None,
    statuses: Tuple[str, ...] = ("ready", "pending"),
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
