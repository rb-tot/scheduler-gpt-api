# db_writes.py
from typing import Any, Dict, List, Optional
from supabase_client import sb_upsert, sb_insert, sb_update
from supabase_client import sb_update_in

def upsert_scheduled_jobs(rows: List[Dict[str, Any]], on_conflict: Optional[List[str]] = None):
    # Default dedupe: work_order + date + technician_id
    return sb_upsert("scheduled_jobs", rows, on=on_conflict or ["work_order", "date", "technician_id"])

def set_job_status(work_order_ids: List[int], new_status: str):
    for wid in work_order_ids:
        sb_update("job_pool", {"work_order": wid}, {"jp_status": new_status})

def write_audit(actor: str, action: str, details: Dict[str, Any], idempotency_key: Optional[str] = None):
    row = {"actor": actor, "action": action, "details": details, "idempotency_key": idempotency_key}
    return sb_insert("audit_log", [row])

def reserve_block(technician_id: int, date_str: str, hours: float, reason: str):
    return sb_insert("blackouts", [{"technician_id": technician_id, "date": date_str, "hours_blocked": hours, "reason": reason}])


def mark_jobs_scheduled(work_orders: List[int], status: str = "Scheduled"):
    """Set jp_status for these work orders. Dedup + bulk update."""
    if not work_orders:
        return []
    ids = sorted({int(w) for w in work_orders})
    return sb_update_in("job_pool", "work_order", ids, {"jp_status": status})

