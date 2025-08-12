from supabase_client import supabase
import pandas as pd, numpy as np, datetime as dt

def _norm(v):
    if isinstance(v, (pd.Timestamp, dt.datetime, dt.date, np.datetime64)):
        return str(pd.to_datetime(v).date())
    if isinstance(v, np.generic):
        return v.item()
    return v

def _clean(row: dict) -> dict:
    return {k: _norm(v) for k, v in row.items()}

def upsert_scheduled_jobs(rows):
    if not rows:
        return
    # dedupe by work_order inside the batch
    uniq = {}
    for r in rows:
        wrk = int(r["work_order"])
        uniq[wrk] = _clean(r)  # keep last occurrence
    batch = list(uniq.values())
    supabase.table("scheduled_jobs").upsert(batch, on_conflict="work_order").execute()

def mark_jobs_scheduled(work_orders):
    if not work_orders:
        return
    # dedupe ids too
    work_orders = list({int(w) for w in work_orders})
    supabase.table("job_pool").update({"jp_status": "Scheduled"}).in_("work_order", work_orders).execute()

