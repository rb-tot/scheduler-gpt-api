# supabase_client.py
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

_SUPABASE_URL = os.getenv("SUPABASE_URL")
_SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")

_client: Optional[Client] = None

def supabase_client() -> Client:
    global _client
    if _client is None:
        if not _SUPABASE_URL or not _SUPABASE_KEY:
            raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
        _client = create_client(_SUPABASE_URL, _SUPABASE_KEY)
    return _client

def _retry(fn, *, retries: int = 3, backoff: float = 0.8):
    last = None
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i == retries - 1:
                break
            time.sleep(backoff * (2 ** i))
    raise last

def sb_select(table: str, *, filters: Optional[List[Tuple[str, str, Any]]] = None, columns: str = "*", limit: Optional[int] = None):
    def _do():
        q = supabase_client().table(table).select(columns)
        for col, op, val in (filters or []):
            if op == "eq":
                q = q.eq(col, val)
            elif op == "neq":
                q = q.neq(col, val)
            elif op == "gte":
                q = q.gte(col, val)
            elif op == "lte":
                q = q.lte(col, val)
            elif op == "in":
                q = q.in_(col, val)
            else:
                raise ValueError(f"Unsupported op {op}")
        if limit:
            q = q.limit(limit)
        return q.execute().data
    return _retry(_do)

def sb_insert(table: str, rows: List[Dict[str, Any]]):
    return _retry(lambda: supabase_client().table(table).insert(rows).execute().data)

def sb_update(table: str, match: Dict[str, Any], patch: Dict[str, Any]):
    def _do():
        q = supabase_client().table(table).update(patch)
        for k, v in match.items():
            q = q.eq(k, v)
        return q.execute().data
    return _retry(_do)
