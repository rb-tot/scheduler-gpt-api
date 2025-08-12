# db_queries.py
from supabase_client import supabase
import pandas as pd

def job_pool_df():
    data = supabase.table("job_pool").select("*").execute().data or []
    return pd.DataFrame(data)

def eligibility_df():
    data = supabase.table("job_technician_eligibility").select("*").execute().data or []
    return pd.DataFrame(data)

def technicians_df():
    data = supabase.table("technicians").select("*").execute().data or []
    return pd.DataFrame(data)
