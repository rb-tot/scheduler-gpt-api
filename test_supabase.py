from supabase_client import supabase

res = supabase.table("technicians").select("technician_id,name").limit(1).execute()
print(res.data)
