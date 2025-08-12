import os
from fastapi import FastAPI, HTTPException
import httpx
from dotenv import load_dotenv, find_dotenv

# ─── 1) Load .env ────────────────────────────────────────────────
dotenv_path = find_dotenv()
if not dotenv_path:
    raise RuntimeError("Cannot find .env file. Place it alongside main.py")
load_dotenv(dotenv_path)

# ─── 2) Read & validate env vars ─────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")

# ─── 3) Build REST URL ──────────────────────────────────────────
# Avoid doubling "/rest/v1"
if SUPABASE_URL.rstrip("/").endswith("/rest/v1"):
    REST_URL = SUPABASE_URL.rstrip("/")
else:
    REST_URL = f"{SUPABASE_URL.rstrip('/')}/rest/v1"

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
}

# ─── 4) FastAPI app & endpoints ──────────────────────────────────
app = FastAPI(title="Scheduler Read-Only API")

@app.get("/technicians")
def list_technicians():
    try:
        resp = httpx.get(f"{REST_URL}/technicians?select=*&order=technician_id",
                         headers=HEADERS)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/job_pool")
def list_job_pool():
    try:
        resp = httpx.get(f"{REST_URL}/job_pool?select=*&order=due_date&limit=200",
                         headers=HEADERS)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/scheduled_jobs")
def list_scheduled_jobs():
    try:
        resp = httpx.get(f"{REST_URL}/scheduled_jobs?select=*&order=start_date&limit=200",
                         headers=HEADERS)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

  