import os
from datetime import date, timedelta
from fastapi import FastAPI, HTTPException
import httpx
from dotenv import load_dotenv, find_dotenv

# Load .env from the current directory
dotenv_path = find_dotenv()
if not dotenv_path:
    raise RuntimeError("Cannot find .env file. Place it alongside main.py.")
load_dotenv(dotenv_path)

# Read environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")

# Ensure REST endpoint is correct
if SUPABASE_URL.endswith("/rest/v1"):
    REST_URL = SUPABASE_URL
else:
    REST_URL = f"{SUPABASE_URL}/rest/v1"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

app = FastAPI(title="Scheduler API")

@app.get("/workload")
async def get_workload(horizon_days: int = 10, target_hours: float = 45.0):
    """
    Generate clustering and region report of upcoming jobs.
    - Fetch all unscheduled jobs.
    - Cluster them by urgency and geography.
    - Return a 10-day summary per region and full details.
    """
    select_fields = ",".join([
        "work_order",
        "site_name",
        "site_city",
        "due_date",
        "sow_1",
        "duration",
        "region",
        "zone_3"
    ])
    # Fetch jobs via Supabase REST
    url = f"{REST_URL}/job_pool?select={select_fields}&status=eq.unscheduled"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=HEADERS)
        resp.raise_for_status()
        jobs = resp.json()

    # Parse due_date and assign clusters
    today = date.today()
    for job in jobs:
        job["due_date"] = date.fromisoformat(job["due_date"])
    sorted_jobs = sorted(
        jobs,
        key=lambda j: ((j["due_date"] - today).days, j["region"], j["zone_3"])
    )

    cluster_id = 1
    accumulated = 0.0
    for job in sorted_jobs:
        if accumulated + job["duration"] > target_hours:
            cluster_id += 1
            accumulated = 0.0
        accumulated += job["duration"]
        job["cluster_id"] = cluster_id

    # Filter for the next horizon_days
    cutoff = today + timedelta(days=horizon_days)
    upcoming = [j for j in sorted_jobs if j["due_date"] <= cutoff]

    # Aggregate per region
    report = {}
    for job in upcoming:
        reg = job["region"]
        rec = report.setdefault(reg, {"job_count": 0, "total_duration": 0.0, "details": []})
        rec["job_count"] += 1
        rec["total_duration"] += job["duration"]
        rec["details"].append({
            "cluster_id": job["cluster_id"],
            "work_order": job["work_order"],
            "site_name": job["site_name"],
            "site_city": job["site_city"],
            "due_date": job["due_date"],
            "sow_1": job["sow_1"],
            "duration": job["duration"],
        })

    summary = [
        {"region": region, "job_count": data["job_count"], "total_duration": data["total_duration"]}
        for region, data in report.items()
    ]

    return {"summary": summary, "details": report}
