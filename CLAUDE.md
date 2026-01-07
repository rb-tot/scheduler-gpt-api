# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SchedulerGPT is a job scheduling optimization system for managing technician workload across geographic regions. It uses a Python FastAPI backend with a vanilla JavaScript frontend, connected to Supabase PostgreSQL with PostGIS for geospatial features.

## Development Commands

```bash
# Install dependencies
pip install -r backend/requirements.txt

# Start development server (serves both API and frontend)
python backend/scheduler_api.py
# OR
uvicorn backend.scheduler_api:app --reload --host 0.0.0.0 --port 8000

# Production start (used by Render)
uvicorn backend.scheduler_api:app --host 0.0.0.0 --port $PORT
```

No npm or build tools required - frontend is vanilla HTML/JS served directly by FastAPI.

## Architecture

```
Frontend (Vanilla JS/HTML)          Backend (FastAPI)              Database (Supabase)
├─ index.html (main scheduler)      ├─ scheduler_api.py            ├─ job_pool
├─ ai-scheduler.html                │   (main API, 2800+ lines)    ├─ scheduled_jobs
├─ data-manager.html                ├─ scheduler_v5_geographic.py  ├─ technicians
├─ tech-manager.html                │   (geographic-first algo)    ├─ job_technician_eligibility
├─ schedule-review-dashboard.html   ├─ scheduler_fillin.py         ├─ sites
├─ analysis.html/js                 │   (fill-in scheduling)       ├─ regions (PostGIS)
└─ style.css                        ├─ scheduler_utils.py          ├─ site_distances
                                    ├─ db_queries.py               └─ time_off_requests
                                    └─ supabase_client.py
```

**Data flow:** Frontend → FastAPI REST endpoints → Scheduling algorithms → Supabase

## Key Files

- **backend/scheduler_api.py** - Main API server with all endpoints. Job management, scheduling, analytics, and data import routes.
- **backend/scheduler_v5_geographic.py** - Geographic-first scheduling algorithm. Groups jobs by region, assigns based on tech location and capacity.
- **backend/scheduler_fillin.py** - Fill-in mode scheduling. Finds gaps in existing schedules and fills with nearby jobs.
- **backend/scheduler_utils.py** - Shared utilities (haversine distance, drive time calculations).
- **backend/db_queries.py** - Data access layer for Supabase queries.
- **backend/supabase_client.py** - Supabase client wrapper with retry logic.

## Scheduling Algorithms

**Geographic-First (scheduler_v5_geographic.py):**
- Groups jobs by region, assigns highest priority first
- Builds optimal routes within regions
- Uses tech home locations for route optimization
- Tracks daily/weekly capacity limits

**Fill-In Mode (scheduler_fillin.py):**
- Analyzes existing schedules for gaps
- Fills gaps with nearby jobs in same region
- Smart corridor routing between existing appointments

**Key calculations:**
- Haversine formula for distance
- Drive time = distance ÷ 55 mph
- Capacity tracked via max_weekly_hours/max_daily_hours

## API Structure

Main endpoint categories in scheduler_api.py:
- `/api/jobs/*` - Job pool management
- `/api/schedule/*` - Schedule generation and assignment
- `/api/technicians/*` - Technician roster
- `/api/analysis/*` - Monthly analytics
- `/api/regions/*` - Geographic regions

Authentication: API key via `X-API-Key` header

## Environment Variables

Required in `.env`:
```
SUPABASE_URL=<your_supabase_url>
SUPABASE_SERVICE_ROLE_KEY=<your_service_role_key>
ACTIONS_API_KEY=devkey123
PUBLIC_BASE_URL=http://localhost:8000
```

## Database

Uses Supabase PostgreSQL with PostGIS. Key tables:
- **job_pool** - Unscheduled jobs with geometry
- **scheduled_jobs** - Jobs assigned to technicians
- **technicians** - Roster with home locations and capacity
- **job_technician_eligibility** - Job-tech qualification mapping
- **regions** - Geographic boundaries (PostGIS geometry)
- **site_distances** - Pre-calculated drive time matrix

Staging tables (stg_*) used for bulk CSV imports.

## Frontend Pages

Served by FastAPI from `/frontend` directory:
- `/` → Main scheduler dashboard
- `/ai-scheduler` → AI scheduling interface
- `/data-manager` → CSV import and job management
- `/tech-manager` → Technician roster management
- `/analysis` → Monthly performance analytics
- `/schedule-review-dashboard` → Schedule review UI
