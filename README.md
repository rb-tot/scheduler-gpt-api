# Unified Job Scheduler

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the server:
```bash
python scheduler_api_unified.py
```

3. Open browser:
```
http://localhost:8000
```

## Features

- **Job Pool**: See all unscheduled jobs with smart filtering
- **Map View**: Visual job locations and tech homes
- **Schedule Calendar**: Week view for each technician
- **Smart Assignment**: Validate and assign jobs with conflict detection
- **Tech Roster**: See capacity and utilization for all techs

## API Endpoints

- `GET /api/jobs/unscheduled` - Get all unscheduled jobs
- `GET /api/technicians/all` - Get all techs with current workload
- `GET /api/schedule/week` - Get a tech's weekly schedule
- `POST /api/schedule/assign` - Assign a job to tech + date
- `DELETE /api/schedule/remove/{work_order}` - Remove job from schedule
- `GET /api/schedule/suggestions` - Smart job recommendations
- `POST /api/schedule/optimize-day` - Optimize route for a day

## Project Structure

```
/
├── scheduler_api_unified.py   # Main API server
├── supabase_client.py          # Database client
├── requirements.txt            # Python dependencies
├── .env                        # Environment config
└── frontend/
    ├── scheduler.html          # Main UI
    ├── scheduler.js            # Application logic
    └── scheduler.css           # Styles
```

## Environment Variables

Required in `.env`:
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` - Service role key
- `ACTIONS_API_KEY` - API authentication key
- `PUBLIC_BASE_URL` - Base URL for API (default: http://localhost:8000)
