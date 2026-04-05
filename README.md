# Public Data Platform

Multi-tenant public-data analytics platform designed for restaurant and food-truck operations.

It ingests live urban signals (traffic, weather, air quality, airport activity), computes interpretable business scores, and serves both API consumers and web operators through admin/client portals.

## Why this project matters

- Built as an end-to-end product workflow, not just a script.
- Converts raw sensor-style data into actionable operations guidance.
- Demonstrates backend engineering, data processing, multi-tenant auth, and practical UI delivery.

## Core capabilities

- Scheduled multi-stream ingestion into MySQL.
- Processing layer with feature signals and stream-health tracking.
- Multi-tenant API with entitlement checks.
- Admin and client login/session flows.
- Purchase-token redemption and API key lifecycle (create/rotate/revoke).
- Restaurant analytics view with opportunity, risk, comfort, trend, and recommendation.

## Architecture overview

1. Orchestrator collects stream data on a fixed schedule.
2. Processor normalizes events and computes business-facing signals.
3. API service exposes tenant-scoped endpoints and web experiences.
4. Admin/client surfaces consume the same signals for decision support.

## Tech stack

- Python 3.13
- FastAPI + Uvicorn
- MySQL 8
- Pydantic
- Requests + Schedule

## Quick start

1. Create and activate your virtual environment.
2. Install dependencies.
3. Configure local secrets.
4. Run migration and seed scripts.
5. Start API service.

PowerShell example:

```powershell
python -m pip install -r requirements.txt
Copy-Item config.example.json config.local.json
Copy-Item credentials.example.json credentials.local.json
python migrate_schema.py
python seed_demo_tenant.py
python -m uvicorn api_service:app --host 127.0.0.1 --port 8000
```

## Main routes

- /admin/login : Admin portal login
- /client/login : Client portal login
- /client/restaurant : Restaurant analytics page

## Security and publishing

This repository is structured for public sharing:

- Public config files are placeholders.
- Local secrets live in config.local.json and credentials.local.json.
- Local secret files are ignored by Git.

Before publishing any similar project:

1. Rotate keys that were ever exposed.
2. Avoid committing .env or local config files.
3. Use placeholder values in tracked examples.

## Suggested portfolio enhancements

- Add screenshots in a docs/images folder and reference them here.
- Add a short demo video link showing the client analytics flow.
- Add a one-paragraph "Engineering challenges" section with lessons learned.
