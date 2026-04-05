# Public Data Platform

Portfolio-friendly, multi-tenant public-data analytics platform for restaurant/food-truck operations.

## Security-first config setup

This repo is safe to publish: public config files contain placeholders only.

1. Copy examples to local secret files:
   - `Copy-Item config.example.json config.local.json`
   - `Copy-Item credentials.example.json credentials.local.json`
2. Fill real secrets in `config.local.json` (and `credentials.local.json` if used).
3. Run scripts normally. The app automatically loads `config.local.json` first, then falls back to `config.json`.

## Quick start

1. Create/activate venv
2. Install deps
   - `python -m pip install -r requirements.txt` (or run `maintenance.py`)
3. Run schema migration
   - `python migrate_schema.py`
4. Seed demo tenant
   - `python seed_demo_tenant.py`
5. Start API
   - `python -m uvicorn api_service:app --host 127.0.0.1 --port 8000`

## Main routes

- `/admin/login` admin portal login
- `/client/login` client portal login
- `/client/restaurant` restaurant analytics page

## Notes for GitHub publishing

- Do not commit `config.local.json`, `credentials.local.json`, `.env`, or `.venv/`.
- Rotate any keys that were previously exposed before this hardening step.
