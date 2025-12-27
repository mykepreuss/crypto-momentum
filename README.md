# Crypto Momentum Scout

Personal, alerts-first service that scans Crypto.com Exchange spot markets, detects early momentum,
and posts high-signal alerts.

Source of truth:
- `docs/PRD.md`
- `docs/SPEC.md`

## Local dev (recommended)
Requirements:
- Python **3.11+** (for local runs/tests)
- Docker (for Postgres)

Quick start (recommended):
```bash
./scripts/dev.sh
```

Reset local dev DB (deletes docker volume, then re-runs migrations):
```bash
./scripts/reset-dev-db.sh
```

Create a venv + install deps:
```bash
# Use your installed 3.11+ interpreter (e.g. python3.12).
python3.12 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e '.[dev]'
```

Start Postgres:
```bash
docker compose up -d db
```

Note (macOS): if `alembic upgrade head` fails with `role "postgres" does not exist`:
- Ensure `DATABASE_URL` points at the docker-mapped port (default: `127.0.0.1:5433`) and not a locally installed Postgres.
- If you have an old local docker volume from a previous run with a different `POSTGRES_USER`, reset local data with `docker compose down -v` then restart `docker compose up -d db`.

Run migrations:
```bash
alembic upgrade head
```

Run migrations inside docker (optional):
```bash
docker compose run --rm app alembic upgrade head
```

Run the API (includes background jobs once implemented):
```bash
uvicorn app.main:app --reload
```

## Tests
```bash
pytest -q
```

## Environment
See `.env.example` for defaults.

## Deployment (always-on)
This service runs background jobs on a 1-minute cadence, so deploy it as an always-on container/VM.
See `docs/DEPLOYMENT.md`.
