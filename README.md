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

Create a venv + install deps:
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e '.[dev]'
```

Start Postgres:
```bash
docker compose up -d db
```

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
