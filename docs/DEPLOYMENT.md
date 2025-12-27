# Deployment (Cloud / Always-On)

Crypto Momentum Scout is **not** a request-only API: it runs background jobs (universe refresh, 1m candle ingestion, scoring, state machine, alert delivery) on a 1-minute cadence.

That means you should deploy it as an **always-on container/VM** (not a serverless “run per request” function).

## Option A (recommended): Small VM + Docker Compose

This is the lowest-complexity, “runs like local dev” option.

1) Create a small Linux VM
- Providers: AWS Lightsail, DigitalOcean Droplet, Hetzner, etc.
- Size: 1–2 GB RAM is usually enough for ~200 symbols.

2) Install Docker + Docker Compose (plugin) on the VM

3) Copy the repo onto the VM
- `git clone ...` (recommended), or upload a tarball.

4) Create a `.env` on the VM
- Start from `.env.example`.
- Set at least:
  - `SLACK_WEBHOOK_URL=...`
  - `SLACK_CHANNEL_NAME=...`
  - `ADMIN_TOKEN=...` (recommended: protects `POST /config`)

5) Start Postgres and run migrations
```bash
docker compose up -d db
docker compose run --rm app alembic upgrade head
```

6) Start the app container
```bash
docker compose up -d app
docker compose logs -f app
```

7) Lock down access
- Prefer allowing port `8000` only from your IP, or put the service behind a reverse proxy + auth (or VPN like Tailscale).
- Do **not** expose Postgres publicly.

8) Verify
```bash
curl http://YOUR_SERVER_IP:8000/health
curl -X POST http://YOUR_SERVER_IP:8000/notify/test -H 'Content-Type: application/json' -d '{"text":"hello from cloud"}'
```

Notes:
- Run **one instance** only. Scaling horizontally will duplicate scanning + alerts.
- Avoid multi-worker Uvicorn/Gunicorn for the same reason (background tasks would run per worker).

## Option B: PaaS (Fly.io / Render / Railway)

Use a platform that supports **always-on containers** plus a managed Postgres.

Requirements:
- Single instance (no autoscaling)
- A “release” / “deploy” command to run `alembic upgrade head` before starting the web process
- Secrets management for `SLACK_WEBHOOK_URL` + `ADMIN_TOKEN`

If you tell me which platform you want (Fly.io vs Render vs Railway), I can add the smallest possible deployment config files to this repo.

