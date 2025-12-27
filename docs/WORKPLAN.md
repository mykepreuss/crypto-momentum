# Crypto Momentum Scout — V1 Workplan

Last updated: 2025-12-27

## PRD/SPEC lock-ins (V1)

1. [x] Alerts-first only (no trading), no user watchlist, Crypto.com Exchange v1 **public REST only** (`public/get-instruments`, `public/get-tickers`, `public/get-candlestick`, `public/get-book`).
2. [x] Universe = top ~200 tradable markets by 24h dollar volume (default quote `USDT`) + **BTC baseline** pair for relative momentum.
3. [x] Data = 1m candles, **last closed candle only**, backfill ~180 on startup, then per-minute fetch `count=2` and upsert by `(symbol, t)`.
4. [x] Signal = returns + BTC-relative returns + acceleration + dollar-volume anomaly (`dv_z`) + breakout + 5m EMA trend filter + VWAP extension; rank + gate (and book spread only for top candidates).
5. [x] Alerts = state machine `OUT/IN`, entry threshold ~`0.80`, exit threshold ~`0.55`, plus stall/trend-break exits; budgets max `5 UP / 5 SLOWING` per rolling 24h; `SLOWING` only if `UP` in last 24h.

## Implementation milestones (order of work)

1. [x] **M0: Project scaffolding** (pyproject, app skeleton, FastAPI startup/shutdown, config loader, logging, Dockerfile + docker-compose).
2. [x] **M1: Database + migrations** (SQLAlchemy 2.x async + Alembic; tables from `docs/SPEC.md`, plus persisted config + universe membership representation).
3. [x] **M2: Exchange client** (`httpx.AsyncClient` wrapper for the 4 endpoints with retries/backoff, 429 handling, timeouts, concurrency semaphore).
4. [x] **M3: Universe refresh job** (pull instruments + tickers, filter tradable spot-like by quote, compute dollar volume, select top `MAX_UNIVERSE_SIZE`, pick BTC baseline; store and expose via `GET /universe`).
5. [x] **M4: Candle ingestion** (startup backfill ~180x 1m candles per symbol; minute loop fetch `count=2`; ignore any candle with `t >= current_minute_start`; upsert; maintain in-memory rolling buffers).
6. [x] **M5: Signal engine** (compute features + cross-sectional percentiles; apply hard gates; apply spread gate for top N; keep latest ranked snapshot and expose `GET /signals/latest`).
7. [x] **M6: State machine + alerting** (persistent OUT/IN per symbol; entry/exit rules + hysteresis + peak tracking; rolling 24h budgets + cooldowns; write alerts rows and deliver via notifier interface).
8. [x] **M7: Evaluation job** (compute `r_5m/r_15m/r_60m` + `mae_60m/mfe_60m` for entry alerts once future candles exist; store in `alert_evaluation`; expose summary via `GET /eval`).
9. [x] **M8: API surface** (`GET /health`, `GET/POST /config`, `GET /alerts`, `GET /alerts/{id}`) and responses are tuning-friendly.
10. [x] **M9: Tests**
    1. [x] Unit tests for parsing, features (returns/dv_z/vwap/EMA), scoring/percentiles, evaluation math, and key helpers.
    2. [x] Integration tests for candle ingestion & DB upsert idempotency (SQLite).
    3. [x] Integration tests for `OUT → IN → OUT` transitions with DB-backed state and budget/cooldown enforcement.

## “Done but should verify” checklist (local ops)

1. [x] Run `python -m pytest -q` in a Python 3.11+ virtualenv.
2. [x] `docker compose up -d db` then `alembic upgrade head` then run `uvicorn app.main:app --reload`.
3. [ ] Confirm end-to-end flow:
   - [x] `/universe` populates
   - [x] candles backfill completes
   - [x] `/signals/latest` returns ranked list
   - [x] alerts appear in `/alerts`
   - [ ] evaluations appear in `/eval` after enough future candles exist (up to ~60m after entry)
4. [x] Compile-check: `PYTHONPYCACHEPREFIX="$PWD/.pycache" python -m compileall -q app`.
