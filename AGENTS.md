# AGENTS.md

## Purpose
Implement V1 of **Crypto Momentum Scout** (alerts-first) using the existing PRD and Spec from this chat, without any LLM features.

Source of truth docs:
- `docs/PRD.md`
- `docs/SPEC.md`

V1 is a personal service that:
- Discovers a liquid universe of tradable Crypto.com Exchange markets (no user-specified watchlist)
- Ingests 1m candles via public REST endpoints
- Computes a simple, explainable momentum score with institutional-style guardrails
- Sends a small number of high-signal alerts per rolling 24 hours:
  - Up to 5 MOMENTUM_UP alerts
  - Up to 5 MOMENTUM_SLOWING alerts (only for symbols that had an entry alert in the last 24h)
- Logs alerts and computes evaluation metrics (+5m/+15m/+60m forward returns, MAE/MFE)

No auto-trading in V1.

---

## Non-goals (V1)
- No LLM integration
- No private endpoints (no API keys needed)
- No portfolio tracking or PnL
- No on-chain, sentiment, or news ingestion
- No multi-exchange support

---

## Implementation principles
- Deterministic math, fully testable
- Use last closed candles only (avoid alert flapping)
- Favor ranking and gating over many thresholds
- Keep performance acceptable for ~200 symbols on 1-minute cadence
- Store enough data to evaluate and tune later

---

## Tech stack (recommended)
- Python 3.11+
- FastAPI for the API
- SQLAlchemy 2.x (async) and Alembic
- Postgres in production (SQLite allowed for local dev)
- httpx (async) for REST calls
- pytest for tests
- Docker Compose for local run

---

## External API: Crypto.com Exchange v1 (public only)
Use these endpoints only:
- `public/get-instruments`
- `public/get-tickers`
- `public/get-candlestick`
- `public/get-book`

Notes:
- REST base: `https://api.crypto.com/exchange/v1/{method}`
- Candlestick response includes o/h/l/c/v/t and is suitable for 1m ingestion
- Only use closed candles for scoring and evaluation

---

## Deliverables
1. Working service that can run continuously (scan loop) and send alerts.
2. Database schema with migrations.
3. API endpoints to inspect config, universe, latest ranks, alerts, and evaluation summary.
4. Tests for core math and state machine.
5. Local docker-compose setup.

---

## Repository layout (target)
```
app/
  main.py                  # FastAPI app + startup tasks
  config.py                # env + defaults
  db.py                    # engine/session
  models.py                # SQLAlchemy models

  exchange_client.py       # REST calls: instruments, candles, tickers, book
  universe.py              # universe build + refresh
  candles.py               # candle upsert + rolling buffers
  features.py              # feature computations
  scoring.py               # scoring + ranking
  state_machine.py         # OUT/IN logic + hysteresis
  alerting.py              # budgets + cooldowns + formatting
  notifier/
    base.py
    telegram.py
    email.py
    webhook.py

  jobs/
    scan_loop.py           # main minute loop
    evaluation.py          # forward return/mae/mfe

tests/
  unit/
  integration/

docs/
  PRD.md
  SPEC.md

docker-compose.yml
Dockerfile
pyproject.toml
README.md
```

---

## Critical gotchas (must follow)
- Use last closed candle only (avoid alert flip-flops).
- Upsert candles by (symbol, t) to make re-fetching idempotent.
- Compute order book spread only for top candidates (avoid N+1 calls).
- Only send MOMENTUM_SLOWING for symbols that had MOMENTUM_UP in the last 24h.
