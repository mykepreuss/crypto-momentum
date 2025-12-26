# Technical Spec — Implementation Blueprint (Codex-ready)

## 1) System Overview
A single backend service that:
- Pulls market instruments + selects a liquid universe
- Maintains rolling 1m candle buffers per instrument
- Computes features → score → gates → rank
- Runs a state machine per instrument to detect regime changes
- Sends alerts (budgeted + throttled)
- Stores alerts + evaluation outcomes for tuning

Recommended tech stack (simple + pragmatic):
- Python 3.11+
- FastAPI for local API/dashboard endpoints
- PostgreSQL (or SQLite for local dev)
- httpx + asyncio for concurrent REST polling
- Optional: Docker compose for deployment

## 2) External Integration: Crypto.com Exchange API v1
REST base: `https://api.crypto.com/exchange/v1/{method}`

Endpoints used (Phase 1, public only):
- `public/get-instruments` → discover tradable markets
- `public/get-candlestick` → 1m candles for features and evaluation
- `public/get-tickers` → 24h volume + last price for universe liquidity filter
- `public/get-book` → bid/ask for spread gate (top candidates only)

## 3) Data Model (DB Schema)
Use SQL migrations (Alembic) or simple SQL init.

### 3.1 Tables
`instruments`
- symbol (PK, text)
- inst_type (text)
- base_ccy (text)
- quote_ccy (text)
- tradable (bool)
- display_name (text, nullable)
- last_seen_ts (bigint ms)

`candles_1m`
- symbol (text)
- t (bigint ms) — candle start time
- o,h,l,c (numeric)
- v (numeric)
- PK: (symbol, t)

`ticker_24h`
- symbol (PK, text)
- last_price (numeric) — from `a`
- vol_24h_base (numeric) — from `v`
- updated_ts (bigint ms)

`signal_state`
- symbol (PK)
- state (text: OUT | IN)
- last_state_change_ts (bigint ms)
- last_entry_alert_ts (bigint ms, nullable)
- last_exit_alert_ts (bigint ms, nullable)
- peak_price_since_entry (numeric, nullable)
- peak_ts_since_entry (bigint ms, nullable)

`alerts`
- id (uuid PK)
- ts (bigint ms)
- symbol (text)
- alert_type (text: MOMENTUM_UP | MOMENTUM_SLOWING)
- score (numeric)
- features_json (jsonb)
- message (text)
- delivered (bool)
- delivery_channel (text)

`alert_evaluation`
- alert_id (uuid FK)
- r_5m / r_15m / r_60m (numeric)
- mae_60m (numeric)
- mfe_60m (numeric)
- computed_ts (bigint ms)

## 4) Universe Selection (Liquid, Tradable, Not User-Specified)
Goal: scan broadly but avoid junk.

Default rules:
- Pull `public/get-instruments` and keep `tradable=true`
- Filter by quote currency: default `USDT` (configurable)
- Spot-only heuristic (inst_type values vary):
  - Prefer: include if symbol contains `_` and does not contain `-`
  - Exclude if symbol contains `-PERP` or looks like dated futures
  - Also allow config override: `allowed_inst_types`
- Apply liquidity filter using `public/get-tickers`:
  - Approx 24h dollar volume: `vol_24h_base * last_price`
  - Keep top `MAX_UNIVERSE_SIZE` by dollar volume (default 200)
  - Always include BTC baseline symbol

Baseline symbol (for relative momentum):
- Auto-select baseline as the highest dollar-volume BTC pair in chosen quote (e.g., `BTC_USDT`), else fallback to any BTC instrument.
- Baseline uses same candle ingestion pipeline.

## 5) Candle Ingestion (Minimal Complexity, High Reliability)
Why REST polling for v1:
- Subscribing to 1m candles for hundreds of pairs via websocket can introduce subscription limits/complexity.
- REST `public/get-candlestick` is sufficient and has generous rate limits.

Ingestion approach:
- On startup: for each symbol in universe, backfill last 180 candles (3 hours) of 1m.
- Each minute: for each symbol, fetch `count=2` of 1m candles and upsert.
- Use only fully closed candles for feature computation.
- Rule: ignore any candle with start time >= current minute start.

Concurrency + throttling:
- Use asyncio semaphore, e.g. `MAX_CONCURRENT_REQUESTS=25`.
- Retry with exponential backoff on errors; respect 429.

## 6) Feature Engineering (80/20 “pro” signal)
All features computed on 1m candles up to the last closed 1m bar.

Let:
- C0 = last closed candle close
- Ck = close k minutes ago
- V0 = last closed candle volume
- P0 = last closed candle close

### 6.1 Returns
- r1 = (C0 / C1) - 1
- r5 = (C0 / C5) - 1
- r15 = (C0 / C15) - 1

### 6.2 Market-relative returns (vs BTC baseline)
Compute the same returns for baseline BTC_BASE:
- rel_r5 = r5(symbol) - r5(BTC_BASE)
- rel_r15 = r15(symbol) - r15(BTC_BASE)

### 6.3 Acceleration (early ignition)
Use two adjacent 5m windows:
- r5_prev = (C5 / C10) - 1
- rel_r5_prev = r5_prev(symbol) - r5_prev(BTC_BASE)
- accel = rel_r5 - rel_r5_prev

### 6.4 Dollar-volume anomaly (participation)
dv_1m[t] = close[t] * volume[t]

Over last 60 closed 1m candles:
- dv_mean = mean(dv_1m)
- dv_std = std(dv_1m)
- dv_z = (dv_1m[last] - dv_mean) / max(dv_std, epsilon)

### 6.5 Breakout (range expansion)
breakout = 1 if C0 > max(high last 20 closed 1m candles) else 0

### 6.6 Trend filter (simple, stable)
Compute 5m candles by rolling up 1m data:
- bucket start = floor(t / (5m)) * 5m
- close = last close, high=max, low=min, volume=sum

Then compute:
- EMA9_5m on 5m closes
- EMA21_5m on 5m closes
- trend_ok = EMA9_5m > EMA21_5m

### 6.7 Overextension (don’t chase)
Compute VWAP over last 60 closed 1m candles:
- typical_price = (h + l + c)/3
- vwap60 = sum(typical_price * v) / sum(v)
- extension = (C0 - vwap60) / vwap60

## 7) Scoring + Gating + Ranking
### 7.1 Score (simple, explainable)
We rank cross-sectionally each scan:
- rank_rel_r15 = percentile rank of rel_r15 across universe
- rank_accel = percentile rank of accel across universe

Score:
```
score = 0.45*rank_rel_r15 + 0.35*rank_accel + 0.20*clamp(dv_z, -3, +6)/6 + 0.10*breakout
```

### 7.2 Hard Gates (must pass)
- trend_ok == True
- dv_z >= DVZ_MIN (default 1.5)
- extension <= EXTENSION_MAX (default 0.08 = +8% above vwap60)
- Basic liquidity: avg(dv_1m last 60) >= MIN_DV_1M_USD (default 10,000)

### 7.3 Microstructure Gate (top candidates only)
For the top 20 by score, call `public/get-book?depth=10` and compute:
- spread = (best_ask - best_bid) / mid

Gate:
- spread <= SPREAD_MAX (default 0.005 = 0.5%)

## 8) State Machine (Stable Alerts)
### 8.1 States
- OUT: not in momentum
- IN: momentum active (post entry)

### 8.2 Entry Condition (OUT → IN)
Trigger entry when:
- score >= ENTRY_SCORE_THRESHOLD (default 0.80)
- passes all gates
- symbol not on cooldown
- entry budget allows

When triggered:
- set state to IN
- initialize peak_price_since_entry = C0
- store last_entry_alert_ts = now
- create alert MOMENTUM_UP

### 8.3 Exit Conditions (IN → OUT)
Exit alert when any condition is true:

A) Score falls below exit threshold (hysteresis)
- score <= EXIT_SCORE_THRESHOLD (default 0.55)

B) Stall detection (fast profit protection)
- no new peak price for STALL_MINUTES (default 10)
- AND dv_z < STALL_DVZ_MAX (default 1.0)

C) Trend break
- EMA9_5m <= EMA21_5m

When exit triggered:
- set state to OUT
- store last_exit_alert_ts
- create alert MOMENTUM_SLOWING

### 8.4 Important rule (keeps alerts sane)
Only send MOMENTUM_SLOWING if:
- that symbol had MOMENTUM_UP alert in the last 24h

## 9) Alert Budgeting + Throttles (Recommended Defaults)
Rolling 24h budget:
- max_entry_alerts_24h = 5
- max_exit_alerts_24h = 5
- max_total_alerts_24h = 10

Cooldowns:
- GLOBAL_ENTRY_COOLDOWN_MIN = 10
- SYMBOL_ENTRY_COOLDOWN_MIN = 90
- SYMBOL_EXIT_COOLDOWN_MIN = 30

Per-scan cap:
- MAX_ENTRY_ALERTS_PER_SCAN = 1

## 10) Alert Payload + Templates
### 10.1 Alert JSON structure
Fields to include in features_json:
- score
- rel_r15, rel_r5, accel
- dv_z
- extension
- spread (if computed)
- breakout
- trend_ok

### 10.2 Message templates (example)
MOMENTUM_UP:
```
MOMENTUM UP: {symbol} | score {score} | rel15 {rel_r15:.2%} | dv_z {dv_z:.1f} | ext {extension:.1%} | spread {spread:.2%}
```

MOMENTUM_SLOWING:
```
MOMENTUM SLOWING: {symbol} | reason {reason} | score {score} | dv_z {dv_z:.1f} | last_peak {peak_price}
```

Notification channels are pluggable:
- Telegram / Email / Webhook

## 11) Evaluation Job (Built-in Learning Loop)
For each entry alert:
- At +5m, +15m, +60m: compute forward return from candle close at those offsets
- Compute over next 60m: MAE and MFE relative to entry close
- Store in `alert_evaluation`

## 12) Internal API Endpoints (FastAPI)
- GET /health
- GET /config
- POST /config (update thresholds, budgets)
- GET /universe
- GET /signals/latest (top ranked list, with scores/features)
- GET /alerts?limit=100
- GET /alerts/{id}
- GET /eval?days=7 (summary stats)

## 13) Code Layout (suggested)
```
app/
  main.py                  # FastAPI app + startup tasks
  config.py                # env + defaults
  db.py                    # engine/session
  models.py                # SQLAlchemy models
  migrations/              # optional (Alembic)

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
```

## 14) Testing Plan (must-have)
Unit tests:
- feature calculations (returns, dv_z, vwap60)
- EMA correctness on known series
- budget enforcement + cooldown logic

Integration tests (mock API):
- candle ingestion & upsert idempotency
- state transitions OUT→IN→OUT

Backtest harness (Phase 1-lite):
- run signal engine on stored candles for 7 days and output evaluation summary

## 15) Deployment (simple)
Docker container + Postgres

One process:
- FastAPI server
- background scan loop started on startup

Logs:
- JSON logs for scan timing, API errors, alerts fired

## Implementation Notes (critical gotchas)
- Use last closed candle only to avoid flip-flop alerts.
- Upsert candles by (symbol, t) to handle re-fetching safely.
- Don’t compute order book spread for everything — only for top candidates.
- Exit alerts only for prior entry alerts — keeps daily total under control.
