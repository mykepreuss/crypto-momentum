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

## Lookback evaluation (paper sim)
Run an alerts-only lookback using **stored alerts + stored 1m candles** (no signal regeneration):
```bash
python scripts/lookback_eval.py --days 30 --initial-usd 500
```

With explicit cost assumptions:
```bash
python scripts/lookback_eval.py --days 30 --fee-bps 10 --slippage-bps 5 --do-cost-per-month 15
```

USDT sleeve mode (keep BTC/ETH hold; trade with a USDT sleeve only):
```bash
python scripts/lookback_eval.py --days 30 --candles-source hist --alerts-csv replay_alerts.csv \
  --trade-mode usdt-sleeve --sleeve-usdt 200 --fee-bps 50 --slippage-bps 5
```

The script writes per-trade details to `lookback_trades.csv`.

## Historical candles (optional)
For long lookbacks, you can backfill 1m candles into a separate table (`candles_1m_hist`) that is **not**
pruned by the live retention job:
```bash
python scripts/backfill_hist_candles.py --days 365 --symbols BTC_USDT,ETH_USDT
```

Backfilling the full universe for 365 days can be very large and take a long time. Start smaller:
```bash
python scripts/backfill_hist_candles.py --days 30 --from-universe --max-symbols 50
```

To use the historical table in the lookback evaluation:
```bash
python scripts/lookback_eval.py --days 365 --candles-source hist
```

## Aggregated historical bars (optional)
If you want to evaluate the same signal logic on **slower timeframes** (to reduce churn/fees), you can
materialize aggregated bars from `candles_1m_hist` into `candles_hist_bars`.

Note: `candles_hist_bars.t` is stored at the **bar close time** (not bar start) to avoid lookahead in
offline replays.

Build 1h bars for a small universe:
```bash
python scripts/build_hist_bars.py --days 365 --bar-minutes 60 --from-universe --max-symbols 50 --replace
```

Then run replay/sweep using bar candles:
```bash
python scripts/replay_backtest.py --days 365 --from-universe --max-symbols 50 --bar-minutes 60
python scripts/parameter_sweep.py --days 30 --from-universe --max-symbols 50 --bar-minutes 60
```

And evaluate regenerated alerts using bar candles:
```bash
python scripts/lookback_eval.py --days 30 --candles-source hist --bar-minutes 60 --alerts-csv replay_alerts.csv
```

## Offline replay backtest (regenerate alerts)
If you have historical candles in `candles_1m_hist`, you can run an offline replay that regenerates
`MOMENTUM_UP` / `MOMENTUM_SLOWING` alerts from the historical candles (no live API calls):
```bash
python scripts/replay_backtest.py --days 365 --from-universe --max-symbols 50
```

This writes `replay_alerts.csv`. You can then run the paper-trade lookback using those regenerated alerts:
```bash
python scripts/lookback_eval.py --days 365 --candles-source hist --alerts-csv replay_alerts.csv
```

## Offline parameter sweep (tuning)
Run an offline grid search over key thresholds using `candles_1m_hist` (regenerates alerts per config),
and write `sweep_results.csv`:
```bash
python scripts/parameter_sweep.py --days 30 --from-universe --max-symbols 50
```

USDT sleeve mode (recommended if you want to include realistic taker fees):
```bash
python scripts/parameter_sweep.py --days 30 --from-universe --max-symbols 50 \
  --trade-mode usdt-sleeve --sleeve-usdt 200 --fee-bps 50 --slippage-bps 5
```

Customize the grid (repeat `--grid`):
```bash
python scripts/parameter_sweep.py --days 30 --from-universe --max-symbols 50 \
  --grid min_dv_1m_usd=0,100,500,2000 \
  --grid dvz_min=-0.5,0,0.5,1.5 \
  --grid entry_score_threshold=0.7,0.75,0.8 \
  --grid extension_max=0.08,0.12
```

If your grid is large, either sample configs:
```bash
python scripts/parameter_sweep.py --days 30 --from-universe --max-symbols 50 --sample 50 --seed 1
```
or confirm you want the full run with `--yes-large-grid`.

## Deployment (always-on)
This service runs background jobs on a 1-minute cadence, so deploy it as an always-on container/VM.
See `docs/DEPLOYMENT.md`.
