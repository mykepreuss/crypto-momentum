# PRD — Crypto Momentum Scout (Alerts-First)

## 1) Product Summary
Build a personal app/service that continuously scans a liquid universe of Crypto.com Exchange markets, detects early momentum leaders on minutes-to-hours horizons, and sends high-signal alerts:

- **MOMENTUM_UP** when a coin enters a strong momentum regime (early enough to capture upside)
- **MOMENTUM_SLOWING** when that momentum stalls or reverses (to support exiting before giveback)

Phase 1 is alerts only (no auto-trading). Phase 2+ can add paper trading and eventually auto-execution.

## 2) Goals
- Early detection: alert on momentum near the start of the move (not “already top gainer”).
- Quality over quantity: target ~10 actionable push alerts per day (rolling 24h).
- Tradeability: avoid illiquid/spread-y pumps that are hard to enter/exit.
- Rule-based exits: momentum slowing alerts should be reliable and timely.

## 3) Non-Goals (v1)
- No portfolio tracking or P&L calculations.
- No on-chain/sentiment/news signals (keep v1 simple).
- No exchange execution (v1); only signal generation + alerts.
- No “predict the future” ML model. This is a scoring + gating system.

## 4) User & Use Cases
User: You (day trading).

Time horizon: minutes → hours (not seconds).

Behavior: You want the system to identify what to watch/trade without you specifying tickers.

Primary use cases:
- “What’s breaking out right now?” → receive **MOMENTUM_UP** alert.
- “Is the move stalling?” → receive **MOMENTUM_SLOWING** alert for the same coin.
- Review: see the daily alerts and whether they worked (forward return + MAE/MFE).

## 5) Glossary (for clarity)
- **Spot:** buying/selling the actual coin (e.g., buy SOL/USDT). No expiry; usually no mandatory leverage.
- **Perps (perpetual swaps):** derivative contract tracking price with funding; can use leverage; different risk model.
- **Momentum:** price moving strongly in one direction with participation (volume/flow), often continuing short-term.
- **Relative momentum:** coin performance vs BTC (or market baseline), not just raw % move.

## 6) Product Strategy (the “pro” simplification)
We implement an institutional-style approach with minimal complexity:
- Cross-sectional ranking: compare coins against each other (leaders), not just thresholds.
- Confirmation via participation: dollar-volume anomaly.
- Volatility/extension sanity checks: avoid “too late” spikes.
- Microstructure gate: spread filter for top candidates only.
- State machine + hysteresis: stable signals; avoids flapping.

## 7) Alert Budget Recommendation
Default (recommended):
- Max total push alerts: 10 per rolling 24 hours
- Max entry alerts: 5 per rolling 24 hours
- Max exit alerts: 5 per rolling 24 hours
- Exit alerts are only eligible for instruments with an entry alert in the last 24h

Budgets apply to alert events created by the signal engine; notification delivery is best-effort with brief retries and does not change eligibility.

This produces a tight loop: at most ~5 “opportunities” per day, each with a corresponding “slowdown” signal.

## 8) Success Metrics (v1)
Tracked automatically per alert:
- Forward returns after **MOMENTUM_UP**: +5m, +15m, +60m
- MAE (max adverse excursion) over next 60m
- MFE (max favorable excursion) over next 60m
- Alert precision proxy: % of entry alerts where +15m return > 0
- Timeliness proxy: MFE occurs after alert (not before)

Targets (initial, not promises):
- ≥55% of **MOMENTUM_UP** alerts have positive +15m returns
- Median MAE smaller than median MFE (signal not constantly underwater)

## 9) Phased Roadmap
### Phase 1 (MVP): Alerts + dashboard
- Universe discovery
- Candle ingestion
- Signal scoring + gating
- Alert delivery (Telegram/Email/Webhook)
- Logging + evaluation metrics

### Phase 2: Paper trading
- Simulated fills + slippage model (simple)
- Entry/exit rule simulation
- Tune thresholds via data

### Phase 3: Auto-trading (guardrailed)
- Create orders via private endpoints
- Position sizing rules
- Kill-switch, max daily loss, max open positions
