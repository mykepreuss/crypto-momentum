from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ConfigPatch(BaseModel):
    # Alert budgets / window
    alert_lookback_hours: Optional[int] = Field(None, ge=1, le=168)
    exit_entry_lookback_hours: Optional[int] = Field(None, ge=1, le=168)
    max_entry_alerts_24h: Optional[int] = Field(None, ge=0, le=50)
    max_exit_alerts_24h: Optional[int] = Field(None, ge=0, le=50)
    max_total_alerts_24h: Optional[int] = Field(None, ge=0, le=200)

    # Signal thresholds
    dvz_min: Optional[float] = Field(None, ge=-50.0, le=50.0)
    extension_max: Optional[float] = Field(None, ge=0.0, le=1.0)
    min_dv_1m_usd: Optional[float] = Field(None, ge=0.0)
    spread_max: Optional[float] = Field(None, ge=0.0, le=1.0)
    book_check_top_n: Optional[int] = Field(None, ge=0, le=500)
    signals_return_limit: Optional[int] = Field(None, ge=1, le=500)

    # Universe filters
    universe_exclude_base_ccy: Optional[str] = None
    universe_exclude_symbols: Optional[str] = None

    # Research gates
    require_btc_trend_ok_for_entries: Optional[bool] = None
    min_rank_rel_r15: Optional[float] = Field(None, ge=0.0, le=1.0)

    # State machine thresholds + cooldowns
    entry_score_threshold: Optional[float] = Field(None, ge=0.0, le=2.0)
    exit_score_threshold: Optional[float] = Field(None, ge=0.0, le=2.0)
    exit_mode: Optional[str] = None
    trailing_stop_pct: Optional[float] = Field(None, ge=0.0, le=1.0)
    stall_minutes: Optional[int] = Field(None, ge=1, le=240)
    stall_dvz_max: Optional[float] = Field(None, ge=-50.0, le=50.0)
    global_entry_cooldown_min: Optional[int] = Field(None, ge=0, le=10_000)
    symbol_entry_cooldown_min: Optional[int] = Field(None, ge=0, le=10_000)
    symbol_exit_cooldown_min: Optional[int] = Field(None, ge=0, le=10_000)
    max_entry_alerts_per_scan: Optional[int] = Field(None, ge=0, le=20)
