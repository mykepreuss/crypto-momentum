from __future__ import annotations

from typing import Optional

from pydantic import HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "dev"
    log_level: str = "INFO"

    # Exchange
    cryptocom_exchange_base_url: HttpUrl = "https://api.crypto.com/exchange/v1"
    quote_ccy: str = "USDT"
    max_universe_size: int = 200
    max_concurrent_requests: int = 25
    http_timeout_s: float = 10.0

    # Alerting budgets (rolling window)
    alert_lookback_hours: int = 24
    max_entry_alerts_24h: int = 5
    max_exit_alerts_24h: int = 5
    max_total_alerts_24h: int = 10

    # Candles / ingestion
    candle_timeframe: str = "1m"
    candle_backfill_count: int = 180
    candle_poll_count: int = 2
    candle_fetch_delay_s: float = 2.0
    candle_buffer_size: int = 300
    candle_retention_days: int = 30
    candle_prune_interval_hours: int = 24
    db_upsert_batch_size: int = 5_000

    # Signal engine thresholds (v1 defaults from SPEC)
    dvz_min: float = 1.5
    extension_max: float = 0.08
    min_dv_1m_usd: float = 10_000.0
    spread_max: float = 0.005
    book_check_top_n: int = 20
    signals_return_limit: int = 50

    # State machine thresholds + cooldowns
    entry_score_threshold: float = 0.80
    exit_score_threshold: float = 0.55
    stall_minutes: int = 10
    stall_dvz_max: float = 1.0
    global_entry_cooldown_min: int = 10
    symbol_entry_cooldown_min: int = 90
    symbol_exit_cooldown_min: int = 30
    max_entry_alerts_per_scan: int = 1

    # Slack notifier
    slack_webhook_url: Optional[HttpUrl] = None
    slack_channel_name: Optional[str] = None

    # Admin (optional). If set, requires X-Admin-Token for mutating endpoints (e.g. POST /config).
    admin_token: Optional[str] = None

    # Database
    # Prefer 127.0.0.1 over localhost to avoid IPv6 (::1) resolving to a different local Postgres.
    # Local docker-compose maps container 5432 -> host 5433 by default.
    database_url: str = "postgresql+asyncpg://postgres:postgres@127.0.0.1:5433/crypto_momentum"

    @field_validator("slack_webhook_url", "slack_channel_name", "admin_token", mode="before")
    @classmethod
    def _empty_str_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v


def get_settings() -> Settings:
    return Settings()
