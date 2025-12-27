from __future__ import annotations

import uuid
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = sa.MetaData(
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


_JSON = sa.JSON().with_variant(postgresql.JSONB, "postgresql")


class Instrument(Base):
    __tablename__ = "instruments"

    symbol: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    inst_type: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    base_ccy: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    quote_ccy: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    tradable: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)
    display_name: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    last_seen_ts: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)


class Candle1m(Base):
    __tablename__ = "candles_1m"

    symbol: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    t: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)  # candle start (ms)
    o: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)
    h: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)
    l: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)
    c: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)
    v: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)

    __table_args__ = (sa.Index("ix_candles_1m_t", "t"),)


class Ticker24h(Base):
    __tablename__ = "ticker_24h"

    symbol: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    last_price: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)
    vol_24h_base: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)
    updated_ts: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)


class UniverseMembership(Base):
    __tablename__ = "universe_membership"

    symbol: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    quote_ccy: Mapped[str] = mapped_column(sa.Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=True)
    is_baseline: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)
    liquidity_rank: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    dollar_vol_24h: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)
    updated_ts: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)

    __table_args__ = (
        sa.Index("ix_universe_membership_quote_ccy", "quote_ccy"),
        sa.Index("ix_universe_membership_is_active", "is_active"),
    )


class SignalState(Base):
    __tablename__ = "signal_state"

    symbol: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    state: Mapped[str] = mapped_column(sa.Text, nullable=False, default="OUT")  # OUT|IN
    last_state_change_ts: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    last_entry_alert_ts: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    last_exit_alert_ts: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    peak_price_since_entry: Mapped[Optional[sa.Numeric]] = mapped_column(sa.Numeric, nullable=True)
    peak_ts_since_entry: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    peak_high_since_entry: Mapped[Optional[sa.Numeric]] = mapped_column(sa.Numeric, nullable=True)
    peak_high_ts_since_entry: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[str] = mapped_column(
        sa.String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )
    ts: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(sa.Text, nullable=False, index=True)
    alert_type: Mapped[str] = mapped_column(sa.Text, nullable=False)  # MOMENTUM_UP|MOMENTUM_SLOWING
    score: Mapped[sa.Numeric] = mapped_column(sa.Numeric, nullable=False)
    features_json: Mapped[dict] = mapped_column(_JSON, nullable=False, default=dict)
    message: Mapped[str] = mapped_column(sa.Text, nullable=False)
    delivered: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)
    delivery_channel: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    delivery_attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0)
    last_delivery_attempt_ts: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    delivery_error: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)


class AlertEvaluation(Base):
    __tablename__ = "alert_evaluation"

    alert_id: Mapped[str] = mapped_column(sa.String(36), primary_key=True)
    r_5m: Mapped[Optional[sa.Numeric]] = mapped_column(sa.Numeric, nullable=True)
    r_15m: Mapped[Optional[sa.Numeric]] = mapped_column(sa.Numeric, nullable=True)
    r_60m: Mapped[Optional[sa.Numeric]] = mapped_column(sa.Numeric, nullable=True)
    mae_60m: Mapped[Optional[sa.Numeric]] = mapped_column(sa.Numeric, nullable=True)
    mfe_60m: Mapped[Optional[sa.Numeric]] = mapped_column(sa.Numeric, nullable=True)
    computed_ts: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)

    __table_args__ = (
        sa.ForeignKeyConstraint(["alert_id"], ["alerts.id"], ondelete="CASCADE"),
    )


class ConfigKV(Base):
    __tablename__ = "config_kv"

    key: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    value_json: Mapped[dict] = mapped_column(_JSON, nullable=False, default=dict)
    updated_ts: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
