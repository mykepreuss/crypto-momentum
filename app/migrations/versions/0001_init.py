"""init

Revision ID: 0001_init
Revises:
Create Date: 2025-12-25

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None

_JSON = sa.JSON().with_variant(postgresql.JSONB, "postgresql")


def upgrade() -> None:
    op.create_table(
        "instruments",
        sa.Column("symbol", sa.Text(), primary_key=True),
        sa.Column("inst_type", sa.Text(), nullable=True),
        sa.Column("base_ccy", sa.Text(), nullable=True),
        sa.Column("quote_ccy", sa.Text(), nullable=True),
        sa.Column("tradable", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("last_seen_ts", sa.BigInteger(), nullable=False),
    )

    op.create_table(
        "ticker_24h",
        sa.Column("symbol", sa.Text(), primary_key=True),
        sa.Column("last_price", sa.Numeric(), nullable=False),
        sa.Column("vol_24h_base", sa.Numeric(), nullable=False),
        sa.Column("updated_ts", sa.BigInteger(), nullable=False),
    )

    op.create_table(
        "candles_1m",
        sa.Column("symbol", sa.Text(), primary_key=True, nullable=False),
        sa.Column("t", sa.BigInteger(), primary_key=True, nullable=False),
        sa.Column("o", sa.Numeric(), nullable=False),
        sa.Column("h", sa.Numeric(), nullable=False),
        sa.Column("l", sa.Numeric(), nullable=False),
        sa.Column("c", sa.Numeric(), nullable=False),
        sa.Column("v", sa.Numeric(), nullable=False),
    )

    op.create_table(
        "universe_membership",
        sa.Column("symbol", sa.Text(), primary_key=True),
        sa.Column("quote_ccy", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("is_baseline", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("liquidity_rank", sa.Integer(), nullable=False),
        sa.Column("dollar_vol_24h", sa.Numeric(), nullable=False),
        sa.Column("updated_ts", sa.BigInteger(), nullable=False),
    )
    op.create_index("ix_universe_membership_quote_ccy", "universe_membership", ["quote_ccy"])
    op.create_index("ix_universe_membership_is_active", "universe_membership", ["is_active"])

    op.create_table(
        "signal_state",
        sa.Column("symbol", sa.Text(), primary_key=True),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'OUT'")),
        sa.Column("last_state_change_ts", sa.BigInteger(), nullable=False),
        sa.Column("last_entry_alert_ts", sa.BigInteger(), nullable=True),
        sa.Column("last_exit_alert_ts", sa.BigInteger(), nullable=True),
        sa.Column("peak_price_since_entry", sa.Numeric(), nullable=True),
        sa.Column("peak_ts_since_entry", sa.BigInteger(), nullable=True),
    )

    op.create_table(
        "alerts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("ts", sa.BigInteger(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("alert_type", sa.Text(), nullable=False),
        sa.Column("score", sa.Numeric(), nullable=False),
        sa.Column("features_json", _JSON, nullable=False, server_default=sa.text("'{}'")),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("delivered", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("delivery_channel", sa.Text(), nullable=True),
    )
    op.create_index("ix_alerts_ts", "alerts", ["ts"])
    op.create_index("ix_alerts_symbol", "alerts", ["symbol"])

    op.create_table(
        "alert_evaluation",
        sa.Column("alert_id", sa.String(length=36), primary_key=True),
        sa.Column("r_5m", sa.Numeric(), nullable=True),
        sa.Column("r_15m", sa.Numeric(), nullable=True),
        sa.Column("r_60m", sa.Numeric(), nullable=True),
        sa.Column("mae_60m", sa.Numeric(), nullable=True),
        sa.Column("mfe_60m", sa.Numeric(), nullable=True),
        sa.Column("computed_ts", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(["alert_id"], ["alerts.id"], ondelete="CASCADE"),
    )

    op.create_table(
        "config_kv",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("value_json", _JSON, nullable=False, server_default=sa.text("'{}'")),
        sa.Column("updated_ts", sa.BigInteger(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("config_kv")
    op.drop_table("alert_evaluation")
    op.drop_index("ix_alerts_symbol", table_name="alerts")
    op.drop_index("ix_alerts_ts", table_name="alerts")
    op.drop_table("alerts")
    op.drop_table("signal_state")
    op.drop_index("ix_universe_membership_is_active", table_name="universe_membership")
    op.drop_index("ix_universe_membership_quote_ccy", table_name="universe_membership")
    op.drop_table("universe_membership")
    op.drop_table("candles_1m")
    op.drop_table("ticker_24h")
    op.drop_table("instruments")

