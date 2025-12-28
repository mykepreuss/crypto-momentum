"""candles_hist_bars table for offline bar replays

Revision ID: 0005_candles_hist_bars
Revises: 0004_candles_1m_hist
Create Date: 2025-12-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0005_candles_hist_bars"
down_revision = "0004_candles_1m_hist"
branch_labels = None
depends_on = None


def _has_table(table: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    return bool(insp.has_table(table))


def upgrade() -> None:
    if _has_table("candles_hist_bars"):
        return

    op.create_table(
        "candles_hist_bars",
        sa.Column("timeframe_min", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("t", sa.BigInteger(), nullable=False),
        sa.Column("o", sa.Numeric(), nullable=False),
        sa.Column("h", sa.Numeric(), nullable=False),
        sa.Column("l", sa.Numeric(), nullable=False),
        sa.Column("c", sa.Numeric(), nullable=False),
        sa.Column("v", sa.Numeric(), nullable=False),
        sa.PrimaryKeyConstraint("timeframe_min", "symbol", "t", name=op.f("pk_candles_hist_bars")),
    )
    op.create_index(
        "ix_candles_hist_bars_timeframe_t",
        "candles_hist_bars",
        ["timeframe_min", "t"],
        unique=False,
    )
    op.create_index(
        "ix_candles_hist_bars_symbol_timeframe_t",
        "candles_hist_bars",
        ["symbol", "timeframe_min", "t"],
        unique=False,
    )


def downgrade() -> None:
    if not _has_table("candles_hist_bars"):
        return
    op.drop_index("ix_candles_hist_bars_symbol_timeframe_t", table_name="candles_hist_bars")
    op.drop_index("ix_candles_hist_bars_timeframe_t", table_name="candles_hist_bars")
    op.drop_table("candles_hist_bars")

