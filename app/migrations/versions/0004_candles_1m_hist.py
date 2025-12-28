"""candles_1m_hist table for long lookbacks

Revision ID: 0004_candles_1m_hist
Revises: 0003_alert_delivery_peak_high
Create Date: 2025-12-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_candles_1m_hist"
down_revision = "0003_alert_delivery_peak_high"
branch_labels = None
depends_on = None


def _has_table(table: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    return bool(insp.has_table(table))


def upgrade() -> None:
    if _has_table("candles_1m_hist"):
        return

    op.create_table(
        "candles_1m_hist",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("t", sa.BigInteger(), nullable=False),
        sa.Column("o", sa.Numeric(), nullable=False),
        sa.Column("h", sa.Numeric(), nullable=False),
        sa.Column("l", sa.Numeric(), nullable=False),
        sa.Column("c", sa.Numeric(), nullable=False),
        sa.Column("v", sa.Numeric(), nullable=False),
        sa.PrimaryKeyConstraint("symbol", "t", name=op.f("pk_candles_1m_hist")),
    )
    op.create_index(op.f("ix_candles_1m_hist_t"), "candles_1m_hist", ["t"], unique=False)


def downgrade() -> None:
    if not _has_table("candles_1m_hist"):
        return
    op.drop_index(op.f("ix_candles_1m_hist_t"), table_name="candles_1m_hist")
    op.drop_table("candles_1m_hist")

