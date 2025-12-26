"""candles_1m t index

Revision ID: 0002_candles_1m_t_index
Revises: 0001_init
Create Date: 2025-12-25

"""

from __future__ import annotations

from alembic import op

revision = "0002_candles_1m_t_index"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_candles_1m_t", "candles_1m", ["t"])


def downgrade() -> None:
    op.drop_index("ix_candles_1m_t", table_name="candles_1m")

