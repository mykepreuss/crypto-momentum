"""alert delivery metadata + peak high tracking

Revision ID: 0003_alert_delivery_peak_high
Revises: 0002_candles_1m_t_index
Create Date: 2025-12-27

NOTE: Alembic's default `alembic_version.version_num` column is VARCHAR(32).
Keep revision IDs <= 32 chars to avoid migration failures.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003_alert_delivery_peak_high"
down_revision = "0002_candles_1m_t_index"
branch_labels = None
depends_on = None


def _has_column(table: str, column: str) -> bool:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    return any(c.get("name") == column for c in insp.get_columns(table))


def upgrade() -> None:
    # Make this migration idempotent for local dev: if a previous attempt applied DDL but failed
    # during the alembic_version update, rerunning should succeed.

    if not _has_column("alerts", "delivery_attempts"):
        op.add_column(
            "alerts",
            sa.Column("delivery_attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
    if not _has_column("alerts", "last_delivery_attempt_ts"):
        op.add_column("alerts", sa.Column("last_delivery_attempt_ts", sa.BigInteger(), nullable=True))
    if not _has_column("alerts", "delivery_error"):
        op.add_column("alerts", sa.Column("delivery_error", sa.Text(), nullable=True))

    if not _has_column("signal_state", "peak_high_since_entry"):
        op.add_column("signal_state", sa.Column("peak_high_since_entry", sa.Numeric(), nullable=True))
    if not _has_column("signal_state", "peak_high_ts_since_entry"):
        op.add_column("signal_state", sa.Column("peak_high_ts_since_entry", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    if _has_column("signal_state", "peak_high_ts_since_entry"):
        op.drop_column("signal_state", "peak_high_ts_since_entry")
    if _has_column("signal_state", "peak_high_since_entry"):
        op.drop_column("signal_state", "peak_high_since_entry")

    if _has_column("alerts", "delivery_error"):
        op.drop_column("alerts", "delivery_error")
    if _has_column("alerts", "last_delivery_attempt_ts"):
        op.drop_column("alerts", "last_delivery_attempt_ts")
    if _has_column("alerts", "delivery_attempts"):
        op.drop_column("alerts", "delivery_attempts")

