"""merge canonical-generation and signed-usage-checkpoint heads

Revision ID: 20260821_0005
Revises: 20260806_0004, 20260818_0004
Create Date: 2026-08-21 00:00:00.000000
"""

from __future__ import annotations

revision: str = "20260821_0005"
down_revision: tuple[str, str] = ("20260806_0004", "20260818_0004")
branch_labels: str | tuple[str, ...] | None = None
depends_on: str | tuple[str, ...] | None = None


def upgrade() -> None:
    """Join the two independently verified schema branches."""


def downgrade() -> None:
    """Split the merge head without changing either parent schema."""
