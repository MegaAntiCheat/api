"""Report update timings

Revision ID: bc89a4c6d2b4
Revises: 82f4e558463f
Create Date: 2024-06-23 21:30:34.321859

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "bc89a4c6d2b4"
down_revision: Union[str, None] = "82f4e558463f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text("""
                ALTER TABLE reports
                ADD COLUMN updated_at timestamptz;

                UPDATE reports
                SET updated_at = created_at
                WHERE updated_at IS NULL;
                """)
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE reports
            DROP COLUMN updated_at;
            """
        )
    )
    pass
