"""initialize

Revision ID: 58fb39990d30
Revises: 
Create Date: 2023-12-30 16:24:24.579092

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '58fb39990d30'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE api_keys (
            steam_id varchar PRIMARY KEY,
            api_key varchar,
            created_at timestamptz,
            updated_at timestamptz
        );
        """
    )

    op.execute(
        """
        CREATE TABLE demo_sessions (
            session_id varchar PRIMARY KEY,
            api_key varchar,
            demo_name varchar,
            active boolean,
            start_time timestamptz,
            end_time timestamptz,
            fake_ip varchar,
            map varchar,
            steam_api_data jsonb,
            ingested boolean,
            demo_oid oid,
            late_bytes bytea,
            created_at timestamptz,
            updated_at timestamptz
        );
        """
    )

    op.execute(
        """
            ALTER TABLE demo_sessions
            ADD CONSTRAINT fk_api_key
            FOREIGN KEY (api_key)
            REFERENCES api_keys(api_key)
            ON UPDATE CASCADE
            ;
        """
    )

    op.execute(
        """
        CREATE TABLE beta_tester_steam_ids (
            steam_id varchar PRIMARY KEY
        );
        """
    )

def downgrade() -> None:
    op.execute(
        """
        DROP TABLE api_keys;
        DROP TABLE demo_sessions;
        """
    )
