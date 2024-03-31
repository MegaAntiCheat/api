import os
from datetime import datetime, timezone
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

DEMOS_PATH = os.path.expanduser(os.path.join("~/media", "demos"))
os.makedirs(DEMOS_PATH, exist_ok=True)


def _make_db_uri(async_url: bool = False) -> str:
    """Correctly make the database URi."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "8050")
    prefix = "postgresql"
    if async_url:
        prefix = f"{prefix}+asyncpg"

    return f"{prefix}://{user}:{password}@{host}:{port}/demos"


def _make_demo_path(session_id: str) -> os.path:
    """Make the demo path for the current session."""
    return os.path.join(DEMOS_PATH, f"{session_id}.dem")


def generate_uuid4_int() -> int:
    """Seems useless, but makes testing easier."""
    return uuid4().int


async def _check_key_exists(engine: AsyncEngine, api_key: str) -> bool:
    """Helper util to determine key existence."""
    async with engine.connect() as conn:
        result = await conn.execute(sa.text("SELECT * FROM api_keys WHERE api_key = :api_key"), {"api_key": api_key})
        data = result.all()
        if not data:
            return False

        return True


async def _check_is_active(engine: AsyncEngine, api_key: str) -> bool:
    """Helper util to determine if a session is active."""
    sql = "SELECT * FROM demo_sessions WHERE api_key = :api_key and active = true;"
    params = {"api_key": api_key}

    async with engine.connect() as conn:
        result = await conn.execute(
            sa.text(sql),
            params,
        )

        data = result.all()
        is_active = bool(data)

        return is_active


def _start_session(engine: Engine, api_key: str, session_id: str, fake_ip: str, map_str: str) -> None:
    """Start a session and persist to DB."""
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                """INSERT INTO demo_sessions (
                    session_id,
                    api_key,
                    active,
                    start_time,
                    end_time,
                    fake_ip,
                    map,
                    steam_api_data,
                    ingested,
                    created_at,
                    updated_at
                ) VALUES (
                    :session_id,
                    :api_key,
                    :active,
                    :start_time,
                    :end_time,
                    :fake_ip,
                    :map,
                    :steam_api_data,
                    :ingested,
                    :created_at,
                    :updated_at
                );
                """
            ),
            {
                "session_id": session_id,
                "api_key": api_key,
                "active": True,
                "start_time": datetime.now().astimezone(timezone.utc).isoformat(),
                "end_time": None,
                "fake_ip": fake_ip,
                "map": map_str,
                "steam_api_data": None,
                "ingested": False,
                "created_at": datetime.now().astimezone(timezone.utc).isoformat(),
                "updated_at": datetime.now().astimezone(timezone.utc).isoformat(),
            },
        )
        conn.commit()


def _close_session(engine: Engine, api_key: str, current_time: datetime) -> None:
    """Close out a session in the DB."""
    # TODO GET THE UNCLOSED SESSION FOR THIS USER AND USE _make_demo_path TO ADD THE DEMO!!!
    with engine.connect() as conn:
        latest_session_id = conn.execute(
            sa.text(
                "SELECT session_id FROM demo_sessions WHERE start_time = (SELECT MAX(start_time) FROM demo_sessions where api_key = :api_key);"  # noqa
            ),
            {"api_key": api_key},
        ).scalar_one_or_none()

    if latest_session_id:
        demo_path = _make_demo_path(latest_session_id)
        _close_session_with_demo(engine, api_key, latest_session_id, current_time, demo_path)
    else:
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    """UPDATE demo_sessions
                    SET
                    active = False,
                    end_time = :end_time,
                    updated_at = :updated_at
                    WHERE
                    active = True AND
                    api_key = :api_key;"""
                ),
                {
                    "api_key": api_key,
                    "end_time": current_time.isoformat(),
                    "updated_at": current_time.isoformat(),
                },
            )
            conn.commit()


def _close_session_with_demo(
    engine: Engine, api_key: str, session_id: str, current_time: datetime, demo_path: str
) -> None:
    """Close out a session in the DB."""
    with engine.connect() as conn:
        oid = conn.connection.lobject(mode="w", new_file=demo_path).oid
        conn.execute(
            sa.text(
                """UPDATE demo_sessions
                SET
                active = False,
                end_time = :end_time,
                demo_oid = :demo_oid,
                updated_at = :updated_at
                WHERE
                api_key = :api_key AND
                session_id = :session_id;"""
            ),
            {
                "api_key": api_key,
                "session_id": session_id,
                "end_time": current_time.isoformat(),
                "updated_at": current_time.isoformat(),
                "demo_oid": oid,
            },
        )
        conn.commit()


def _late_bytes(engine: Engine, api_key: str, late_bytes: bytes, current_time: datetime) -> None:
    """Add late bytes to the DB."""
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                """UPDATE demo_sessions
                SET
                late_bytes = :late_bytes
                updated_at = :updated_at
                WHERE
                api_key = :api_key
                AND updated_at = (
                    SELECT MAX(updated_at) FROM demo_sessions WHERE api_key = :api_key
                );"""
            ),
            {
                "api_key": api_key,
                "late_bytes": late_bytes,
                "updated_at": current_time.isoformat(),
            },
        )
        conn.commit()


def check_steam_id_has_api_key(engine: Engine, steam_id: str) -> bool:
    """Check that a given steam id has an API key or not."""
    with engine.connect() as conn:
        result = conn.execute(
            sa.text("SELECT * FROM api_keys WHERE steam_id = :steam_id"), {"steam_id": steam_id}
        ).one_or_none()

        return bool(result)


def provision_api_key(engine: Engine, steam_id: str, api_key: str) -> None:
    """Provision an API key."""
    with engine.connect() as conn:
        created_at = datetime.now().astimezone(timezone.utc).isoformat()
        updated_at = created_at
        conn.execute(
            sa.text(
                """INSERT INTO api_keys (
                    steam_id, api_key, created_at, updated_at
                    ) VALUES (
                        :steam_id, :api_key, :created_at, :updated_at);"""
            ),
            {"steam_id": steam_id, "api_key": api_key, "created_at": created_at, "updated_at": updated_at},
        )
        conn.commit()
