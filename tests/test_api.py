"""Integration tests."""

import csv
import io
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import pytest
import sqlalchemy as sa
from httpx import Response
from litestar import Litestar
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_403_FORBIDDEN
from litestar.testing import TestClient

from masterbase.app import app
from masterbase.lib import LATE_BYTES_END, LATE_BYTES_START, add_report
from masterbase.models import ReportReason

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def steam_id() -> str:
    """Return steam ID fixture."""
    return "76561111111111111"


@pytest.fixture(scope="session")
def api_key() -> str:
    """Return API key ID fixture."""
    return "my-api-key"


def _open_mock_session(test_client: TestClient[Litestar], api_key: str) -> Response:
    return test_client.get(
        "/session_id",
        params={"api_key": api_key, "fake_ip": "169.254.215.11%3A58480", "map": "some_map", "demo_name": "demo.dem"},
    )


@pytest.fixture(scope="session")
def test_client(steam_id: str, api_key: str) -> Iterator[TestClient[Litestar]]:
    """Yield a test client for testing."""
    with TestClient(app=app) as client:
        with app.state.engine.connect() as conn:
            sql = "INSERT INTO api_keys VALUES (:steam_id, :api_key, NOW(), NOW());"
            conn.execute(sa.text(sql), {"steam_id": steam_id, "api_key": api_key})
            sql = "INSERT INTO analyst_steam_ids VALUES (:steam_id);"
            conn.execute(sa.text(sql), {"steam_id": steam_id})
            conn.commit()
        yield client

        with app.state.engine.connect() as conn:
            sql = "TRUNCATE TABLE reports CASCADE;"
            conn.execute(sa.text(sql))
            sql = "TRUNCATE TABLE demo_sessions CASCADE;"
            conn.execute(sa.text(sql))
            sql = "TRUNCATE TABLE api_keys CASCADE;"
            conn.execute(sa.text(sql), {"api_key": api_key})
            sql = "TRUNCATE TABLE analyst_steam_ids CASCADE;"
            conn.execute(sa.text(sql), {"steam_id": steam_id})
            conn.commit()


def test_report_reasons_match(test_client: TestClient[Litestar]) -> None:
    """Ensure that Pydantic report reasons match the postgres type registry."""
    with test_client.app.state.engine.connect() as conn:
        cursor = conn.execute(
            sa.text(
                """
            SELECT enumlabel
            FROM pg_enum
            JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
            WHERE pg_type.typname = 'report_reason';
            """
            )
        )
    db_reasons = next(zip(*cursor))
    pd_reasons = tuple(variant.value for variant in ReportReason)
    assert db_reasons == pd_reasons, (
        "Database and Pydantic: ORM mismatch!"
        + f"\n\tDatabase accepts {db_reasons}."
        + f"\n\tPydantic accepts {pd_reasons}."
    )


def test_close_session_no_session(test_client: TestClient[Litestar], api_key: str) -> None:
    """Test closing a session yields a 403."""
    response = test_client.get("/close_session", params={"api_key": api_key})
    assert response.status_code == HTTP_403_FORBIDDEN


def _send_demo_file(test_client: TestClient[Litestar], api_key: str, session_id: str):
    with test_client.websocket_connect("/demos", params={"api_key": api_key, "session_id": session_id}) as socket:
        time.sleep(5)
        with open("tests/data/test_demo.dem", "rb") as f:
            while True:
                chunk = f.read(720896)
                if not chunk:
                    break
                socket.send_bytes(chunk)
                time.sleep(1)
            socket.close()


def test_demo_streaming(test_client: TestClient[Litestar], api_key: str) -> None:
    """Test streaming a demo to the database with a header overwrite."""
    session_id = _open_mock_session(test_client, api_key).json()["session_id"]
    assert isinstance(session_id, int)

    _send_demo_file(test_client, api_key, str(session_id))

    late_bytes_hex = "7031cf44a7af0100cea70100f5e00400"
    late_bytes_response = test_client.post(
        "/late_bytes", params={"api_key": api_key}, json={"late_bytes": late_bytes_hex}
    )  # noqa
    assert late_bytes_response.status_code == HTTP_201_CREATED

    close_session_response = test_client.get("/close_session", params={"api_key": api_key})
    assert close_session_response.status_code == HTTP_200_OK

    with test_client.stream("GET", "/demodata", params={"api_key": api_key, "session_id": session_id}) as demo_stream:
        demo_out = demo_stream.read()

    with open("tests/data/test_demo.dem", "rb") as f:
        demo_in = f.read()
        demo_in = demo_in[:LATE_BYTES_START] + bytes.fromhex(late_bytes_hex) + demo_in[LATE_BYTES_END:]
        assert demo_in == demo_out


def test_demo_streaming_no_late(test_client: TestClient[Litestar], api_key: str) -> None:
    """Test streaming a demo to the database without a header overwrite."""
    session_id = _open_mock_session(test_client, api_key).json()["session_id"]
    assert isinstance(session_id, int)

    _send_demo_file(test_client, api_key, str(session_id))

    close_session_response = test_client.get("/close_session", params={"api_key": api_key})
    assert close_session_response.status_code == HTTP_200_OK

    with test_client.stream("GET", "/demodata", params={"api_key": api_key, "session_id": session_id}) as demo_stream:
        demo_out = demo_stream.read()

    with open("tests/data/test_demo.dem", "rb") as f:
        demo_in = f.read()
        assert demo_in == demo_out


def test_db_exports(test_client: TestClient[Litestar], api_key: str) -> None:
    """Test on-demand exports from the database."""

    def _parse_reports(body):
        records = csv.DictReader(io.TextIOWrapper(io.BytesIO(body), encoding="utf8"))
        fields = records.fieldnames
        assert fields is not None
        assert set(fields) == {"session_id", "target_steam_id", "reason", "created_at"}
        return tuple(sorted(records, key=lambda r: r["created_at"]))

    session_id = str(_open_mock_session(test_client, api_key).json()["session_id"])
    # Insert mock reports
    expected = []
    for i in range(10):
        reason = "cheater" if i % 2 == 0 else "bot"
        target_steam_id = f"{i:020d}"
        record = {"session_id": session_id, "target_steam_id": target_steam_id, "reason": reason}
        if i == 4:
            time.sleep(1.0)  # postgres timestamp comparisons are second-precison
        add_report(test_client.app.state.engine, **record)
        expected.append(record)

    test_client.get("/close_session", params={"api_key": api_key})
    response = test_client.get("/db_export", params={"api_key": api_key, "table": "reports"})
    returned_full = _parse_reports(response.content)
    assert tuple(expected) == tuple({k: v for k, v in r.items() if k != "created_at"} for r in returned_full)
    since = returned_full[4]["created_at"]

    tzone = timezone(timedelta(hours=int(since[-3:])))
    stamp = datetime.strptime(since[:-3], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=tzone)
    response = test_client.get(
        "/db_export", params={"api_key": api_key, "table": "reports", "since": stamp.isoformat()}
    )
    returned_since = _parse_reports(response.content)
    assert returned_full[4:] == returned_since


def test_upsert_report_reason(test_client: TestClient[Litestar], api_key: str) -> None:
    """Ensure that upserts of reports during the same session work as intended."""
    session_id = str(_open_mock_session(test_client, api_key).json()["session_id"])
    engine = test_client.app.state.engine
    target_sid = f"{0:020d}"
    add_report(engine, session_id, target_sid, reason="bot")
    add_report(engine, session_id, target_sid, reason="cheater")
    with engine.connect() as txn:
        results = txn.execute(
            sa.text("""SELECT reason FROM reports
                    WHERE session_id = :session_id
                    AND target_steam_id = :target_sid"""),
            {"session_id": session_id, "target_sid": target_sid},
        )
        assert results.scalar_one() == "cheater"
