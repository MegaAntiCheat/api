import base64
import os
from datetime import datetime, timezone
from typing import BinaryIO, cast
from urllib.parse import urlencode
from uuid import uuid4

import requests
import sqlalchemy as sa
from litestar import Litestar, MediaType, Request, WebSocket, get, post, websocket_listener
from litestar.connection import ASGIConnection
from litestar.datastructures import State
from litestar.di import Provide
from litestar.exceptions import NotAuthorizedException
from litestar.handlers import WebsocketListener
from litestar.handlers.base import BaseRouteHandler
from litestar.response import Redirect
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from api.lib import generate_uuid4_int

DEMOS_PATH = os.path.expanduser(os.path.join("~/media", "demos"))
os.makedirs(DEMOS_PATH, exist_ok=True)


def _make_db_uri(async_url: bool = False) -> str:
    """Correctly make the database URi."""
    user = os.environ["PG_USER"]
    password = os.environ["PG_PASS"]
    prefix = "postgresql"
    if async_url:
        prefix = f"{prefix}+asyncpg"

    return f"{prefix}://{user}:{password}@localhost:5432/demos"


def get_db_connection(app: Litestar) -> Engine:
    """Returns the db engine.

    If it doesn't exist, creates it and saves it in on the application state object
    """
    if not getattr(app.state, "engine", None):
        app.state.engine = create_engine(_make_db_uri())
    return cast("Engine", app.state.engine)


def close_db_connection(app: Litestar) -> None:
    """Closes the db connection stored in the application State object."""
    if getattr(app.state, "engine", None):
        cast("Engine", app.state.engine).dispose()


def get_async_db_connection(app: Litestar) -> Engine:
    """Returns the async db engine.

    If it doesn't exist, creates it and saves it in on the application state object
    """
    if not getattr(app.state, "async_engine", None):
        app.state.async_engine = create_async_engine(_make_db_uri(async_url=True))
    return cast("AsyncEngine", app.state.async_engine)


async def close_async_db_connection(app: Litestar) -> None:
    """Closes the db connection stored in the application State object."""
    if getattr(app.state, "async_engine", None):
        await cast("AsyncEngine", app.state.async_engine).dispose()


async def _check_key_exists(engine: AsyncEngine, api_key: str) -> bool:
    """Helper util to determine key existence."""
    async with engine.connect() as conn:
        result = await conn.execute(sa.text("SELECT * FROM api_keys WHERE api_key = :api_key"), {"api_key": api_key})
        data = result.all()
        if not data:
            return False

        return True


async def _check_is_active(engine: AsyncEngine, api_key: str, session_id: str | None = None) -> bool:
    """Helper util to determine if a session is active."""

    sql = "SELECT * FROM demo_sessions WHERE api_key = :api_key and active = true;"
    params = {"api_key": api_key}

    if session_id is not None:
        sql = f"{sql.rstrip(';')} AND session_id = :session_id"
        params["session_id"] = session_id

    async with engine.connect() as conn:
        result = await conn.execute(
            sa.text(sql),
            params,
        )

        data = result.all()
        is_active = bool(data)

        return is_active


async def valid_key_guard(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    """A Guard clause to validate the user's API key."""
    api_key = connection.query_params["api_key"]

    async_engine = connection.app.state.async_engine
    exists = await _check_key_exists(async_engine, api_key)
    if not exists:
        raise NotAuthorizedException()


async def user_in_session_guard(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    """Assert that the user is not currently in a session."""
    async_engine = connection.app.state.async_engine

    api_key = connection.query_params["api_key"]
    is_active = await _check_is_active(async_engine, api_key)

    if is_active:
        raise NotAuthorizedException(
            detail="User is already in a session, either remember your session token or close it out at `/close_session`!"  # noqa
        )


async def user_not_in_session_guard(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    """Assert that the user is not currently in a session."""
    async_engine = connection.app.state.async_engine

    api_key = connection.query_params["api_key"]
    session_id = connection.query_params["session_id"]
    is_active = await _check_is_active(async_engine, api_key, session_id)
    if not is_active:
        raise NotAuthorizedException(
            detail="User is not in a session, create one at `/session_id`!"  # noqa
        )


@get("/session_id", guards=[valid_key_guard, user_in_session_guard], sync_to_thread=False)
def session_id(
    request: Request,
    api_key: str,
    fake_ip: str,
    map: str,
) -> dict[str, int]:
    """Return a session ID, as well as persist to database.

    This is to help us know what is happening downstream:
        - How many active upload sessions
        - If the upload request contains a valid session ID
        - Currently valid upload session ID's so client could reconnect

    Returns:
        {"session_id": some integer}
    """

    _session_id = generate_uuid4_int()
    engine = request.app.state.engine
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO demo_sessions (session_id, api_key, active, start_time, end_time, fake_ip, map, steam_api_data, ingested, created_at, updated_at) VALUES (:session_id, :api_key, :active, :start_time, :end_time, :fake_ip, :map, :steam_api_data, :ingested, :created_at, :updated_at);"  # noqa
            ),
            {
                "session_id": _session_id,
                "api_key": api_key,
                "active": True,
                "start_time": datetime.now().astimezone(timezone.utc).isoformat(),
                "end_time": None,
                "fake_ip": fake_ip,
                "map": map,
                "steam_api_data": None,
                "ingested": False,
                "created_at": datetime.now().astimezone(timezone.utc).isoformat(),
                "updated_at": datetime.now().astimezone(timezone.utc).isoformat(),
            },
        )
        conn.commit()
    return {"session_id": _session_id}


@get("/close_session", guards=[valid_key_guard, user_not_in_session_guard], sync_to_thread=False)
def close_session(request: Request, api_key: str, session_id: str) -> dict[str, bool]:
    """Close a session out.

    Returns:
        {"closed_successfully": True or False}
    """
    engine = request.app.state.engine
    current_time = datetime.now().astimezone(timezone.utc).isoformat()
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                "UPDATE demo_sessions SET active = False, end_time = :end_time, updated_at = :updated_at WHERE session_id = :session_id AND api_key = :api_key;"  # noqa
            ),
            {
                "api_key": api_key,
                "session_id": session_id,
                "end_time": current_time,
                "updated_at": current_time,
            },
        )
        conn.commit()

    return {"closed_successfully": True}


class DemoHandler(WebsocketListener):
    path = "/demos"
    receive_mode = "binary"

    async def on_accept(self, socket: WebSocket, api_key: str, session_id: str) -> None:
        engine = socket.app.state.async_engine
        exists = await _check_key_exists(engine, api_key)
        if not exists:
            await socket.close()

        active = await _check_is_active(engine, api_key, session_id)
        if not active:
            await socket.close()

        self.api_key = api_key
        self.session_id = session_id
        self.path = os.path.join(DEMOS_PATH, f"{session_id}.dem")
        self.handle = open(os.path.join(DEMOS_PATH, f"{session_id}.dem"), "wb")

    def on_disconnect(self, socket: WebSocket) -> None:
        self.handle.close()

        demo = open(self.path, "rb").read()

        current_time = datetime.now().astimezone(timezone.utc).isoformat()
        engine = socket.app.state.engine

        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "UPDATE demo_sessions SET active = False, end_time = :end_time, demo = :demo, updated_at = :updated_at WHERE session_id = :session_id AND api_key = :api_key;"  # noqa
                ),
                {
                    "api_key": self.api_key,
                    "session_id": self.session_id,
                    "end_time": current_time,
                    "updated_at": current_time,
                    "demo": demo,
                },
            )
            conn.commit()

    def on_receive(self, data: bytes) -> None:
        self.handle.write(data)


@get("/provision", sync_to_thread=False)
def provision(request: Request) -> Redirect:
    """Provision a login/API key.

    Mostly stolen from https://github.com/TeddiO/pySteamSignIn/blob/master/pysteamsignin/steamsignin.py

    Args:
        request: current request object

    Returns:
        Redirect to the steam sign in
    """
    auth_params = {
        "openid.ns": "http://specs.openid.net/auth/2.0",
        "openid.mode": "checkid_setup",
        "openid.return_to": f"{request.base_url}/provision_handler",
        "openid.realm": f"{request.base_url}/provision_handler",
        "openid.identity": "http://specs.openid.net/auth/2.0/identifier_select",
        "openid.claimed_id": "http://specs.openid.net/auth/2.0/identifier_select",
    }

    encoded = urlencode(auth_params)

    return Redirect(
        path=f"https://steamcommunity.com/openid/login?{encoded}",
        status_code=303,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )


@get("/provision_handler", media_type=MediaType.HTML, sync_to_thread=True)
def provision_handler(request: Request) -> str:
    """Handle a response from Steam.

    Mostly stolen from https://github.com/TeddiO/pySteamSignIn/blob/master/pysteamsignin/steamsignin.py

    Args:
        request: key value request params from the steam sign in to check against.

    Returns:
        Page of HTML for user.
    """
    data = request.query_params
    validation_args = {
        "openid.assoc_handle": data["openid.assoc_handle"],
        "openid.signed": data["openid.signed"],
        "openid.sig": data["openid.sig"],
        "openid.ns": data["openid.ns"],
    }

    signed_args = data["openid.signed"].split(",")
    for item in signed_args:
        arg = f"openid.{item}"
        if data[arg] not in validation_args:
            validation_args[arg] = data[arg]

    validation_args["openid.mode"] = "check_authentication"
    parsed_args = urlencode(validation_args).encode()

    response = requests.get("https://steamcommunity.com/openid/login", params=parsed_args)
    decoded = response.content.decode()
    _, valid_str, _ = decoded.split("\n")
    # valid_str looks like `is_valid:true`
    valid = bool(valid_str.split(":"))

    if not valid:
        text = "Could not log you in!"

    else:
        # great we have the steam id, now lets either provision a new key and display it to the user
        # if it is not in the DB or say that it already exists, and if they forgot it to let an admin know...
        # admin will then delete the steam ID of the user in the DB and a new sign in will work.
        steam_id = os.path.split(data["openid.claimed_id"])[-1]

        with request.app.state.engine.connect() as conn:
            result = conn.execute(
                sa.text("SELECT * FROM api_keys WHERE steam_id = :steam_id"), {"steam_id": steam_id}
            ).one_or_none()

            if result is None:
                api_key = uuid4().int
                created_at = datetime.now().astimezone(timezone.utc).isoformat()
                updated_at = created_at
                conn.execute(
                    sa.text(
                        "INSERT INTO api_keys (steam_id, api_key, created_at, updated_at) VALUES (:steam_id, :api_key, :created_at, :updated_at);"  # noqa
                    ),
                    {"steam_id": steam_id, "api_key": api_key, "created_at": created_at, "updated_at": updated_at},
                )
                conn.commit()  # commit changes...
                text = f"You have successfully been authenticated! Your API key is {api_key}! Do not lose this as the client needs it!"  # noqa

            else:
                text = f"Your steam id of {steam_id} already exists in our DB! If you forgot your API key, please let an admin know."  # noqa

    return f"""
        <html>
            <body>
                <div>
                    <span>{text}</span>
                </div>
            </body>
        </html>
        """


app = Litestar(
    on_startup=[get_db_connection, get_async_db_connection],
    route_handlers=[session_id, close_session, DemoHandler, provision, provision_handler],
    on_shutdown=[close_db_connection, close_async_db_connection],
)
