import base64
import os
from datetime import datetime, timezone
from typing import BinaryIO, cast
from urllib.parse import urlencode
from uuid import uuid4

import requests
import sqlalchemy as sa
from litestar import Litestar, MediaType, Request, get, websocket, websocket_listener
from litestar.connection import ASGIConnection
from litestar.datastructures import State
from litestar.di import Provide
from litestar.exceptions import NotAuthorizedException
from litestar.handlers.base import BaseRouteHandler
from litestar.response import Redirect
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


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


async def valid_key_guard(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    """A Guard clause to validate the user's API key."""
    api_key = connection.query_params["api_key"]

    async_engine = connection.app.state.async_engine
    async with async_engine.connect() as conn:
        result = await conn.execute(sa.text("SELECT * FROM api_keys WHERE api_key = :api_key"), {"api_key": api_key})
        data = result.all()
        if not data:
            raise NotAuthorizedException()


class DemoSessionManager:
    """Helper class to manage incoming data streams."""

    DEMOS_PATH = os.path.expanduser(os.path.join("~/media", "demos"))
    SENTINEL = -1

    def __init__(self) -> None:
        self.file_handles: dict[str, BinaryIO] = {}
        os.makedirs(self.DEMOS_PATH, exist_ok=True)

    def make_or_get_file_handle(self, session_id: int) -> BinaryIO:
        """Take in a session ID and create or return a file handle.

        Args:
            session_id: Session ID.

        Returns:
            File handle for the session_id
        """
        if session_id not in self.file_handles:
            write_path = os.path.join(self.DEMOS_PATH, f"{session_id}.dem")
            self.file_handles[session_id] = open(write_path, "wb")

        return self.file_handles[session_id]

    def handle_demo_data(self, data: dict[str, str | bytes | int]) -> None:
        """Handle incoming data from a client upload.

        Args:
            data: dict of {session_id: ..., data: bytes or self.SENTINEL}
        """
        session_id = data["session_id"]

        file_handle = self.make_or_get_file_handle(session_id)

        _data = base64.b64decode(data["data"])

        if _data == self.SENTINEL:
            file_handle.close()
        else:
            file_handle.write(_data)
            file_handle.flush()

        def close(self, session_id: int) -> None:
            self.file_handles[session_id].close()


demo_manager = DemoSessionManager()


def generate_uuid4_int() -> int:
    """Seems useless, but makes testing easier."""
    return uuid4().int


@get("/session_id", guards=[valid_key_guard], sync_to_thread=False)
def session_id(api_key: str) -> dict[str, int]:
    """Return a session ID, as well as persist to database.

    This is to help us know what is happening downstream:
        - How many active upload sessions
        - If the upload request contains a valid session ID
        - Currently valid upload session ID's so client could reconnect

    Returns:
        {"session_id": some integer}
    """
    session_id = generate_uuid4_int()
    return {"session_id": session_id}


@get("/session_id_active", guards=[valid_key_guard], sync_to_thread=False)
def session_id_active(api_key: str, session_id: int) -> dict[str, bool]:
    """Tell if a session ID is active by querying the database.

    Returns:
        {"is_active": True or False}
    """
    is_active = ...

    return {"is_active": is_active}


@get("/close_session", guards=[valid_key_guard], sync_to_thread=False)
def close_session(api_key: str, session_id: int) -> dict[str, bool]:
    """Close a session out.

    Returns:
        {"closed_successfully": True or False}
    """
    try:
        demo_manager.close(session_id)
    except Exception:
        ...

    return {"closed_successfully": ...}


@websocket_listener("/demos")
async def demo_session(data: dict[str, str | bytes | int]) -> dict[str, str]:
    """Handle incoming data from a client upload.

    Smart enough to know where to write data to based on the session ID
    as to handle reconnecting/duplicates.

    Might want to implement something like
    https://docs.litestar.dev/2/usage/websockets.html#class-based-websocket-handling
    because it looks cleaner and likely can handle auth/accepting/valid session id better
    """
    demo_manager.handle_demo_data(data)


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


@get("/provision_handler", media_type=MediaType.HTML)
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
    route_handlers=[session_id, session_id_active, close_session, demo_session, provision, provision_handler],
    on_shutdown=[close_db_connection, close_async_db_connection],
)
