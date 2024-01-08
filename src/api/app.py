import base64
import os
from datetime import datetime, timezone
from typing import BinaryIO, cast
from urllib.parse import urlencode
from uuid import uuid4

import requests
import sqlalchemy as sa
from api.lib import (
    _check_is_active,
    _check_key_exists,
    _close_session,
    _close_session_with_demo,
    _make_db_uri,
    _start_session,
    check_steam_id_has_api_key,
    generate_uuid4_int,
    provision_api_key,
)
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

DEMOS_PATH = os.path.expanduser(os.path.join("~/media", "demos"))
os.makedirs(DEMOS_PATH, exist_ok=True)


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
            detail="User already in a session, either remember your session token or close it out at `/close_session`!"
        )


async def user_not_in_session_guard(connection: ASGIConnection, _: BaseRouteHandler) -> None:
    """Assert that the user is not currently in a session."""
    async_engine = connection.app.state.async_engine

    api_key = connection.query_params["api_key"]
    session_id = connection.query_params["session_id"]
    is_active = await _check_is_active(async_engine, api_key, session_id)
    if not is_active:
        raise NotAuthorizedException(detail="User is not in a session, create one at `/session_id`!")


@get("/session_id", guards=[valid_key_guard, user_in_session_guard], sync_to_thread=False)
def session_id(
    request: Request,
    api_key: str,
    fake_ip: str,
    map: str,
) -> dict[str, str]:
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

    _start_session(engine, api_key, session_id, fake_ip, map)

    return {"session_id": _session_id}


@get("/close_session", guards=[valid_key_guard, user_not_in_session_guard], sync_to_thread=False)
def close_session(request: Request, api_key: str, session_id: str) -> dict[str, bool]:
    """Close a session out.

    Returns:
        {"closed_successfully": True or False}
    """
    engine = request.app.state.engine
    current_time = datetime.now().astimezone(timezone.utc)
    _close_session(engine, api_key, session_id, current_time)

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

        engine = socket.app.state.engine
        current_time = datetime.now().astimezone(timezone.utc)

        _close_session_with_demo(engine, self.api_key, self.session_id, current_time, demo)

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
        engine = app.state.engine
        has_key = check_steam_id_has_api_key(engine, steam_id)

        if not has_key:
            api_key = uuid4().int
            provision_api_key(engine, steam_id, api_key)
            text = f"Successfully authenticated! Your API key is {api_key}! Do not lose this as the client needs it!"

        else:
            text = f"Steam ID {steam_id} already exists! If you forgot your API key, please let an admin know."

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
