"""Litestar Application for serving and ingesting data."""

import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote, urlencode

import requests
import uvicorn
from litestar import Litestar, MediaType, Request, WebSocket, get, post
from litestar.exceptions import HTTPException, PermissionDeniedException, ValidationException
from litestar.handlers import WebsocketListener
from litestar.response import Redirect, Response, Stream
from litestar.status_codes import HTTP_500_INTERNAL_SERVER_ERROR
from sqlalchemy.exc import IntegrityError

from masterbase.anomaly import DetectionState
from masterbase.guards import (
    analyst_guard,
    session_closed_guard,
    user_in_session_guard,
    user_not_in_session_guard,
    valid_key_guard,
    valid_session_guard,
)
from masterbase.lib import (
    DemoSessionManager,
    SocketManagerMapType,
    add_loser,
    add_report,
    async_steam_id_from_api_key,
    check_is_active,
    check_is_loser,
    check_is_open,
    check_key_exists,
    check_steam_id_has_api_key,
    check_steam_id_is_beta_tester,
    close_session_helper,
    db_export_chunks,
    demo_blob_name,
    generate_api_key,
    generate_uuid4_int,
    late_bytes_helper,
    list_demos_helper,
    provision_api_key,
    resolve_hostname,
    set_open_false,
    set_open_true,
    start_session_helper,
    steam_id_from_api_key,
    update_api_key,
)
from masterbase.models import ExportTable, LateBytesBody, ReportBody
from masterbase.registers import shutdown_registers, startup_registers
from masterbase.steam import account_exists, is_limited_account

logger = logging.getLogger(__name__)


# use this to ensure client only has one open connection
streaming_sessions: SocketManagerMapType = {}


@get("/session_id", guards=[valid_key_guard, user_in_session_guard, valid_session_guard], sync_to_thread=False)
def session_id(
    request: Request,
    api_key: str,
    demo_name: str,
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
    steam_id = steam_id_from_api_key(engine, api_key)

    fake_ip = unquote(fake_ip)
    if not fake_ip.startswith("169"):
        to_resolve, port = fake_ip.split(":")
        fake_ip = f"{resolve_hostname(to_resolve)}:{port}"
    start_session_helper(engine, steam_id, str(_session_id), demo_name, fake_ip, map)

    return {"session_id": _session_id}


@get("/close_session", guards=[valid_key_guard, user_not_in_session_guard], sync_to_thread=False)
def close_session(request: Request, api_key: str) -> dict[str, bool]:
    """Close a session out. Will find the latest open session for a user.

    Returns:
        {"closed_successfully": True}
    """
    minio_client = request.app.state.minio_client
    engine = request.app.state.engine

    steam_id = steam_id_from_api_key(engine, api_key)
    msg = close_session_helper(minio_client, engine, steam_id, streaming_sessions)
    logger.info(msg)

    return {"closed_successfully": True}


@post("/late_bytes", guards=[valid_key_guard, user_not_in_session_guard], sync_to_thread=False)
def late_bytes(request: Request, api_key: str, data: LateBytesBody) -> dict[str, bool]:
    """Add late bytes to a closed demo session.

    Returns:
        {"late_bytes": True}
    """
    engine = request.app.state.engine
    current_time = datetime.now().astimezone(timezone.utc)
    steam_id = steam_id_from_api_key(engine, api_key)
    converted_late_bytes = bytes.fromhex(data.late_bytes)
    error = late_bytes_helper(engine, steam_id, converted_late_bytes, current_time)
    if error is None:
        return {"late_bytes": True}
    else:
        raise HTTPException(detail=error, status_code=422, extra={"late_bytes": False})


@get("/analyst_list_demos", guards=[valid_key_guard, analyst_guard], sync_to_thread=False)
def analyst_list_demos(
    request: Request, api_key: str, page_size: int | None = None, page_number: int | None = None
) -> list[dict[str, str]]:
    """List all demo data."""
    if page_size is None or page_size >= 50 or page_size < 1:
        page_size = 50
    if page_number is None or page_number < 1:
        page_number = 1
    engine = request.app.state.engine
    demos = list_demos_helper(engine, api_key, page_size, page_number, analyst=True)
    return demos


@get("/list_demos", guards=[valid_key_guard], sync_to_thread=False)
def list_demos(
    request: Request, api_key: str, page_size: int | None = None, page_number: int | None = None
) -> list[dict[str, str]]:
    """List demo data for user with `api_key`."""
    if page_size is None or page_size >= 50 or page_size < 1:
        page_size = 50
    if page_number is None or page_number < 1:
        page_number = 1
    engine = request.app.state.engine
    demos = list_demos_helper(engine, api_key, page_size, page_number, analyst=False)
    return demos


@get("/demodata", guards=[valid_key_guard, session_closed_guard, analyst_guard])
async def demodata(request: Request, api_key: str, session_id: str) -> Stream:
    """Return the demo."""
    minio_client = request.app.state.minio_client
    blob_name = demo_blob_name(session_id)
    file = minio_client.get_object("demoblobs", blob_name)
    stat = minio_client.stat_object("demoblobs", blob_name)

    headers = {
        "Content-Disposition": f'attachment; filename="{blob_name}"',
        "Content-Length": str(stat.size),
    }

    return Stream(file.stream(), media_type=MediaType.TEXT, headers=headers)


@get("/db_export", guards=[valid_key_guard, analyst_guard], sync_to_thread=False)
def db_export(request: Request, api_key: str, table: ExportTable, since: datetime | None = None) -> Stream:
    """Return a database export of the requested `table`."""
    engine = request.app.state.engine
    filename = f"{table.value}-{datetime.now()}.csv"
    return Stream(
        lambda: db_export_chunks(engine, table.value, since),
        headers={
            "Content-Type": "text/csv",
            "Content-Disposition": f"attachment; filename={filename}",
        },
    )


@post("/report", guards=[valid_key_guard])
async def report_player(request: Request, api_key: str, data: ReportBody) -> dict[str, bool]:
    """Add a player report."""
    engine = request.app.state.engine

    exists = account_exists(str(data.target_steam_id))
    if not exists:
        raise PermissionDeniedException(detail="Unknown target_steam_id!")
    try:
        add_report(engine, data.session_id, str(data.target_steam_id), data.reason.value)
        return {"report_added": True}
    except IntegrityError:
        raise HTTPException(detail=f"Unknown session ID {data.session_id}", status_code=402)


class DemoHandler(WebsocketListener):
    """Custom Websocket Class."""

    path = "/demos"
    receive_mode = "binary"

    async def on_accept(self, socket: WebSocket, api_key: str, session_id: str) -> None:  # type: ignore
        """Accept a user and create handle."""
        engine = socket.app.state.async_engine
        exists = await check_key_exists(engine, api_key)
        if not exists:
            logger.info("Invalid API key, closing!")
            await socket.close()

        steam_id = await async_steam_id_from_api_key(engine, api_key)
        active = await check_is_active(engine, steam_id)
        if not active:
            logger.info("User is not in a session, closing!")
            await socket.close()

        session_open = await check_is_open(engine, steam_id, session_id)
        if session_open:
            logger.info("User is already streaming data, closing!")
            await socket.close()

        await set_open_true(engine, steam_id, session_id)

        session_manager = DemoSessionManager(session_id=session_id, detection_state=DetectionState())

        if os.path.exists(session_manager.demo_path):
            mode = "ab"
            logger.info(f"Found existing handle for session ID: {session_id}")
        else:
            logger.info(f"Creating new handle for session ID: {session_id}")
            mode = "wb"

        session_manager.set_demo_handle(mode)
        streaming_sessions[socket] = session_manager

    async def on_disconnect(self, socket: WebSocket) -> None:  # type: ignore
        """Close handle on disconnect."""
        session_manager = streaming_sessions[socket]
        logger.info(f"Received socket disconnect from session ID: {session_manager.session_id}")
        session_manager.disconnect()
        await set_open_false(socket.app.state.async_engine, session_manager.session_id)

    def on_receive(self, data: bytes, socket: WebSocket) -> None:
        """Write data on disconnect."""
        session_manager = streaming_sessions[socket]
        logger.info(f"Sinking {len(data)} bytes to {session_manager.session_id}")
        session_manager.update(data)


@get("/provision", sync_to_thread=False)
def provision(request: Request) -> Redirect:
    """Provision a login/API key.

    Mostly stolen from https://github.com/TeddiO/pySteamSignIn/blob/master/pysteamsignin/steamsignin.py

    Args:
        request: current request object

    Returns:
        Redirect to the steam sign in
    """
    # enforce https on base_url
    base_url = str(request.base_url)
    if not base_url.startswith("https") and not os.environ["DEVELOPMENT"]:
        base_url = base_url.replace("http", "https")

    auth_params = {
        "openid.ns": "http://specs.openid.net/auth/2.0",
        "openid.mode": "checkid_setup",
        "openid.return_to": f"{base_url}/provision_handler",
        "openid.realm": f"{base_url}/provision_handler",
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
    # KeyError thrown when someone navigates here without a redirect from /provision...
    try:
        validation_args = {
            "openid.assoc_handle": data["openid.assoc_handle"],
            "openid.signed": data["openid.signed"],
            "openid.sig": data["openid.sig"],
            "openid.ns": data["openid.ns"],
        }
    except KeyError:
        return "<span>You aren't supposed to be here!</span>"

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
        # block limited accounts...

        # if we have seen this steam id before...
        loser = check_is_loser(engine, steam_id)
        if loser:
            return "limited"

        # if we have not seen this steam id before
        limited = is_limited_account(steam_id)
        if limited:
            add_loser(app.state.engine, steam_id)
            return "limited"

        is_beta_tester = check_steam_id_is_beta_tester(engine, steam_id)

        if not is_beta_tester:
            return "<span>You aren't a beta tester! Sorry!</span>"

        api_key = check_steam_id_has_api_key(engine, steam_id)
        new_api_key = generate_api_key()
        invalidated_text = ""
        if api_key is not None:
            # invalidate old API key and provision a new one
            invalidated_text = "Your old key was invalidated!"
            update_api_key(engine, steam_id, new_api_key)

        else:
            provision_api_key(engine, steam_id, new_api_key)

        text = f"Successfully authenticated! Your API key is '{new_api_key}' {invalidated_text} Do not lose this as the client needs it!"  # noqa

    return f"""
        <html>
            <body>
                <div>
                    <span>{text}</span>
                </div>
            </body>
        </html>
        """


def readable_exception_handler(_: Request, exception: Exception) -> Response:
    """Handle exceptions subclassed from HTTPException."""
    status_code = getattr(exception, "status_code", HTTP_500_INTERNAL_SERVER_ERROR)
    if isinstance(exception, ValidationException):
        return Response(
            content=json.dumps({"detail": "Validation error in request parameters", "errors": exception.extra}),
            status_code=status_code,
            media_type=MediaType.JSON,
        )
    if isinstance(exception, HTTPException):
        content = exception.detail
    else:
        content = "Internal Error Occurred!"
        logger.error("Exception occurred!", exc_info=exception)

    return Response(
        media_type=MediaType.TEXT,
        content=content,
        status_code=status_code,
    )


app = Litestar(
    on_startup=startup_registers,
    route_handlers=[
        session_id,
        close_session,
        DemoHandler,
        provision,
        provision_handler,
        late_bytes,
        demodata,
        list_demos,
        analyst_list_demos,
        report_player,
        db_export,
    ],
    exception_handlers={Exception: readable_exception_handler},
    on_shutdown=shutdown_registers,
    opt={"DEVELOPMENT": bool(os.getenv("DEVELOPMENT"))},
)


def main() -> None:
    """Enter app and setup config."""
    config = uvicorn.Config(
        "app:app", host="0.0.0.0", log_level="info", workers=6, ws_ping_interval=None, loop="uvloop"
    )
    server = uvicorn.Server(config)
    server.run()


if __name__ == "__main__":
    main()
