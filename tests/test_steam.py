import json
import os
from typing import Any
from uuid import uuid4

import pytest
import toml
from api.auth import STEAM_API_KEY_KEYNAME, get_steam_api_key
from api.servers import Filters


@pytest.fixture(scope="session")
def steam_id() -> str:
    """Random session-scoped Steam ID."""
    return str(uuid4().int)


def write_json_steam_id(steam_id: str, tmpdir: os.PathLike) -> None:
    """Helper util to write a json JSON Steam ID."""
    data = {STEAM_API_KEY_KEYNAME: steam_id}
    path = os.path.join(tmpdir, "id.json")
    with open(path, "w") as f:
        f.write(json.dumps(data))

    return path


def write_toml_steam_id(steam_id: str, tmpdir: os.PathLike) -> None:
    """Helper util to write a json TOML Steam ID."""
    data = {STEAM_API_KEY_KEYNAME: steam_id}
    path = os.path.join(tmpdir, "id.toml")
    with open(path, "w") as f:
        f.write(toml.dumps(data))

    return path


def write_environment_steam_id(steam_id: str) -> None:
    """Helper util to write an Environment Variable Steam ID."""
    os.environ[STEAM_API_KEY_KEYNAME] = steam_id

    return STEAM_API_KEY_KEYNAME


def test_get_steam_api_key(steam_id: str, tmpdir: os.PathLike) -> None:
    """Test thhe ``get_steam_api_key`` function."""
    # test json
    key_location = write_json_steam_id(steam_id, tmpdir)
    assert get_steam_api_key(key_location) == steam_id

    # test toml
    key_location = write_toml_steam_id(steam_id, tmpdir)
    assert get_steam_api_key(key_location) == steam_id

    # test environment variable
    key_location = write_environment_steam_id(steam_id)
    assert get_steam_api_key(key_location) == steam_id

    with pytest.raises(KeyError):
        key_location = write_environment_steam_id(steam_id)
        os.environ.pop(STEAM_API_KEY_KEYNAME)
        get_steam_api_key(key_location)


@pytest.mark.parametrize("value,expected", [(True, 1), (False, 0), (None, None)])
def test_coerce_boolean(value: bool | None, expected: int | None) -> None:
    """Test that boolean params are coerced correctly."""
    actual = Filters.coerce_boolean(value)

    assert actual == expected


@pytest.mark.parametrize("value,expected", [("foo", ["foo"]), (["bar"], ["bar"]), (None, None)])
def test_coerce_listable(value: list[str] | str | None, expected: list[str] | None) -> None:
    """Test that listable params are coerced correctly."""
    actual = Filters.coerce_listable(value)

    assert actual == expected


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({}, ""),
        ({"dedicated": True}, r"filter=\dedicated\1"),
        ({"dedicated": True, "secure": False, "mapname": "asdf"}, r"filter=\dedicated\1,\secure\0,\map\asdf"),
        ({"dedicated": True, "gametype": ["foo", "bar"]}, r"filter=\dedicated\1,\gametype\foo,bar"),
        (
            {"dedicated": True, "secure": True, "gametype": ["foo", "bar"]},
            r"filter=\dedicated\1,\secure\1,\gametype\foo,bar",
        ),
    ],
)
def test_filters(kwargs: dict[str, Any], expected: str) -> None:
    filters = Filters(**kwargs)

    actual = filters._make_filter_str()
    assert actual == expected
