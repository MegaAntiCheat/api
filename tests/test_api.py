import base64
import time
from importlib.resources import files
from unittest import mock

import pytest
from api.app import app
from litestar.exceptions import NotAuthorizedException
from litestar.testing import TestClient

from tests import data

DEMO_CHUNKSIZE_BYTES = 1024


@pytest.fixture
def demo_file_path() -> str:
    source = files(data).joinpath("test_demo.dem")
    return str(source)


@pytest.fixture
def session_id() -> int:
    return 123


@pytest.fixture
def client() -> TestClient:
    yield TestClient(app=app)


def test_guard(client, session_id) -> None:
    with client as test_client:
        response = test_client.get("/session_id", params={"api_key": "foo"}).json()
        assert response == {"status_code": 401, "detail": "Unauthorized"}
