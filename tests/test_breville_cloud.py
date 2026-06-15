"""Tests for Breville+ cloud helper functions."""
from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from custom_components.joule_sous_vide.breville_cloud import (
    BREVILLE_AUTH_AUDIENCE,
    async_fetch_breville_ble_auth_key,
)


def _jwt(payload: dict[str, Any]) -> str:
    body = json.dumps(payload).encode()
    encoded = base64.urlsafe_b64encode(body).decode().rstrip("=")
    return f"header.{encoded}.signature"


class _FakeResponse:
    def __init__(self, status: int, payload: Any) -> None:
        self.status = status
        self._payload = payload
        self.headers = {}
        self.history = ()
        self.request_info = None

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None

    async def text(self) -> str:
        return json.dumps(self._payload)


class _FakeSession:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.id_token = _jwt({"sub": "auth0|005QU00000YtxALYAZ"})
        self.secret_key = bytes.fromhex("25ea00112233445566778899aabb75a7")

    def request(self, method: str, url: str, **kwargs: Any) -> _FakeResponse:
        self.requests.append((method, url, kwargs))
        if url == "https://my.breville.com/oauth/token":
            return _FakeResponse(
                200,
                {
                    "access_token": _jwt({"sub": "auth0|005QU00000YtxALYAZ"}),
                    "id_token": self.id_token,
                },
            )
        if url.endswith("/user/v2/user/auth0%7C005QU00000YtxALYAZ/appliances"):
            return _FakeResponse(
                200,
                {
                    "appliances": [
                        {
                            "name": "Jouletide",
                            "model": "CSJ100",
                            "serialNumber": "164213370",
                            "circulatorId": "c000004d93404f9e",
                        }
                    ]
                },
            )
        if url.endswith("/appliance/164213370/get-csj-secret-key"):
            return _FakeResponse(
                200,
                {"secretKey": base64.b64encode(self.secret_key).decode()},
            )
        return _FakeResponse(404, {"message": "not found"})


@pytest.mark.asyncio
async def test_fetch_breville_ble_auth_key_uses_breville_plus_auth_flow() -> None:
    """The helper uses the current Breville+ Auth0 domain and IoT API headers."""
    session = _FakeSession()

    result = await async_fetch_breville_ble_auth_key(
        session,  # type: ignore[arg-type]
        email="ficus@example.com",
        password="secret",
        circulator_id="c000004d93404f9e",
    )

    assert result.auth_key == session.secret_key
    assert result.serial_number == "164213370"
    assert result.circulator_id == "c000004d93404f9e"

    login = session.requests[0]
    assert login[0] == "POST"
    assert login[1] == "https://my.breville.com/oauth/token"
    assert login[2]["json"]["audience"] == BREVILLE_AUTH_AUDIENCE

    appliances = session.requests[1]
    assert appliances[0] == "GET"
    assert appliances[2]["headers"]["sf-id-token"] == session.id_token
    assert appliances[2]["headers"]["app"] == "brevilleJoule"
