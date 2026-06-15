"""Breville+ cloud helpers for importing legacy Joule BLE auth keys."""
from __future__ import annotations

import base64
import json
import logging
import urllib.parse
from dataclasses import dataclass
from typing import Any

from aiohttp import ClientError, ClientResponseError, ClientSession

_LOGGER = logging.getLogger(__name__)

BREVILLE_AUTH_DOMAIN = "my.breville.com"
BREVILLE_AUTH_AUDIENCE_DOMAIN = "iden-prod.us.auth0.com"
BREVILLE_AUTH_AUDIENCE = f"https://{BREVILLE_AUTH_AUDIENCE_DOMAIN}/userinfo"
BREVILLE_AUTH_CLIENT_ID = "A2IYXGeuX1g8s049YEri6WC6hu2wlrMZ"
BREVILLE_AUTH_REALM = "Salesforce"
BREVILLE_AUTH_SCOPE = "openid profile email offline_access"
BREVILLE_USER_API_URL = "https://iot-api.breville.com/user"
BREVILLE_APPLIANCE_API_URL = "https://iot-api.breville.com/appliance/v1"
BREVILLE_APP_HEADER = "brevilleJoule"


class BrevilleCloudError(Exception):
    """Raised when Breville cloud auth or appliance lookup fails."""


class BrevilleCloudAuthError(BrevilleCloudError):
    """Raised when Breville credentials are rejected."""


class BrevilleApplianceNotFound(BrevilleCloudError):
    """Raised when no matching legacy Joule appliance can be selected."""


@dataclass(frozen=True)
class BrevilleCloudKey:
    """Imported Joule auth material from Breville+."""

    auth_key: bytes
    circulator_id: str
    serial_number: str
    name: str


async def async_fetch_breville_ble_auth_key(
    session: ClientSession,
    *,
    email: str,
    password: str,
    circulator_id: str | None = None,
    serial_number: str | None = None,
) -> BrevilleCloudKey:
    """Sign in to Breville+ and return the stored legacy Joule BLE auth key."""
    tokens = await _async_breville_login(session, email=email, password=password)
    appliances, _auth_label, _user_id = await _async_fetch_appliances(session, tokens)
    appliance = _choose_appliance(
        appliances,
        target_circulator_id=circulator_id or "",
        target_serial=serial_number,
    )
    if appliance is None:
        raise BrevilleApplianceNotFound(
            "Could not find a matching Joule in the Breville account"
        )

    selected_circulator_id = _appliance_circulator_id(appliance)
    selected_serial = _appliance_serial(appliance)
    if not selected_circulator_id or not selected_serial:
        raise BrevilleApplianceNotFound(
            "Breville appliance record is missing circulator id or serial number"
        )

    auth_key = await _async_fetch_csj_secret_key(
        session,
        tokens,
        serial_number=selected_serial,
        circulator_id=selected_circulator_id,
    )
    return BrevilleCloudKey(
        auth_key=auth_key,
        circulator_id=selected_circulator_id,
        serial_number=selected_serial,
        name=_appliance_name(appliance),
    )


async def _async_breville_login(
    session: ClientSession,
    *,
    email: str,
    password: str,
) -> dict[str, Any]:
    payload = {
        "grant_type": "http://auth0.com/oauth/grant-type/password-realm",
        "username": email,
        "password": password,
        "realm": BREVILLE_AUTH_REALM,
        "scope": BREVILLE_AUTH_SCOPE,
        "audience": BREVILLE_AUTH_AUDIENCE,
        "client_id": BREVILLE_AUTH_CLIENT_ID,
    }
    try:
        response = await _async_json_request(
            session,
            "POST",
            f"https://{BREVILLE_AUTH_DOMAIN}/oauth/token",
            json=payload,
        )
    except ClientResponseError as err:
        if err.status in (400, 401, 403):
            raise BrevilleCloudAuthError("Breville credentials were rejected") from err
        raise BrevilleCloudError(f"Breville login failed: HTTP {err.status}") from err

    if not isinstance(response, dict) or not isinstance(
        response.get("access_token"), str
    ):
        raise BrevilleCloudError("Breville login response did not include a token")
    return response


async def _async_fetch_appliances(
    session: ClientSession,
    tokens: dict[str, Any],
) -> tuple[list[dict[str, Any]], str, str]:
    failures: list[str] = []
    for auth_label, headers in _iot_auth_candidates(tokens):
        for user_id in _user_id_candidates(tokens):
            quoted_user_id = urllib.parse.quote(user_id, safe="")
            try:
                response = await _async_json_request(
                    session,
                    "GET",
                    f"{BREVILLE_USER_API_URL}/v2/user/{quoted_user_id}/appliances",
                    headers=headers,
                )
            except ClientResponseError as err:
                if err.status in (401, 403, 404):
                    failures.append(f"{auth_label} user_id={user_id!r}: {err.status}")
                    continue
                raise BrevilleCloudError(
                    f"Breville appliance lookup failed: HTTP {err.status}"
                ) from err
            return _extract_appliances(response), auth_label, user_id

    details = "; ".join(failures[-6:]) if failures else "no auth candidates"
    raise BrevilleCloudAuthError(f"Breville appliance lookup was unauthorized: {details}")


async def _async_fetch_csj_secret_key(
    session: ClientSession,
    tokens: dict[str, Any],
    *,
    serial_number: str,
    circulator_id: str,
) -> bytes:
    token = tokens.get("id_token")
    if not isinstance(token, str) or not token:
        raise BrevilleCloudAuthError("Breville login response did not include an id_token")

    quoted_serial = urllib.parse.quote(serial_number, safe="")
    try:
        response = await _async_json_request(
            session,
            "POST",
            f"{BREVILLE_APPLIANCE_API_URL}/appliance/{quoted_serial}/get-csj-secret-key",
            headers=_iot_headers(token),
            json={"circulatorId": circulator_id},
        )
    except ClientResponseError as err:
        if err.status in (401, 403):
            raise BrevilleCloudAuthError(
                "Breville rejected the Joule secret-key request"
            ) from err
        raise BrevilleCloudError(
            f"Breville secret-key request failed: HTTP {err.status}"
        ) from err

    if not isinstance(response, dict):
        raise BrevilleCloudError("Unexpected Breville secret-key response")
    secret_key = _first_string(response, ("secretKey", "secret_key"))
    if not secret_key:
        raise BrevilleCloudError("Breville secret-key response had no secretKey")
    return _decode_cloud_secret_key(secret_key)


async def _async_json_request(
    session: ClientSession,
    method: str,
    url: str,
    **kwargs: Any,
) -> Any:
    try:
        async with session.request(method, url, **kwargs) as response:
            text = await response.text()
            if response.status >= 400:
                raise ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=text,
                    headers=response.headers,
                )
            if not text:
                return None
            try:
                return json.loads(text)
            except json.JSONDecodeError as err:
                raise BrevilleCloudError(
                    f"Breville returned non-JSON response from {url}"
                ) from err
    except ClientResponseError:
        raise
    except ClientError as err:
        raise BrevilleCloudError(f"Breville request failed: {err}") from err


def _iot_headers(token: str, *, token_format: str = "sf-id-token") -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "app": BREVILLE_APP_HEADER,
        "Content-Type": "application/json",
    }
    if token_format == "sf-id-token":
        headers["sf-id-token"] = token
    elif token_format == "bearer":
        headers["Authorization"] = f"Bearer {token}"
    else:
        raise ValueError(f"unknown Breville token format: {token_format}")
    return headers


def _iot_auth_candidates(tokens: dict[str, Any]) -> list[tuple[str, dict[str, str]]]:
    candidates: list[tuple[str, dict[str, str]]] = []
    id_token = tokens.get("id_token")
    access_token = tokens.get("access_token")
    if isinstance(id_token, str) and id_token:
        candidates.append(("id-token/sf-id-token", _iot_headers(id_token)))
    if isinstance(access_token, str) and access_token:
        candidates.append(("access-token/sf-id-token", _iot_headers(access_token)))
        candidates.append(("access-token/bearer", _iot_headers(access_token, token_format="bearer")))
    return candidates


def _user_id_candidates(tokens: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    for key in ("id_token", "access_token"):
        token = tokens.get(key)
        if not isinstance(token, str):
            continue
        payload = _jwt_payload(token)
        for claim in (
            "id",
            "userId",
            "user_id",
            "https://breville.com/user_id",
            "https://breville.com/userId",
            "https://www.breville.com/user_id",
            "https://www.breville.com/userId",
            "sub",
        ):
            value = payload.get(claim)
            if isinstance(value, str):
                _add_unique(candidates, value)
        claims = payload.get("https://hasura.io/jwt/claims")
        if isinstance(claims, dict):
            for claim in ("x-hasura-user-id", "x-hasura-auth0-id", "x-hasura-userid"):
                value = claims.get(claim)
                if isinstance(value, str):
                    _add_unique(candidates, value)
    return candidates


def _jwt_payload(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    try:
        segment = parts[1]
        padded = segment + "=" * ((4 - len(segment) % 4) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        decoded = json.loads(raw.decode("utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _extract_appliances(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    if isinstance(response, dict):
        for container in (response, response.get("data")):
            if not isinstance(container, dict):
                continue
            appliances = container.get("appliances")
            if isinstance(appliances, list):
                return [item for item in appliances if isinstance(item, dict)]
    raise BrevilleCloudError("Unexpected Breville appliances response")


def _choose_appliance(
    appliances: list[dict[str, Any]],
    *,
    target_circulator_id: str,
    target_serial: str | None,
) -> dict[str, Any] | None:
    if target_serial:
        normalized_serial = _normalize_identifier(target_serial)
        for appliance in appliances:
            if _normalize_identifier(_appliance_serial(appliance)) == normalized_serial:
                return appliance

    normalized_target = _normalize_identifier(target_circulator_id)
    if normalized_target:
        for appliance in appliances:
            values = [_appliance_circulator_id(appliance), *_all_strings(appliance)]
            if any(_normalize_identifier(value) == normalized_target for value in values):
                return appliance

    candidates = [
        appliance
        for appliance in appliances
        if "CSJ" in _appliance_model(appliance).upper()
        or "JOULE" in _appliance_model(appliance).upper()
        or _appliance_circulator_id(appliance)
    ]
    if len(candidates) == 1:
        return candidates[0]
    _LOGGER.debug("Could not select one Joule appliance from %d candidates", len(candidates))
    return None


def _appliance_circulator_id(appliance: dict[str, Any]) -> str:
    return _first_string(
        appliance,
        (
            "circulatorId",
            "circulator_id",
            "recipientAddress",
            "recipient_address",
            "jouleId",
            "joule_id",
        ),
    ).lower()


def _appliance_serial(appliance: dict[str, Any]) -> str:
    return _first_string(
        appliance,
        ("serialNumber", "serial_number", "serialNo", "serial", "applianceSerial"),
    )


def _appliance_model(appliance: dict[str, Any]) -> str:
    return _first_string(
        appliance,
        ("model", "modelNumber", "model_number", "productModel", "applianceModel"),
    )


def _appliance_name(appliance: dict[str, Any]) -> str:
    return _first_string(appliance, ("name", "applianceName", "nickname", "label"))


def _first_string(value: Any, keys: tuple[str, ...]) -> str:
    if isinstance(value, dict):
        for key in keys:
            child = value.get(key)
            if isinstance(child, str) and child:
                return child
        for child in value.values():
            found = _first_string(child, keys)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _first_string(child, keys)
            if found:
                return found
    return ""


def _all_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        strings: list[str] = []
        for child in value.values():
            strings.extend(_all_strings(child))
        return strings
    if isinstance(value, list):
        strings = []
        for child in value:
            strings.extend(_all_strings(child))
        return strings
    return []


def _decode_cloud_secret_key(value: str) -> bytes:
    secret = value.strip()
    hex_chars = set("0123456789abcdefABCDEF")
    if len(secret) % 2 == 0 and secret and all(ch in hex_chars for ch in secret):
        return bytes.fromhex(secret)
    padded = secret + "=" * ((4 - len(secret) % 4) % 4)
    try:
        raw = base64.b64decode(padded, validate=True)
    except Exception:  # noqa: BLE001
        raw = base64.urlsafe_b64decode(padded)
    if not raw:
        raise BrevilleCloudError("Breville returned an empty secret key")
    return raw


def _normalize_identifier(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def _add_unique(values: list[str], value: str | None) -> None:
    if value and value not in values:
        values.append(value)
