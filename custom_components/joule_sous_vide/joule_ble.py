"""BLE API client for the ChefSteps Joule Sous Vide.

All methods are async, using bleak (HA's standard BLE library).
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from bleak import BleakClient, BleakError

from .const import SUBSCRIBE_CHAR_UUID, WRITE_CHAR_UUID

_LOGGER = logging.getLogger(__name__)


class JouleBLEError(Exception):
    """Raised for any BLE communication failure with the Joule device."""


class JouleBLEAPI:
    """Manages the BLE connection and GATT characteristic I/O."""

    def __init__(self, mac_address: str) -> None:
        self.mac_address = mac_address
        self._client: BleakClient | None = None

    async def ensure_connected(self) -> bool:
        """Connect to the device if not already connected.

        Returns True if a fresh connection was established (caller should
        re-subscribe to notifications), False if already connected.
        """
        if self._client is not None and self._client.is_connected:
            return False
        await self.connect()
        return True

    async def connect(self) -> None:
        """Open a BLE connection to the device."""
        try:
            client = BleakClient(self.mac_address)
            await client.connect()
            self._client = client
            _LOGGER.info("Connected to Joule at %s", self.mac_address)
        except BleakError as err:
            self._client = None
            raise JouleBLEError(f"Failed to connect to {self.mac_address}") from err
        except Exception as err:
            self._client = None
            raise JouleBLEError(
                f"BLE backend error for {self.mac_address}: {err}"
            ) from err

    async def disconnect(self) -> None:
        """Close the BLE connection."""
        try:
            if self._client is not None:
                await self._client.disconnect()
        except BleakError as err:
            _LOGGER.warning("Error during disconnect from %s: %s", self.mac_address, err)
        finally:
            self._client = None

    async def write_message(self, payload: bytes) -> None:
        """Write a protobuf-encoded message to the device."""
        try:
            await self._client.write_gatt_char(WRITE_CHAR_UUID, bytearray(payload))
        except BleakError as err:
            raise JouleBLEError("Failed to write message to Joule") from err

    async def subscribe(self, callback: Callable[[Any, bytearray], None]) -> None:
        """Subscribe to notifications on the subscribe characteristic.

        ``callback`` is called with ``(characteristic, data)`` for each notification.
        """
        try:
            await self._client.start_notify(SUBSCRIBE_CHAR_UUID, callback)
        except BleakError as err:
            raise JouleBLEError("Failed to subscribe to Joule notifications") from err
