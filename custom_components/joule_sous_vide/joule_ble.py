"""BLE API client for the ChefSteps Joule Sous Vide.

All methods are async. Uses bleak-retry-connector for reliable connection
establishment within HA's bluetooth stack.
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from bleak import BleakClient, BleakError
from bleak_retry_connector import establish_connection

from homeassistant.components.bluetooth import async_ble_device_from_address
from homeassistant.core import HomeAssistant

from .const import SUBSCRIBE_CHAR_UUID, WRITE_CHAR_UUID

_LOGGER = logging.getLogger(__name__)


class JouleBLEError(Exception):
    """Raised for any BLE communication failure with the Joule device."""


class JouleBLEAPI:
    """Manages the BLE connection and GATT characteristic I/O."""

    def __init__(self, hass: HomeAssistant, mac_address: str) -> None:
        self._hass = hass
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
        """Open a BLE connection to the device via HA's bluetooth stack."""
        try:
            ble_device = async_ble_device_from_address(
                self._hass, self.mac_address, connectable=True
            )
            if ble_device is None:
                raise JouleBLEError(
                    f"Device {self.mac_address} not found by bluetooth scanner"
                )
            client = await establish_connection(
                BleakClient, ble_device, self.mac_address
            )
            self._client = client
            _LOGGER.info("Connected to Joule at %s", self.mac_address)
        except BleakError as err:
            self._client = None
            raise JouleBLEError(f"Failed to connect to {self.mac_address}") from err
        except JouleBLEError:
            raise
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
            await self._client.write_gatt_char(
                WRITE_CHAR_UUID, bytearray(payload), response=False
            )
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
