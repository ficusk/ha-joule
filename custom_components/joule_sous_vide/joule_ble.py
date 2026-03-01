"""BLE API client for the ChefSteps Joule Sous Vide.

All methods are async. Uses bleak-retry-connector for reliable connection
establishment within HA's bluetooth stack.
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from bleak import BleakClient, BleakError
from bleak_retry_connector import establish_connection

from homeassistant.components.bluetooth import (
    async_ble_device_from_address,
    async_discovered_service_info,
)
from homeassistant.core import HomeAssistant

from .const import (
    FILE_CHAR_UUID,
    JOULE_MANUFACTURER_ID,
    SUBSCRIBE_CHAR_UUID,
    WRITE_CHAR_UUID,
)

_LOGGER = logging.getLogger(__name__)


class JouleBLEError(Exception):
    """Raised for any BLE communication failure with the Joule device."""


def mac_to_bytes(mac: str) -> bytes:
    """Convert a MAC address string like 'CF:8D:98:27:9B:98' to 6 bytes."""
    return bytes(int(b, 16) for b in mac.split(":"))


class JouleBLEAPI:
    """Manages the BLE connection and GATT characteristic I/O."""

    def __init__(self, hass: HomeAssistant, mac_address: str) -> None:
        self._hass = hass
        self.mac_address = mac_address
        self._client: BleakClient | None = None
        # 8-byte circulator address from BLE manufacturer advertising data.
        # The Joule advertises under company ID 0x0159 (ChefSteps); the payload
        # is the 8-byte address used as recipientAddress in protobuf messages.
        self.recipient_address: bytes = self._extract_circulator_address()
        # 8-byte sender address — SDK uses JWT token field 'a', or falls back
        # to "aabbaabbaabbaabb" when no cloud token is available.
        self.sender_address: bytes = bytes.fromhex("aabbaabbaabbaabb")

    def _extract_circulator_address(self) -> bytes:
        """Extract the 8-byte circulator address from BLE manufacturer data.

        The Joule advertises manufacturer-specific data under company ID 0x0159.
        HA's bluetooth scanner strips the 2-byte company ID prefix, so the value
        at key 0x0159 is the raw 8-byte circulator address.

        Falls back to the MAC padded to 8 bytes if manufacturer data is not
        (yet) available — this fallback will likely NOT work but allows setup
        to proceed so the user sees a helpful error rather than a crash.
        """
        try:
            for info in async_discovered_service_info(self._hass, connectable=True):
                if info.address.upper() == self.mac_address.upper():
                    if JOULE_MANUFACTURER_ID in info.manufacturer_data:
                        addr = bytes(info.manufacturer_data[JOULE_MANUFACTURER_ID])
                        _LOGGER.warning(
                            "Circulator address from manufacturer data: %s (%d bytes)",
                            addr.hex(), len(addr),
                        )
                        return addr
                    _LOGGER.warning(
                        "Found device %s but no manufacturer data for ID 0x%04X. "
                        "Available keys: %s",
                        self.mac_address,
                        JOULE_MANUFACTURER_ID,
                        list(info.manufacturer_data.keys()),
                    )
                    break
        except Exception:  # noqa: BLE001
            _LOGGER.warning(
                "Could not query bluetooth scanner for manufacturer data"
            )

        # Fallback: pad MAC to 8 bytes (unlikely to work)
        fallback = mac_to_bytes(self.mac_address) + b"\x00\x00"
        _LOGGER.warning(
            "Using padded MAC as fallback circulator address: %s", fallback.hex(),
        )
        return fallback

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
            _LOGGER.warning(
                "BLEDevice: name=%s, rssi=%s",
                ble_device.name,
                getattr(ble_device, "rssi", "N/A"),
            )

            client = await establish_connection(
                BleakClient, ble_device, self.mac_address
            )
            self._client = client
            _LOGGER.warning(
                "Connected to Joule at %s (MTU=%d)",
                self.mac_address,
                client.mtu_size,
            )
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

    async def verify_and_enable_notifications(self) -> bool:
        """Verify CCCD on 4325 is 0x0001, manually write if needed.

        Must be called AFTER subscribe().  Returns True if notifications
        are confirmed enabled, False otherwise.

        The v0.9.6 GATT dump showed CCCD = 0x0000 (disabled) before subscribe.
        If bleak's start_notify() didn't successfully write the CCCD, this
        method writes it manually.
        """
        if self._client is None:
            return False

        cccd_uuid = "00002902-0000-1000-8000-00805f9b34fb"
        for service in self._client.services:
            for char in service.characteristics:
                if char.uuid == SUBSCRIBE_CHAR_UUID:
                    for desc in char.descriptors:
                        if desc.uuid == cccd_uuid:
                            return await self._check_and_write_cccd(desc.handle)
        _LOGGER.warning("CCCD descriptor not found on 4325!")
        return False

    async def _check_and_write_cccd(self, handle: int) -> bool:
        """Read CCCD at handle, write 0x0001 if not already enabled."""
        try:
            val = bytes(await self._client.read_gatt_descriptor(handle))
            _LOGGER.warning(
                "CCCD on 4325 (handle %d) after subscribe: %s",
                handle, val.hex(),
            )
            if val == b"\x00\x00":
                _LOGGER.warning(
                    "Notifications NOT enabled! Manually writing CCCD 0x0001..."
                )
                await self._client.write_gatt_descriptor(
                    handle, b"\x01\x00"
                )
                val2 = bytes(
                    await self._client.read_gatt_descriptor(handle)
                )
                _LOGGER.warning("CCCD after manual write: %s", val2.hex())
                return val2 == b"\x01\x00"
            if val == b"\x01\x00":
                _LOGGER.warning("CCCD correctly shows notifications enabled")
                return True
            _LOGGER.warning("CCCD unexpected value: %s", val.hex())
            return False
        except BleakError as err:
            _LOGGER.warning("CCCD verify/write failed: %s", err)
            return False

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
        """Write a protobuf-encoded message to the device (write-with-response)."""
        _LOGGER.warning(
            "BLE WRITE to %s (%d bytes): %s", WRITE_CHAR_UUID, len(payload), payload.hex()
        )
        try:
            await self._client.write_gatt_char(
                WRITE_CHAR_UUID, bytearray(payload), response=True
            )
        except BleakError as err:
            raise JouleBLEError("Failed to write message to Joule") from err

    async def write_message_no_response(self, payload: bytes) -> None:
        """Write a protobuf-encoded message to 4322 with response=False."""
        _LOGGER.warning(
            "BLE WRITE-NR to %s (%d bytes): %s", WRITE_CHAR_UUID, len(payload), payload.hex()
        )
        try:
            await self._client.write_gatt_char(
                WRITE_CHAR_UUID, bytearray(payload), response=False
            )
        except BleakError as err:
            raise JouleBLEError("Failed to write (no-response) to Joule") from err

    async def write_to_file_char(self, payload: bytes) -> None:
        """Write a message to the FILE characteristic (4326) using write-without-response."""
        _LOGGER.warning(
            "BLE WRITE-WOR to %s (%d bytes): %s",
            FILE_CHAR_UUID, len(payload), payload.hex(),
        )
        try:
            await self._client.write_gatt_char(
                FILE_CHAR_UUID, bytearray(payload), response=False
            )
        except BleakError as err:
            raise JouleBLEError("Failed to write to FILE char on Joule") from err

    async def read_characteristic(self, char_uuid: str) -> bytes | None:
        """Read a GATT characteristic by UUID. Returns None on error."""
        try:
            data = await self._client.read_gatt_char(char_uuid)
            _LOGGER.warning(
                "BLE READ from %s: %d bytes, raw=%s",
                char_uuid,
                len(data),
                data.hex(),
            )
            return bytes(data)
        except BleakError as err:
            _LOGGER.warning("BLE READ failed on %s: %s", char_uuid, err)
            return None

    async def subscribe(self, callback: Callable[[Any, bytearray], None]) -> None:
        """Subscribe to notifications on the subscribe characteristic.

        ``callback`` is called with ``(characteristic, data)`` for each notification.
        """
        try:
            _LOGGER.warning("Subscribing to primary %s", SUBSCRIBE_CHAR_UUID)
            await self._client.start_notify(SUBSCRIBE_CHAR_UUID, callback)
            _LOGGER.warning("Subscribe complete")
        except BleakError as err:
            raise JouleBLEError("Failed to subscribe to Joule notifications") from err
