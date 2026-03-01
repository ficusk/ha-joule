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

from .const import FILE_CHAR_UUID, READ_CHAR_UUID, SUBSCRIBE_CHAR_UUID, WRITE_CHAR_UUID

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
        self.recipient_address: bytes = mac_to_bytes(mac_address)
        # BLE uses LSB-first byte order for addresses
        self.recipient_address_reversed: bytes = self.recipient_address[::-1]
        # Non-zero sender address (arbitrary app identifier)
        self.sender_address: bytes = b"\x01\x00\x00\x00\x00\x01"

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
            # Log BLEDevice details to identify adapter (local vs proxy)
            _LOGGER.warning(
                "BLEDevice: name=%s, rssi=%s, details=%s",
                ble_device.name,
                getattr(ble_device, "rssi", "N/A"),
                ble_device.details,
            )
            client = await establish_connection(
                BleakClient, ble_device, self.mac_address
            )
            self._client = client
            _LOGGER.warning("Connected to Joule at %s", self.mac_address)
            # Dump GATT services at WARNING level so they always appear
            for service in client.services:
                _LOGGER.warning("  Service: %s", service.uuid)
                for char in service.characteristics:
                    props = ", ".join(char.properties)
                    _LOGGER.warning(
                        "    Char: %s [%s] handle=%s",
                        char.uuid,
                        props,
                        char.handle,
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

    async def disconnect(self) -> None:
        """Close the BLE connection."""
        try:
            if self._client is not None:
                await self._client.disconnect()
        except BleakError as err:
            _LOGGER.warning("Error during disconnect from %s: %s", self.mac_address, err)
        finally:
            self._client = None

    async def pair(self) -> bool:
        """Attempt BLE pairing/bonding with the device.

        On Linux (BlueZ), registers a D-Bus NoInputNoOutput pairing agent
        first so that "Just Works" pairing can complete without user
        interaction.  Returns True if pairing succeeded.
        """
        agent_registered = False
        bus = None
        try:
            # Try to register a D-Bus pairing agent (Linux/BlueZ only)
            bus, agent_registered = await self._register_pairing_agent()
        except Exception:  # noqa: BLE001
            _LOGGER.warning("D-Bus agent registration skipped (not Linux?)")

        try:
            _LOGGER.warning("Attempting BLE pair with %s", self.mac_address)
            result = await self._client.pair()
            _LOGGER.warning("Pair result: %s", result)
            return True
        except (BleakError, Exception) as err:  # noqa: BLE001
            _LOGGER.warning("Pair failed (non-fatal): %s", err)
            return False
        finally:
            if agent_registered and bus is not None:
                await self._unregister_pairing_agent(bus)

    async def _register_pairing_agent(self) -> tuple[Any, bool]:
        """Register a NoInputNoOutput pairing agent with BlueZ via D-Bus.

        Returns (bus, True) on success, (None, False) on failure.
        The agent class is defined in _dbus_agent.py (without PEP 563
        future annotations) so dbus_fast can read D-Bus type signatures.
        """
        try:
            from dbus_fast.aio import MessageBus
            from dbus_fast import BusType
            from ._dbus_agent import JouleAgent
        except ImportError:
            _LOGGER.warning("dbus_fast not available — skipping agent")
            return None, False

        bus = None
        try:
            bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            agent_path = "/joule_sous_vide/agent"

            agent = JouleAgent()
            bus.export(agent_path, agent)

            # Call AgentManager1.RegisterAgent + RequestDefaultAgent
            introspection = await bus.introspect("org.bluez", "/org/bluez")
            proxy = bus.get_proxy_object(
                "org.bluez", "/org/bluez", introspection,
            )
            agent_mgr = proxy.get_interface("org.bluez.AgentManager1")
            await agent_mgr.call_register_agent(agent_path, "NoInputNoOutput")
            await agent_mgr.call_request_default_agent(agent_path)
            _LOGGER.warning("D-Bus pairing agent registered at %s", agent_path)
            return bus, True

        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Failed to register D-Bus agent: %s", err)
            if bus is not None:
                bus.disconnect()
            return None, False

    async def _unregister_pairing_agent(self, bus: Any) -> None:
        """Unregister the pairing agent and disconnect the D-Bus bus."""
        try:
            agent_path = "/joule_sous_vide/agent"
            introspection = await bus.introspect("org.bluez", "/org/bluez")
            proxy = bus.get_proxy_object(
                "org.bluez", "/org/bluez", introspection,
            )
            agent_mgr = proxy.get_interface("org.bluez.AgentManager1")
            await agent_mgr.call_unregister_agent(agent_path)
            _LOGGER.warning("D-Bus pairing agent unregistered")
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Failed to unregister D-Bus agent (non-fatal)")
        finally:
            try:
                bus.disconnect()
            except Exception:  # noqa: BLE001
                pass

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
