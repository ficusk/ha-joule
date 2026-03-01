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
    READ_CHAR_UUID,
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

    async def _query_bluez_dbus(self) -> None:
        """Query BlueZ D-Bus directly for device properties.

        HA's bluetooth scanner may not expose ManufacturerData even though
        BlueZ has it cached.  This queries the D-Bus object at the device
        path to get ALL Device1 properties and extract the circulator address
        if ManufacturerData is present.
        """
        try:
            from dbus_fast.aio import MessageBus
            from dbus_fast import BusType, Variant
        except ImportError:
            _LOGGER.warning("dbus_fast not available — skipping D-Bus query")
            return

        dbus_path = (
            f"/org/bluez/hci0/dev_{self.mac_address.replace(':', '_')}"
        )
        bus = None
        try:
            bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
            introspection = await bus.introspect("org.bluez", dbus_path)
            proxy = bus.get_proxy_object(
                "org.bluez", dbus_path, introspection
            )
            props = proxy.get_interface("org.freedesktop.DBus.Properties")
            all_props = await props.call_get_all("org.bluez.Device1")

            # Log all interesting properties
            for key in (
                "Name", "Address", "RSSI", "ManufacturerData",
                "ServiceUUIDs", "ServiceData", "UUIDs",
            ):
                if key in all_props:
                    val = all_props[key]
                    if isinstance(val, Variant):
                        val = val.value
                    _LOGGER.warning("  D-Bus %s = %s", key, val)

            # Extract ManufacturerData if present
            if "ManufacturerData" in all_props:
                raw = all_props["ManufacturerData"]
                if isinstance(raw, Variant):
                    raw = raw.value
                for company_id, payload in raw.items():
                    if isinstance(payload, Variant):
                        payload = payload.value
                    payload_bytes = bytes(payload)
                    _LOGGER.warning(
                        "  D-Bus ManufacturerData[0x%04X] = %s (%d bytes)",
                        company_id, payload_bytes.hex(), len(payload_bytes),
                    )
                    if company_id == JOULE_MANUFACTURER_ID:
                        _LOGGER.warning(
                            "  Found circulator address via D-Bus: %s",
                            payload_bytes.hex(),
                        )
                        self.recipient_address = payload_bytes
            else:
                _LOGGER.warning(
                    "  D-Bus: ManufacturerData property NOT present for %s",
                    self.mac_address,
                )
        except Exception:  # noqa: BLE001
            _LOGGER.warning("D-Bus query failed for %s (non-fatal)", dbus_path)
        finally:
            if bus is not None:
                try:
                    bus.disconnect()
                except Exception:  # noqa: BLE001
                    pass

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
            # Log BLEDevice details and re-extract manufacturer data if stale
            _LOGGER.warning(
                "BLEDevice: name=%s, rssi=%s, details=%s",
                ble_device.name,
                getattr(ble_device, "rssi", "N/A"),
                ble_device.details,
            )
            # Re-check manufacturer data at connect time (scanner may have
            # updated since __init__)
            for info in async_discovered_service_info(self._hass, connectable=True):
                if info.address.upper() == self.mac_address.upper():
                    _LOGGER.warning(
                        "Manufacturer data at connect: %s",
                        {hex(k): v.hex() for k, v in info.manufacturer_data.items()},
                    )
                    if JOULE_MANUFACTURER_ID in info.manufacturer_data:
                        fresh = bytes(info.manufacturer_data[JOULE_MANUFACTURER_ID])
                        if fresh != self.recipient_address:
                            _LOGGER.warning(
                                "Updating circulator address: %s -> %s",
                                self.recipient_address.hex(), fresh.hex(),
                            )
                            self.recipient_address = fresh
                    break
            # Query BlueZ D-Bus directly for ManufacturerData (HA scanner
            # may not have it, but BlueZ might have it cached)
            await self._query_bluez_dbus()

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
