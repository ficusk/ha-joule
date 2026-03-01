"""Data update coordinator for the Joule Sous Vide integration."""
from __future__ import annotations

import asyncio
from datetime import timedelta
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    CONF_MAC_ADDRESS,
    DEFAULT_COOK_TIME_MINUTES,
    DEFAULT_SCAN_INTERVAL,
    DEFAULT_TARGET_TEMPERATURE,
    DEFAULT_TEMPERATURE_UNIT,
    DOMAIN,
    READ_CHAR_UUID,
    SUBSCRIBE_CHAR_UUID,
)
from .joule_ble import JouleBLEAPI, JouleBLEError
from .joule_proto import (
    CirculatorDataPoint,
    ProgramStep,
    build_live_feed_message,
    build_start_cook_message,
    build_start_key_exchange_message,
    build_stop_cook_message,
    build_submit_key_message,
    decode_stream_message,
    parse_notification,
)

_LOGGER = logging.getLogger(__name__)

# Config entry option key for persisted BLE auth key
CONF_BLE_AUTH_KEY = "ble_auth_key"


class JouleCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Single owner of the BLE connection; provides data to all entities.

    Entities must not create their own BLE connections. They read exclusively
    from coordinator.data and call coordinator methods for control actions.

    The Joule uses a notification-triggered-read pattern:
    - Notifications on 4325 are "data ready" signals (may contain no payload)
    - The actual protobuf data must be READ from 4323 after each notification
    """

    NOTIFICATION_TIMEOUT: float = 10.0  # seconds; overridden in tests
    KEY_EXCHANGE_TIMEOUT: float = 60.0  # seconds; user must press button

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self._entry = entry
        self.api = JouleBLEAPI(hass, entry.data[CONF_MAC_ADDRESS])
        self._is_cooking: bool = False
        self._target_temperature: float = DEFAULT_TARGET_TEMPERATURE  # always °C
        self._cook_time_minutes: float = DEFAULT_COOK_TIME_MINUTES
        self._temperature_unit: str = entry.options.get(
            "temperature_unit", DEFAULT_TEMPERATURE_UNIT
        )
        self._latest_data_point: CirculatorDataPoint | None = None
        self._notification_received: asyncio.Event = asyncio.Event()
        self._subscribed: bool = False
        self._authenticated: bool = False
        # Load persisted auth key from config entry options (if previously paired)
        stored_key = entry.options.get(CONF_BLE_AUTH_KEY)
        self._auth_key: bytes | None = (
            bytes.fromhex(stored_key) if stored_key else None
        )

        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL),
        )

    def _on_notification(self, characteristic: Any, data: bytearray) -> None:
        """Handle a BLE notification from bleak (runs on the event loop).

        The Joule uses notification-triggered-read: notifications on 4325 are
        "data ready" signals.  The actual protobuf data lives on 4323 (the read
        characteristic).  We schedule a read from 4323 every time a notification
        fires.  If the notification itself carries protobuf data we try to
        decode that too.
        """
        _LOGGER.warning(
            "NOTIFICATION on 4325: %d bytes, raw=%s",
            len(data),
            data.hex() if data else "(empty)",
        )
        # Schedule a read from 4323 (the real data source)
        self.hass.async_create_task(self._read_and_process())
        # Also try to decode inline notification data (if non-empty)
        if data and len(data) > 0:
            self._try_decode_message(bytes(data), source="notification")

    async def _read_and_process(self) -> None:
        """Read 4323 and process any data found (notification-triggered-read)."""
        try:
            read_data = await self.api.read_characteristic(READ_CHAR_UUID)
            if read_data and len(read_data) > 0:
                _LOGGER.warning(
                    "READ from 4323 (triggered by notification): %d bytes: %s",
                    len(read_data), read_data.hex(),
                )
                self._try_decode_message(read_data, source="4323-read")
            else:
                _LOGGER.warning(
                    "READ from 4323 after notification: empty — signalling anyway",
                )
                self._notification_received.set()
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Failed to read 4323 after notification")
            self._notification_received.set()

    def _try_decode_message(self, data: bytes, source: str) -> None:
        """Decode a protobuf StreamMessage and update internal state."""
        try:
            msg = decode_stream_message(data)
            if msg.start_key_exchange_reply is not None:
                key = msg.start_key_exchange_reply.secret_key
                _LOGGER.warning(
                    "Got StartKeyExchangeReply from %s! key=%s result=%d",
                    source, key.hex(), msg.start_key_exchange_reply.result,
                )
                self._auth_key = key
                self._persist_auth_key(key)
                self._notification_received.set()
            elif msg.submit_key_reply is not None:
                _LOGGER.warning(
                    "Got SubmitKeyReply from %s! result=%d",
                    source, msg.submit_key_reply.result,
                )
                self._authenticated = True
                self._notification_received.set()
            elif msg.pong is not None:
                _LOGGER.warning("Got PONG from %s!", source)
                self._notification_received.set()
            elif msg.circulator_data_point is not None:
                dp = msg.circulator_data_point
                _LOGGER.warning(
                    "CirculatorDataPoint from %s: bath_temp=%.2f, step=%s",
                    source, dp.bath_temp, dp.program_step,
                )
                self._latest_data_point = dp
                self._notification_received.set()
            else:
                _LOGGER.warning(
                    "Message from %s: handle=%d end=%s sender=%s — unrecognized",
                    source, msg.handle, msg.end,
                    msg.sender_address.hex() if msg.sender_address else "none",
                )
                self._notification_received.set()
        except Exception:  # noqa: BLE001
            _LOGGER.warning("Failed to decode message from %s: %s", source, data.hex())
            self._notification_received.set()

    def _persist_auth_key(self, key: bytes) -> None:
        """Save the BLE auth key to config entry options for future connections."""
        _LOGGER.warning("Persisting BLE auth key: %s", key.hex())
        self.hass.config_entries.async_update_entry(
            self._entry,
            options={**self._entry.options, CONF_BLE_AUTH_KEY: key.hex()},
        )

    async def _try_write_and_wait(
        self, label: str, payload: bytes, timeout: float,
    ) -> bool:
        """Write a payload to 4322 (write-with-response) and wait for data.

        The 4322 characteristic has the [write] property (Write Request only).
        The write has a 10s timeout to avoid blocking if device doesn't ACK.
        After the write, waits for a notification on 4325 (triggers a read
        from 4323).  During the wait, reads 4323 every 5 seconds to keep the
        BLE connection alive and to catch responses.  Returns True on response.
        """
        _LOGGER.warning(
            "%s (%d bytes): %s", label, len(payload), payload.hex(),
        )
        self._notification_received.clear()

        # Write with response=True (matches 4322's [write] property).
        try:
            async with asyncio.timeout(10):
                await self.api.write_message(payload)
            _LOGGER.warning("Write succeeded for %s", label)
        except TimeoutError:
            _LOGGER.warning("Write timed out for %s", label)
            return False
        except JouleBLEError as err:
            _LOGGER.warning("Write failed for %s: %s", label, err)
            return False

        # Poll: wait for notification OR check 4323+4325 every 5 seconds
        elapsed = 0.0
        poll_interval = 5.0
        while elapsed < timeout:
            wait_time = min(poll_interval, timeout - elapsed)
            try:
                async with asyncio.timeout(wait_time):
                    await self._notification_received.wait()
                _LOGGER.warning("Got response after %s!", label)
                return True
            except TimeoutError:
                elapsed += wait_time

            # Read BOTH 4323 and 4325 — response could be on either
            for char_uuid, char_name in [
                (READ_CHAR_UUID, "4323"),
                (SUBSCRIBE_CHAR_UUID, "4325"),
            ]:
                read_data = await self.api.read_characteristic(char_uuid)
                if read_data and len(read_data) > 0:
                    _LOGGER.warning(
                        "Poll %s after %s: %d bytes: %s",
                        char_name, label, len(read_data), read_data.hex(),
                    )
                    self._try_decode_message(
                        read_data, source=f"{char_name}-poll-{label}",
                    )
                    if self._notification_received.is_set():
                        return True

            _LOGGER.warning(
                "Poll after %s at %.0fs: both empty", label, elapsed,
            )

        _LOGGER.warning("No response to %s after %.0fs", label, timeout)
        return False

    async def _read_and_log(self, char_uuid: str, label: str) -> bytes | None:
        """Read a characteristic and log the result. Returns raw bytes."""
        data = await self.api.read_characteristic(char_uuid)
        if data and len(data) > 0:
            _LOGGER.warning(
                "%s: %d bytes: %s", label, len(data), data.hex(),
            )
        else:
            _LOGGER.warning("%s: empty", label)
        return data

    async def _async_update_data(self) -> dict[str, Any]:
        """Diagnostic v0.8.7: BLE pairing + raw handshake + GAP reads.

        v0.8.6 confirmed 4325 data (00010203) is STATIC — not a response.
        The device accepts GATT writes but the firmware ignores our protobuf.
        This version tests whether BLE-level pairing or a raw handshake
        (writing 4324 nonce directly to 4322) activates the command handler.
        """
        try:
            reconnected = await self.api.ensure_connected()
            if reconnected:
                _LOGGER.warning("Fresh BLE connection — will re-subscribe")
                self._subscribed = False
                self._authenticated = False

            # === STEP 1: Read GAP characteristics for firmware info ===
            _LOGGER.warning("=== STEP 1: GAP characteristics ===")
            gap_name = await self.api.read_characteristic(
                "00002a00-0000-1000-8000-00805f9b34fb"
            )
            if gap_name:
                try:
                    _LOGGER.warning(
                        "Device Name: %s (hex: %s)",
                        bytes(gap_name).decode("utf-8", errors="replace"),
                        gap_name.hex(),
                    )
                except Exception:  # noqa: BLE001
                    _LOGGER.warning("Device Name (raw): %s", gap_name.hex())

            gap_appearance = await self.api.read_characteristic(
                "00002a01-0000-1000-8000-00805f9b34fb"
            )
            if gap_appearance:
                _LOGGER.warning("Appearance: %s", gap_appearance.hex())

            gap_params = await self.api.read_characteristic(
                "00002a04-0000-1000-8000-00805f9b34fb"
            )
            if gap_params:
                _LOGGER.warning("Preferred Conn Params: %s", gap_params.hex())

            # === STEP 2: Subscribe + baseline reads ===
            _LOGGER.warning("=== STEP 2: Subscribe + baseline ===")
            if not self._subscribed:
                await self.api.subscribe(self._on_notification)
                self._subscribed = True

            char_4324 = "700b4324-9836-4383-a2b2-31a9098d1473"
            nonce_4324 = await self._read_and_log(char_4324, "4324 nonce")
            baseline_4325 = await self._read_and_log(
                SUBSCRIBE_CHAR_UUID, "BASELINE 4325",
            )
            baseline_4323 = await self._read_and_log(
                READ_CHAR_UUID, "BASELINE 4323",
            )

            # === STEP 3: Attempt BLE pairing ===
            _LOGGER.warning("=== STEP 3: BLE pairing attempt ===")
            await self.api.pair()

            # Check if pairing changed anything
            post_pair_4325 = await self._read_and_log(
                SUBSCRIBE_CHAR_UUID, "4325 AFTER PAIR",
            )
            post_pair_4323 = await self._read_and_log(
                READ_CHAR_UUID, "4323 AFTER PAIR",
            )

            # === STEP 4: Raw handshake — write 4324 nonce to 4322 ===
            _LOGGER.warning("=== STEP 4: Raw nonce handshake ===")
            if nonce_4324 and len(nonce_4324) > 0:
                _LOGGER.warning(
                    "Writing RAW 4324 nonce to 4322 (%d bytes): %s",
                    len(nonce_4324), nonce_4324.hex(),
                )
                try:
                    await self.api.write_message(nonce_4324)
                    _LOGGER.warning("Raw nonce write to 4322 succeeded")
                except JouleBLEError as err:
                    _LOGGER.warning("Raw nonce write failed: %s", err)

                await self._read_and_log(
                    SUBSCRIBE_CHAR_UUID, "4325 after raw nonce",
                )
                await self._read_and_log(
                    READ_CHAR_UUID, "4323 after raw nonce",
                )

                # Check if 4324 nonce changed after writing it back
                new_4324 = await self._read_and_log(
                    char_4324, "4324 after raw nonce write",
                )

                # Wait 1s and check again
                await asyncio.sleep(1)
                await self._read_and_log(
                    SUBSCRIBE_CHAR_UUID, "4325 1s after raw nonce",
                )
                await self._read_and_log(
                    READ_CHAR_UUID, "4323 1s after raw nonce",
                )

            # === STEP 5: Write by HANDLE instead of UUID ===
            _LOGGER.warning("=== STEP 5: Write Ping by handle 16 ===")
            from .joule_proto import StreamMessage, Ping, encode_stream_message

            recipient = self.api.recipient_address
            payload_ping = encode_stream_message(StreamMessage(
                handle=0, end=False,
                recipient_address=recipient,
                ping=Ping(),
            ))
            _LOGGER.warning(
                "WRITE Ping by HANDLE 16 (%d bytes): %s",
                len(payload_ping), payload_ping.hex(),
            )
            try:
                await self.api._client.write_gatt_char(
                    16, bytearray(payload_ping), response=True,
                )
                _LOGGER.warning("Write Ping by handle 16 succeeded")
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("Write Ping by handle 16 failed: %s", err)

            await self._read_and_log(
                SUBSCRIBE_CHAR_UUID, "4325 after handle-write Ping",
            )
            await self._read_and_log(
                READ_CHAR_UUID, "4323 after handle-write Ping",
            )

            # === STEP 6: Subscribe to Service Changed indications ===
            _LOGGER.warning("=== STEP 6: Service Changed indications ===")
            try:
                svc_changed_uuid = "00002a05-0000-1000-8000-00805f9b34fb"
                await self.api._client.start_notify(
                    svc_changed_uuid,
                    lambda c, d: _LOGGER.warning(
                        "SERVICE CHANGED indication: %d bytes: %s",
                        len(d), d.hex(),
                    ),
                )
                _LOGGER.warning("Subscribed to Service Changed (handle 10)")
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("Service Changed subscribe failed: %s", err)

            # === STEP 7: Write raw single-byte probes to 4322 ===
            _LOGGER.warning("=== STEP 7: Raw byte probes ===")
            for raw_byte, desc in [
                (b"\x00", "0x00"),
                (b"\x01", "0x01"),
                (b"\x08\x01", "varint field1=1"),
            ]:
                _LOGGER.warning("Writing raw %s to 4322", desc)
                try:
                    await self.api.write_message(raw_byte)
                    _LOGGER.warning("Raw %s write succeeded", desc)
                    r = await self._read_and_log(
                        SUBSCRIBE_CHAR_UUID, f"4325 after raw {desc}",
                    )
                    await self._read_and_log(
                        READ_CHAR_UUID, f"4323 after raw {desc}",
                    )
                except JouleBLEError as err:
                    _LOGGER.warning("Raw %s write failed: %s", desc, err)

        except JouleBLEError as err:
            raise UpdateFailed(f"BLE communication failed: {err}") from err

        current_temperature: float = 0.0
        if self._latest_data_point is not None:
            current_temperature = self._latest_data_point.bath_temp

            step = self._latest_data_point.program_step
            if step in (
                ProgramStep.PRE_HEAT,
                ProgramStep.WAIT_FOR_FOOD,
                ProgramStep.COOK,
            ):
                self._is_cooking = True
            elif step in (ProgramStep.UNKNOWN, ProgramStep.WAIT_FOR_REMOVE_FOOD):
                self._is_cooking = False

        return {
            "current_temperature": current_temperature,
            "is_cooking": self._is_cooking,
            "target_temperature": self._target_temperature,  # °C
            "cook_time_minutes": self._cook_time_minutes,
            "temperature_unit": self._temperature_unit,
        }

    async def async_start_cooking(
        self, target_temperature: float, cook_time_minutes: float
    ) -> None:
        """Send a protobuf StartProgramRequest to the device."""
        self._target_temperature = target_temperature
        self._cook_time_minutes = cook_time_minutes
        try:
            await self.api.ensure_connected()
            cook_time_seconds = int(cook_time_minutes * 60)
            payload = build_start_cook_message(
                target_temperature,
                cook_time_seconds,
                sender=self.api.sender_address,
                recipient=self.api.recipient_address,
            )
            await self.api.write_message(payload)
        except JouleBLEError as err:
            raise HomeAssistantError(f"Failed to start cooking: {err}") from err

        self._is_cooking = True
        await self.async_refresh()

    async def async_set_target_temperature(self, value_celsius: float) -> None:
        """Update the target temperature (always in °C) without starting a cook."""
        self._target_temperature = value_celsius
        await self.async_refresh()

    async def async_set_temperature_unit(self, unit: str) -> None:
        """Update the display unit preference and persist it to the config entry."""
        self._temperature_unit = unit
        self.hass.config_entries.async_update_entry(
            self._entry,
            options={**self._entry.options, "temperature_unit": unit},
        )
        await self.async_refresh()

    async def async_set_cook_time(self, value: float) -> None:
        """Update the cook time without starting a cook."""
        self._cook_time_minutes = value
        await self.async_refresh()

    async def async_stop_cooking(self) -> None:
        """Send a protobuf StopCirculatorRequest to the device."""
        try:
            await self.api.ensure_connected()
            payload = build_stop_cook_message(
                sender=self.api.sender_address,
                recipient=self.api.recipient_address,
            )
            await self.api.write_message(payload)
        except JouleBLEError as err:
            raise HomeAssistantError(f"Failed to stop cooking: {err}") from err

        self._is_cooking = False
        await self.async_refresh()
