"""Data update coordinator for the Joule Sous Vide integration."""
from __future__ import annotations

import asyncio
from datetime import timedelta
import logging
import random
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    CONF_BLE_AUTH_KEY,
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
    build_compact_start_cook_message,
    build_identify_circulator_message,
    build_live_feed_message,
    build_start_cook_message,
    build_start_key_exchange_message,
    build_stop_cook_message,
    build_submit_key_message,
    decode_stream_message,
)

_LOGGER = logging.getLogger(__name__)


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
        self._session_handle: int = self._new_session_handle()
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

    @staticmethod
    def _new_session_handle() -> int:
        """Generate a random session handle (matches [redacted] SDK behavior).

        The SDK creates one handle per session and reuses it for all messages.
        The Joule tracks per-handle sessions — commands like StartProgramRequest
        must arrive on the same handle that was authenticated.
        """
        return random.randint(1, 2**31 - 1)

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

    def _persist_auth_key(self, key: bytes) -> None:
        """Save the BLE auth key to config entry options for future connections."""
        _LOGGER.warning("Persisting BLE auth key: %s", key.hex())
        self.hass.config_entries.async_update_entry(
            self._entry,
            options={**self._entry.options, CONF_BLE_AUTH_KEY: key.hex()},
        )

    async def _safe_write(
        self, label: str, payload: bytes, *, response: bool = True,
    ) -> bool:
        """Write a payload to 4322 with error handling. Returns True on success."""
        self._notification_received.clear()
        try:
            async with asyncio.timeout(5):
                if response:
                    await self.api.write_message(payload)
                else:
                    await self.api.write_message_no_response(payload)
            _LOGGER.warning("Write OK: %s (resp=%s)", label, response)
            return True
        except (TimeoutError, JouleBLEError) as err:
            _LOGGER.warning("Write FAIL: %s: %s", label, err)
            return False

    async def _try_write_and_wait(
        self, label: str, payload: bytes, timeout: float,
        *, no_response: bool = False,
    ) -> bool:
        """Write a payload to 4322 and wait for data.

        When no_response=False (default), uses write-with-response (ATT Write
        Request).  When no_response=True, uses write-without-response (ATT
        Write Command).  After the write, waits for a notification on 4325
        (triggers a read from 4323).  Polls 4323 every 5 seconds as keepalive.
        Returns True on response.
        """
        _LOGGER.warning(
            "%s (%d bytes): %s", label, len(payload), payload.hex(),
        )
        self._notification_received.clear()

        try:
            async with asyncio.timeout(10):
                if no_response:
                    await self.api.write_message_no_response(payload)
                else:
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

    async def _key_exchange(self) -> bool:
        """Perform application-level key exchange with the Joule.

        The Joule uses its own key exchange (NOT OS-level BLE pairing).
        The user must press the button on top of the Joule within 60 seconds.

        v0.9.9 revealed that compact messages missing proto2 `required` fields
        (senderAddress, recipientAddress) are silently rejected by the Joule's
        nanopb parser.  This version uses encode_stream_message() which always
        includes those fields — even when empty — satisfying proto2 validation.

        Message variants (all include required fields 5 and 6):
        - empty addrs (14B): fits in 20-byte ATT payload at MTU=23
        - empty sender + 6-byte MAC recipient (20B): fits exactly
        - full 8-byte addrs (34B): via Long Write (response=True)
        """
        from homeassistant.components.persistent_notification import (
            async_create,
            async_dismiss,
        )
        from .joule_ble import mac_to_bytes
        from .joule_proto import (
            StreamMessage,
            StartKeyExchangeRequest,
            encode_stream_message,
        )

        mtu_payload = self.api.mtu_size - 3 if self.api.mtu_size > 3 else 20
        _LOGGER.warning("Effective ATT payload: %d bytes", mtu_payload)

        async_create(
            self.hass,
            "**Press the button on top of your Joule** to complete pairing.\n\n"
            "You have 60 seconds.",
            title="Joule Pairing Required",
            notification_id="joule_sous_vide_pairing",
        )

        try:
            mac_6 = mac_to_bytes(self.api.mac_address)

            # --- Variant A: empty addresses (12 bytes) ---
            # All required fields present; addresses are zero-length bytes.
            # Session handle + no end field (matches iOS app capture).
            msg_empty = StreamMessage(
                handle=self._session_handle,
                sender_address=b"",
                recipient_address=b"",
                start_key_exchange_request=StartKeyExchangeRequest(),
            )
            payload_empty = encode_stream_message(msg_empty)
            _LOGGER.warning(
                "KE-empty-addrs (%d bytes): %s",
                len(payload_empty), payload_empty.hex(),
            )
            await self._safe_write("KE-empty-WR", payload_empty, response=True)
            await self._safe_write("KE-empty-WC", payload_empty, response=False)

            # --- Variant B: empty sender + 6-byte MAC recipient ---
            msg_mac6 = StreamMessage(
                handle=self._session_handle,
                sender_address=b"",
                recipient_address=mac_6,
                start_key_exchange_request=StartKeyExchangeRequest(),
            )
            payload_mac6 = encode_stream_message(msg_mac6)
            _LOGGER.warning(
                "KE-mac6 (%d bytes): %s",
                len(payload_mac6), payload_mac6.hex(),
            )
            await self._safe_write("KE-mac6-WR", payload_mac6, response=True)

            # --- Variant C: full format with 8-byte padded addresses ---
            full_payload = build_start_key_exchange_message(
                sender=self.api.sender_address,
                recipient=self.api.recipient_address,
                handle=self._session_handle,
            )
            _LOGGER.warning(
                "KE-full (%d bytes, MTU payload=%d): %s",
                len(full_payload), mtu_payload, full_payload.hex(),
            )
            await self._safe_write(
                "KE-full-longwrite", full_payload, response=True,
            )

            # --- Wait for any response ---
            _LOGGER.warning(
                "All KE variants sent — waiting %.0fs for response "
                "(PRESS JOULE BUTTON NOW!)",
                self.KEY_EXCHANGE_TIMEOUT,
            )
            got_reply = await self._try_write_and_wait(
                "KE-wait",
                payload_empty,
                self.KEY_EXCHANGE_TIMEOUT,
            )

            if got_reply and self._auth_key is not None:
                _LOGGER.warning("Got secret key! Submitting...")
                return await self._submit_key(self._auth_key)

            _LOGGER.warning(
                "Key exchange timed out — no variant got a response"
            )
            return False

        finally:
            async_dismiss(self.hass, "joule_sous_vide_pairing")

    async def _submit_key(self, key: bytes) -> bool:
        """Submit a secret key to authenticate with the Joule.

        Tries empty addresses first (matching the iOS app capture), then
        falls back to full addresses if no response.

        Returns True if the device accepted the key.
        """
        _LOGGER.warning("Submitting auth key: %s", key.hex())

        # Variant 1: empty addresses (matches iOS app exactly — 26 bytes)
        payload_empty = build_submit_key_message(
            secret_key=key,
            sender=b"",
            recipient=b"",
            handle=self._session_handle,
        )
        got_reply = await self._try_write_and_wait(
            "SubmitKey-empty-addrs", payload_empty, self.NOTIFICATION_TIMEOUT,
        )
        if got_reply and self._authenticated:
            _LOGGER.warning("Authentication successful (empty addrs)!")
            return True

        # Variant 2: full addresses (original behavior — 42 bytes, Long Write)
        payload_full = build_submit_key_message(
            secret_key=key,
            sender=self.api.sender_address,
            recipient=self.api.recipient_address,
            handle=self._session_handle,
        )
        got_reply = await self._try_write_and_wait(
            "SubmitKey-full-addrs", payload_full, self.NOTIFICATION_TIMEOUT,
        )
        if got_reply and self._authenticated:
            _LOGGER.warning("Authentication successful (full addrs)!")
            return True

        _LOGGER.warning("SubmitKeyRequest failed — no variant got a response")
        return False

    async def _async_update_data(self) -> dict[str, Any]:
        """Poll the Joule for current state.

        Authentication flow (first connection, no stored key):
          1. Subscribe to notifications on 4325
          2. Send StartKeyExchangeRequest — user presses Joule button
          3. Receive StartKeyExchangeReply with secret key
          4. Send SubmitKeyRequest with the key
          5. Receive SubmitKeyReply — now authorized

        Reconnection with stored key:
          1. Subscribe to notifications on 4325
          2. Send SubmitKeyRequest with stored key
          3. Receive SubmitKeyReply — now authorized

        After authentication:
          - Send BeginLiveFeed to request streaming data
          - Joule responds with CirculatorDataPoint via notifications
        """
        try:
            reconnected = await self.api.ensure_connected()
            if reconnected:
                self._session_handle = self._new_session_handle()
                _LOGGER.warning(
                    "Fresh BLE connection — new session handle=%08x",
                    self._session_handle,
                )
                self._subscribed = False
                self._authenticated = False

            # Step 1: Subscribe to notifications on 4325
            if not self._subscribed:
                # Enable Service Changed indications first (required by Joule
                # firmware — the official iOS app does this before anything else)
                sc_ok = await self.api.enable_service_changed_indications()
                _LOGGER.warning(
                    "Service Changed indications: %s",
                    "enabled" if sc_ok else "not available",
                )

                # Brief delay after Service Changed to let firmware process
                # the indication registration before we send commands
                if sc_ok:
                    await asyncio.sleep(0.5)

                await self.api.subscribe(self._on_notification)
                self._subscribed = True
                _LOGGER.warning("Subscribed to 4325 notifications")

                # Verify CCCD was actually written
                cccd_ok = await self.api.verify_and_enable_notifications()
                _LOGGER.warning("CCCD verification: %s", "OK" if cccd_ok else "FAILED")

                # Log MTU after subscribe — start_notify may have triggered
                # MTU exchange via AcquireNotify
                _LOGGER.warning(
                    "MTU after subscribe: %d", self.api.mtu_size,
                )

                # Read 4323 to clear any stale buffer — the official app
                # always reads 4323 on notification; if the device waits for
                # us to consume stale data before accepting commands, this
                # unblocks it.
                stale = await self.api.read_characteristic(READ_CHAR_UUID)
                _LOGGER.warning(
                    "Initial 4323 read (clear buffer): %s",
                    stale.hex() if stale else "empty",
                )

            # Step 2: Authenticate if needed
            if not self._authenticated:
                if self._auth_key is not None:
                    _LOGGER.warning("Have stored auth key — submitting")
                    authenticated = await self._submit_key(self._auth_key)
                    if not authenticated:
                        _LOGGER.warning(
                            "Stored key rejected — starting fresh key exchange"
                        )
                        self._auth_key = None
                        authenticated = await self._key_exchange()
                else:
                    _LOGGER.warning(
                        "No auth key — starting key exchange "
                        "(press Joule button!)"
                    )
                    authenticated = await self._key_exchange()

                if not authenticated:
                    _LOGGER.warning(
                        "Authentication failed — will retry next poll"
                    )

            # Step 3: Request live data feed
            if self._authenticated:
                payload = build_live_feed_message(
                    sender=self.api.sender_address,
                    recipient=self.api.recipient_address,
                    handle=self._session_handle,
                )
                await self._try_write_and_wait(
                    "BeginLiveFeed", payload, self.NOTIFICATION_TIMEOUT,
                )

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
        """Send a protobuf StartProgramRequest to the device.

        At MTU=23 (20-byte ATT payload), messages >20 bytes require the BLE
        Long Write procedure.  Auth messages (~24 bytes, 2 Long Write chunks)
        work fine.  The full iOS-matching cook message is 68 bytes (4 chunks)
        and has never worked — BlueZ may handle 4-chunk Long Writes differently
        from CoreBluetooth.

        Strategy:
          1. Send IdentifyCirculatorRequest (12 bytes, single ATT write) —
             the iOS app sends this before cook commands
          2. Try a compact cook message (~21–30 bytes, ≤2 Long Write chunks)
             matching the [redacted] SDK structure
          3. Fall back to the full 68-byte iOS-matching message
        """
        self._target_temperature = target_temperature
        self._cook_time_minutes = cook_time_minutes
        try:
            await self.api.ensure_connected()
            cook_time_seconds = int(cook_time_minutes * 60)

            feed_id = 0
            seq_num = 0
            if self._latest_data_point is not None:
                feed_id = self._latest_data_point.feed_id
                seq_num = self._latest_data_point.sequence_number
            _LOGGER.warning(
                "StartProgramRequest: temp=%.1f°C cook_time=%ds "
                "feed_id=%d seq=%d handle=%08x",
                target_temperature, cook_time_seconds,
                feed_id, seq_num, self._session_handle,
            )

            # Pre-cook: IdentifyCirculatorRequest (iOS does this before cook)
            identify_payload = build_identify_circulator_message(
                sender=b"",
                recipient=b"",
                handle=self._session_handle,
            )
            _LOGGER.warning(
                "Pre-cook IdentifyCirculatorRequest (%d bytes): %s",
                len(identify_payload), identify_payload.hex(),
            )
            await self._safe_write("IdentifyCirculator", identify_payload)
            await asyncio.sleep(0.5)

            # Attempt 1: compact message (~21–30 bytes, ≤2 Long Write chunks)
            # Omits ProgramMetadata + field 7 that aren't in [redacted] SDK.
            compact_payload = build_compact_start_cook_message(
                target_temperature,
                sender=b"",
                recipient=b"",
                handle=self._session_handle,
                feed_id=feed_id,
                sequence_number=seq_num,
            )
            _LOGGER.warning(
                "Compact StartProgramRequest (%d bytes): %s",
                len(compact_payload), compact_payload.hex(),
            )
            got_reply = await self._try_write_and_wait(
                "StartProgram-compact", compact_payload, 5.0,
            )
            if got_reply:
                _LOGGER.warning(
                    "Compact cook message got a response! "
                    "Long Write 4-chunk theory confirmed."
                )
            else:
                _LOGGER.warning(
                    "No response to compact cook — trying full 68-byte message"
                )

            # Attempt 2: full iOS-matching message (68 bytes, 4 Long Write chunks)
            full_payload = build_start_cook_message(
                target_temperature,
                cook_time_seconds,
                sender=b"",
                recipient=b"",
                handle=self._session_handle,
                feed_id=feed_id,
                sequence_number=seq_num,
            )
            _LOGGER.warning(
                "Full StartProgramRequest (%d bytes): %s",
                len(full_payload), full_payload.hex(),
            )
            got_reply = await self._try_write_and_wait(
                "StartProgram-full", full_payload, 5.0,
            )
            if got_reply:
                _LOGGER.warning("Full cook message got a response!")
        except JouleBLEError as err:
            raise HomeAssistantError(f"Failed to start cooking: {err}") from err

        self._is_cooking = True
        self._publish_state()

    def _publish_state(self) -> None:
        """Push current state to entities without triggering a full poll.

        Used after cook commands to reflect optimistic state immediately.
        A full _async_update_data() poll would read stale CirculatorDataPoint
        data (still step=UNKNOWN) and override the is_cooking flag.
        """
        current_temp = 0.0
        if self._latest_data_point is not None:
            current_temp = self._latest_data_point.bath_temp
        self.async_set_updated_data({
            "current_temperature": current_temp,
            "is_cooking": self._is_cooking,
            "target_temperature": self._target_temperature,
            "cook_time_minutes": self._cook_time_minutes,
            "temperature_unit": self._temperature_unit,
        })

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

            feed_id = 0
            seq_num = 0
            if self._latest_data_point is not None:
                feed_id = self._latest_data_point.feed_id
                seq_num = self._latest_data_point.sequence_number

            payload = build_stop_cook_message(
                sender=b"",
                recipient=b"",
                handle=self._session_handle,
                feed_id=feed_id,
                sequence_number=seq_num,
            )
            _LOGGER.warning(
                "StopCirculatorRequest (%d bytes): %s",
                len(payload), payload.hex(),
            )
            await self.api.write_message(payload)
        except JouleBLEError as err:
            raise HomeAssistantError(f"Failed to stop cooking: {err}") from err

        self._is_cooking = False
        self._publish_state()
