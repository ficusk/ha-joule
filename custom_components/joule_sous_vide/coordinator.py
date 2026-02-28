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
    FILE_CHAR_UUID,
    READ_CHAR_UUID,
    SUBSCRIBE_CHAR_UUID,
)
from .joule_ble import JouleBLEAPI, JouleBLEError
from .joule_proto import (
    CirculatorDataPoint,
    ProgramStep,
    build_identify_circulator_message,
    build_live_feed_message,
    build_start_cook_message,
    build_stop_cook_message,
    parse_notification,
)

_LOGGER = logging.getLogger(__name__)


class JouleCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Single owner of the BLE connection; provides data to all entities.

    Entities must not create their own BLE connections. They read exclusively
    from coordinator.data and call coordinator methods for control actions.

    Polling sends a BeginLiveFeedRequest and waits for a CirculatorDataPoint
    notification containing the current bath temperature and cooking state.
    """

    NOTIFICATION_TIMEOUT: float = 10.0  # seconds; overridden in tests

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

        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL),
        )

    def _on_notification(self, characteristic: Any, data: bytearray) -> None:
        """Handle a BLE notification from bleak (runs on the event loop)."""
        _LOGGER.warning(
            "NOTIFICATION on primary char: %d bytes, raw=%s",
            len(data),
            data.hex(),
        )
        data_point = parse_notification(bytes(data))
        if data_point is not None:
            _LOGGER.warning(
                "Parsed CirculatorDataPoint: bath_temp=%.2f, step=%s",
                data_point.bath_temp,
                data_point.program_step,
            )
            self._latest_data_point = data_point
            self._notification_received.set()
        else:
            _LOGGER.warning("Notification did not contain a CirculatorDataPoint")

    async def _try_write_and_wait(
        self, label: str, payload: bytes, timeout: float,
        *, use_file_char: bool = False,
    ) -> bool:
        """Write a payload and wait for a notification. Returns True if received."""
        _LOGGER.warning(
            "%s (%d bytes): %s", label, len(payload), payload.hex(),
        )
        self._notification_received.clear()
        if use_file_char:
            await self.api.write_to_file_char(payload)
        else:
            await self.api.write_message(payload)
        try:
            async with asyncio.timeout(timeout):
                await self._notification_received.wait()
            _LOGGER.warning("Got notification after %s!", label)
            return True
        except TimeoutError:
            _LOGGER.warning("No response to %s", label)
            return False

    async def _async_update_data(self) -> dict[str, Any]:
        """Poll the device for the current temperature.

        Subscribes to notifications on the first poll, then sends a series
        of command variants to trigger a CirculatorDataPoint notification.
        """
        try:
            reconnected = await self.api.ensure_connected()
            if reconnected:
                _LOGGER.warning("Fresh BLE connection — will re-subscribe")
                self._subscribed = False

            if not self._subscribed:
                _LOGGER.warning("Subscribing to notifications")
                await self.api.subscribe(self._on_notification)
                self._subscribed = True

            sender = self.api.sender_address
            recipient = self.api.recipient_address
            recipient_rev = self.api.recipient_address_reversed
            attempt_timeout = self.NOTIFICATION_TIMEOUT / 5

            # Attempt 1: IdentifyCirculatorRequest handshake (may be required
            # before the device responds to other commands)
            ident_payload = build_identify_circulator_message(
                sender=sender, recipient=recipient,
            )
            if await self._try_write_and_wait(
                "Attempt 1: IdentifyCirculatorRequest → 4322",
                ident_payload, attempt_timeout,
            ):
                pass  # got a notification, but still send LiveFeed below

            # Attempt 2: BeginLiveFeed with MSB addresses → 4322
            payload_msb = build_live_feed_message(
                sender=sender, recipient=recipient,
            )
            if await self._try_write_and_wait(
                "Attempt 2: BeginLiveFeed MSB addr → 4322",
                payload_msb, attempt_timeout,
            ):
                pass  # success

            if not self._notification_received.is_set():
                # Attempt 3: BeginLiveFeed with reversed (LSB) MAC → 4322
                payload_lsb = build_live_feed_message(
                    sender=sender, recipient=recipient_rev,
                )
                if await self._try_write_and_wait(
                    "Attempt 3: BeginLiveFeed LSB addr → 4322",
                    payload_lsb, attempt_timeout,
                ):
                    pass

            if not self._notification_received.is_set():
                # Attempt 4: BeginLiveFeed → 4326 FILE_CHAR (write-without-response)
                if await self._try_write_and_wait(
                    "Attempt 4: BeginLiveFeed → 4326 (WOR)",
                    payload_msb, attempt_timeout,
                    use_file_char=True,
                ):
                    pass

            if not self._notification_received.is_set():
                # Attempt 5: BeginLiveFeed with no addresses (minimal)
                payload_bare = build_live_feed_message(
                    sender=b"", recipient=b"",
                )
                if await self._try_write_and_wait(
                    "Attempt 5: BeginLiveFeed bare → 4322",
                    payload_bare, attempt_timeout,
                ):
                    pass

            if not self._notification_received.is_set():
                _LOGGER.warning("All 5 attempts failed — no notification received")

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
