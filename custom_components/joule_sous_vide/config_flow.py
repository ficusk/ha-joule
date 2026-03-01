"""Config flow for the Joule Sous Vide integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult

from .const import CONF_BLE_AUTH_KEY, CONF_MAC_ADDRESS, DOMAIN
from .joule_ble import JouleBLEAPI, JouleBLEError

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_MAC_ADDRESS): str,
    }
)


class JouleConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the user-facing configuration flow.

    Prompts for the Joule's Bluetooth MAC address and validates it by
    attempting a real BLE connection before creating the config entry.
    """

    VERSION = 1

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> JouleOptionsFlow:
        """Return the options flow handler."""
        return JouleOptionsFlow(config_entry)

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Show the MAC address form and validate the connection."""
        errors: dict[str, str] = {}

        if user_input is not None:
            mac_address = user_input[CONF_MAC_ADDRESS]

            await self.async_set_unique_id(mac_address)
            self._abort_if_unique_id_configured()

            api = JouleBLEAPI(self.hass, mac_address)
            try:
                await api.connect()
                await api.disconnect()
            except JouleBLEError:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error connecting to Joule at %s", mac_address)
                errors["base"] = "unknown"
            else:
                return self.async_create_entry(
                    title=f"Joule {mac_address}",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )


class JouleOptionsFlow(config_entries.OptionsFlow):
    """Handle options for the Joule integration (auth key import)."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Show the auth key input form."""
        errors: dict[str, str] = {}

        if user_input is not None:
            raw_key = user_input.get(CONF_BLE_AUTH_KEY, "").strip()
            if raw_key:
                try:
                    bytes.fromhex(raw_key)
                except ValueError:
                    errors[CONF_BLE_AUTH_KEY] = "invalid_auth_key"

            if not errors:
                return self.async_create_entry(
                    title="",
                    data={
                        **self._entry.options,
                        CONF_BLE_AUTH_KEY: raw_key if raw_key else None,
                    },
                )

        current_key = self._entry.options.get(CONF_BLE_AUTH_KEY, "")
        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_BLE_AUTH_KEY,
                    default=current_key or "",
                ): str,
            }
        )

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
        )
