"""Config flow for the Joule Sous Vide integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .breville_cloud import (
    BrevilleApplianceNotFound,
    BrevilleCloudAuthError,
    BrevilleCloudError,
    async_fetch_breville_ble_auth_key,
)
from .const import (
    CONF_BLE_AUTH_KEY,
    CONF_BREVILLE_EMAIL,
    CONF_BREVILLE_PASSWORD,
    CONF_BREVILLE_SERIAL_NUMBER,
    CONF_MAC_ADDRESS,
    DOMAIN,
)
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
    """Handle options for the Joule integration."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Show the auth key input form."""
        errors: dict[str, str] = {}

        if user_input is not None:
            raw_key = user_input.get(CONF_BLE_AUTH_KEY, "").strip()
            email = user_input.get(CONF_BREVILLE_EMAIL, "").strip()
            password = user_input.get(CONF_BREVILLE_PASSWORD, "")
            serial_number = user_input.get(CONF_BREVILLE_SERIAL_NUMBER, "").strip()
            cloud_requested = bool(email or password)
            options = {**self._entry.options}

            if cloud_requested:
                if not email or not password:
                    errors["base"] = "missing_breville_credentials"
                else:
                    try:
                        result = await async_fetch_breville_ble_auth_key(
                            async_get_clientsession(self.hass),
                            email=email,
                            password=password,
                            circulator_id=self._target_circulator_id(),
                            serial_number=serial_number or None,
                        )
                    except BrevilleCloudAuthError:
                        errors["base"] = "invalid_breville_auth"
                    except BrevilleApplianceNotFound:
                        errors["base"] = "breville_appliance_not_found"
                    except BrevilleCloudError:
                        _LOGGER.exception("Breville cloud import failed")
                        errors["base"] = "breville_cloud_error"
                    else:
                        options[CONF_BLE_AUTH_KEY] = result.auth_key.hex()
                        options[CONF_BREVILLE_SERIAL_NUMBER] = result.serial_number
            elif raw_key:
                try:
                    bytes.fromhex(raw_key)
                except ValueError:
                    errors[CONF_BLE_AUTH_KEY] = "invalid_auth_key"
                else:
                    options[CONF_BLE_AUTH_KEY] = raw_key
                    if serial_number:
                        options[CONF_BREVILLE_SERIAL_NUMBER] = serial_number
            else:
                options[CONF_BLE_AUTH_KEY] = None
                if serial_number:
                    options[CONF_BREVILLE_SERIAL_NUMBER] = serial_number

            if not errors:
                return self.async_create_entry(
                    title="",
                    data=options,
                )

        current_key = self._entry.options.get(CONF_BLE_AUTH_KEY, "")
        current_serial = self._entry.options.get(CONF_BREVILLE_SERIAL_NUMBER, "")
        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_BLE_AUTH_KEY,
                    default=current_key or "",
                ): str,
                vol.Optional(CONF_BREVILLE_EMAIL, default=""): str,
                vol.Optional(CONF_BREVILLE_PASSWORD): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.PASSWORD
                    )
                ),
                vol.Optional(
                    CONF_BREVILLE_SERIAL_NUMBER,
                    default=current_serial or "",
                ): str,
            }
        )

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
        )

    def _target_circulator_id(self) -> str | None:
        """Return the advertised Joule protocol address if HA has seen it."""
        try:
            api = JouleBLEAPI(self.hass, self._entry.data[CONF_MAC_ADDRESS])
        except Exception:  # noqa: BLE001
            _LOGGER.debug("Could not derive Joule circulator id from BLE cache")
            return None
        return api.recipient_address.hex()
