"""Config flow for the Joule Sous Vide integration."""
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components.bluetooth import (
    BluetoothServiceInfoBleak,
    async_discovered_service_info,
)
from homeassistant.core import HomeAssistant
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
    JOULE_MANUFACTURER_ID,
    JOULE_SERVICE_UUID,
)
from .joule_ble import JouleBLEAPI, JouleBLEError

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_MAC_ADDRESS): str,
    }
)


def _is_joule_advertisement(discovery_info: BluetoothServiceInfoBleak) -> bool:
    """Return True if a Bluetooth advertisement looks like a Joule."""
    name = (discovery_info.name or "").lower()
    service_uuids = {uuid.lower() for uuid in discovery_info.service_uuids}
    return (
        "joule" in name
        or JOULE_MANUFACTURER_ID in discovery_info.manufacturer_data
        or JOULE_SERVICE_UUID.lower() in service_uuids
    )


def _discovery_title(discovery_info: BluetoothServiceInfoBleak) -> str:
    """Return a readable title for a discovered Joule."""
    name = discovery_info.name or "Joule"
    return f"{name} ({discovery_info.address})"


class JouleConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the user-facing configuration flow.

    Discovers nearby Joules from Home Assistant's Bluetooth scanner and
    validates the selected device by attempting a real BLE connection before
    creating the config entry.
    """

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._mac_address: str | None = None
        self._discovery_info: BluetoothServiceInfoBleak | None = None
        self._discovered_devices: dict[str, BluetoothServiceInfoBleak] = {}

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> JouleOptionsFlow:
        """Return the options flow handler."""
        return JouleOptionsFlow(config_entry)

    async def async_step_bluetooth(
        self, discovery_info: BluetoothServiceInfoBleak
    ) -> FlowResult:
        """Handle a Bluetooth-discovered Joule."""
        if not _is_joule_advertisement(discovery_info):
            return self.async_abort(reason="not_supported")

        await self.async_set_unique_id(discovery_info.address)
        self._abort_if_unique_id_configured()
        self._discovery_info = discovery_info
        self.context["title_placeholders"] = {
            "name": _discovery_title(discovery_info)
        }
        return await self.async_step_bluetooth_confirm()

    async def async_step_bluetooth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Confirm adding a Bluetooth-discovered Joule."""
        assert self._discovery_info is not None
        if user_input is not None:
            return await self._async_validate_and_create_entry(
                self._discovery_info.address
            )

        self._set_confirm_only()
        return self.async_show_form(
            step_id="bluetooth_confirm",
            description_placeholders={
                "name": _discovery_title(self._discovery_info)
            },
        )

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Show discovered Joules or a manual address fallback."""
        errors: dict[str, str] = {}

        if user_input is not None:
            mac_address = user_input[CONF_MAC_ADDRESS]
            return await self._async_validate_and_create_entry(mac_address, errors)

        data_schema = self._async_discovered_device_schema()

        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            errors=errors,
        )

    async def _async_validate_and_create_entry(
        self,
        mac_address: str,
        errors: dict[str, str] | None = None,
    ) -> FlowResult:
        """Validate a Joule address and create the config entry."""
        errors = errors if errors is not None else {}
        await self.async_set_unique_id(mac_address, raise_on_progress=False)
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
            self._mac_address = mac_address
            return await self.async_step_breville()

        return self.async_show_form(
            step_id="user",
            data_schema=self._async_discovered_device_schema(),
            errors=errors,
        )

    async def async_step_breville(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Optionally import the BLE auth key from Breville+ during setup."""
        assert self._mac_address is not None
        errors: dict[str, str] = {}

        if user_input is not None:
            email = user_input.get(CONF_BREVILLE_EMAIL, "").strip()
            password = user_input.get(CONF_BREVILLE_PASSWORD, "")
            serial_number = user_input.get(CONF_BREVILLE_SERIAL_NUMBER, "").strip()
            options: dict[str, Any] = {}

            if not email and not password and not serial_number:
                return self._create_entry(options)

            if not email or not password:
                errors["base"] = "missing_breville_credentials"
            else:
                try:
                    result = await async_fetch_breville_ble_auth_key(
                        async_get_clientsession(self.hass),
                        email=email,
                        password=password,
                        circulator_id=_target_circulator_id(
                            self.hass, self._mac_address
                        ),
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
                    return self._create_entry(options)

        schema = vol.Schema(
            {
                vol.Optional(CONF_BREVILLE_EMAIL, default=""): str,
                vol.Optional(CONF_BREVILLE_PASSWORD): selector.TextSelector(
                    selector.TextSelectorConfig(
                        type=selector.TextSelectorType.PASSWORD
                    )
                ),
                vol.Optional(CONF_BREVILLE_SERIAL_NUMBER, default=""): str,
            }
        )

        return self.async_show_form(
            step_id="breville",
            data_schema=schema,
            errors=errors,
        )

    def _create_entry(self, options: dict[str, Any]) -> FlowResult:
        """Create the config entry for the selected Joule."""
        assert self._mac_address is not None
        return self.async_create_entry(
            title=f"Joule {self._mac_address}",
            data={CONF_MAC_ADDRESS: self._mac_address},
            options=options,
        )

    def _async_discovered_device_schema(self) -> vol.Schema:
        """Build a schema from currently discovered Joules, with manual fallback."""
        current_addresses = self._async_current_ids()
        if discovery := self._discovery_info:
            self._discovered_devices[discovery.address] = discovery

        try:
            discoveries = async_discovered_service_info(self.hass, connectable=True)
        except RuntimeError:
            _LOGGER.debug("Home Assistant Bluetooth discovery is not ready")
            discoveries = []

        for discovery in discoveries:
            if (
                discovery.address in current_addresses
                or discovery.address in self._discovered_devices
                or not _is_joule_advertisement(discovery)
            ):
                continue
            self._discovered_devices[discovery.address] = discovery

        if not self._discovered_devices:
            return STEP_USER_DATA_SCHEMA

        return vol.Schema(
            {
                vol.Required(CONF_MAC_ADDRESS): vol.In(
                    {
                        address: _discovery_title(discovery)
                        for address, discovery in self._discovered_devices.items()
                    }
                )
            }
        )


def _target_circulator_id(hass: HomeAssistant, mac_address: str) -> str | None:
    """Return the advertised Joule protocol address if HA has seen it."""
    try:
        api = JouleBLEAPI(hass, mac_address)
    except Exception:  # noqa: BLE001
        _LOGGER.debug("Could not derive Joule circulator id from BLE cache")
        return None
    return api.recipient_address.hex()


class JouleOptionsFlow(config_entries.OptionsFlow):
    """Handle options for the Joule integration."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Show options menu."""
        return self.async_show_menu(
            step_id="init",
            menu_options=["breville", "manual_key"],
        )

    async def async_step_breville(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Import the BLE auth key from Breville+."""
        errors: dict[str, str] = {}

        if user_input is not None:
            email = user_input.get(CONF_BREVILLE_EMAIL, "").strip()
            password = user_input.get(CONF_BREVILLE_PASSWORD, "")
            serial_number = user_input.get(CONF_BREVILLE_SERIAL_NUMBER, "").strip()
            options = {**self._entry.options}

            if not email or not password:
                errors["base"] = "missing_breville_credentials"
            else:
                try:
                    result = await async_fetch_breville_ble_auth_key(
                        async_get_clientsession(self.hass),
                        email=email,
                        password=password,
                        circulator_id=_target_circulator_id(
                            self.hass, self._entry.data[CONF_MAC_ADDRESS]
                        ),
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

            if not errors:
                return self.async_create_entry(
                    title="",
                    data=options,
                )

        current_serial = self._entry.options.get(CONF_BREVILLE_SERIAL_NUMBER, "")
        schema = vol.Schema(
            {
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
            step_id="breville",
            data_schema=schema,
            errors=errors,
        )

    async def async_step_manual_key(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Allow manually pasting or clearing a BLE auth key."""
        errors: dict[str, str] = {}

        if user_input is not None:
            raw_key = user_input.get(CONF_BLE_AUTH_KEY, "").strip()
            options = {**self._entry.options}
            if raw_key:
                try:
                    bytes.fromhex(raw_key)
                except ValueError:
                    errors[CONF_BLE_AUTH_KEY] = "invalid_auth_key"
                else:
                    options[CONF_BLE_AUTH_KEY] = raw_key
            else:
                options[CONF_BLE_AUTH_KEY] = None

            if not errors:
                return self.async_create_entry(title="", data=options)

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
            step_id="manual_key",
            data_schema=schema,
            errors=errors,
        )
