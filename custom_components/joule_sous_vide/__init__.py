"""The Joule Sous Vide integration."""
from __future__ import annotations

import logging
from pathlib import Path

from homeassistant.components.http import StaticPathConfig
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, Platform
from homeassistant.core import Event, HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import DOMAIN
from .coordinator import JouleCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.NUMBER, Platform.SELECT, Platform.SENSOR, Platform.SWITCH]

LOVELACE_CARD_URL = f"/{DOMAIN}/joule-card.js"
LOVELACE_CARD_PATH = str(Path(__file__).parent / "www" / "joule-card.js")


async def _register_lovelace_resource(hass: HomeAssistant) -> None:
    """Add the card JS as a Lovelace resource if not already present.

    Must be called after the Lovelace resource collection has been loaded
    from disk (i.e. after EVENT_HOMEASSISTANT_STARTED).
    """
    try:
        resources = hass.data["lovelace"]["resources"]
        if resources is None:
            # YAML mode — user manages resources manually.
            _LOGGER.debug(
                "Lovelace is in YAML mode; skipping automatic resource registration"
            )
            return
    except (KeyError, TypeError):
        _LOGGER.debug("Lovelace resource collection not available")
        return

    # Ensure the collection is loaded so the duplicate check is reliable.
    if not resources.loaded:
        await resources.async_load()
        resources.loaded = True

    for item in resources.async_items():
        if item.get("url") == LOVELACE_CARD_URL:
            _LOGGER.debug("Lovelace resource %s already registered", LOVELACE_CARD_URL)
            return

    await resources.async_create_item({"res_type": "module", "url": LOVELACE_CARD_URL})
    _LOGGER.info("Registered Lovelace resource %s", LOVELACE_CARD_URL)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Register the Lovelace card as a static resource."""
    # Serve the JS file at /joule_sous_vide/joule-card.js.
    if hass.http is not None:
        if Path(LOVELACE_CARD_PATH).is_file():
            await hass.http.async_register_static_paths(
                [
                    StaticPathConfig(
                        LOVELACE_CARD_URL,
                        LOVELACE_CARD_PATH,
                        cache_headers=False,
                    )
                ]
            )
        else:
            _LOGGER.warning("Card JS not found at %s", LOVELACE_CARD_PATH)

    # Defer resource registration until HA is fully started so the Lovelace
    # resource collection has been loaded from disk and dedup works correctly.
    async def _on_ha_started(event: Event) -> None:
        await _register_lovelace_resource(hass)

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _on_ha_started)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Joule Sous Vide from a config entry.

    Creates the coordinator, performs the first BLE poll to verify the
    device is reachable, then forwards setup to the sensor and switch platforms.
    Raises ConfigEntryNotReady if the device cannot be reached so HA will
    retry automatically.
    """
    coordinator = JouleCoordinator(hass, entry)

    try:
        await coordinator.async_config_entry_first_refresh()
    except Exception as err:
        raise ConfigEntryNotReady(
            f"Cannot connect to Joule at {entry.data.get('mac_address')}: {err}"
        ) from err

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry and disconnect from the device."""
    coordinator: JouleCoordinator = hass.data[DOMAIN][entry.entry_id]

    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)
        await hass.async_add_executor_job(coordinator.api.disconnect)

    return unload_ok
