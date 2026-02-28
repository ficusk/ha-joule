"""The Joule Sous Vide integration."""
from __future__ import annotations

import logging
import shutil
from pathlib import Path

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, Platform
from homeassistant.core import Event, HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import DOMAIN
from .coordinator import JouleCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.NUMBER, Platform.SELECT, Platform.SENSOR, Platform.SWITCH]

LOVELACE_CARD_URL = f"/{DOMAIN}/joule-card.js"
LOVELACE_LOCAL_URL = "/local/joule-sous-vide-card.js"
LOVELACE_CARD_PATH = Path(__file__).parent / "www" / "joule-card.js"


def _copy_card_to_www(hass: HomeAssistant) -> None:
    """Copy the card JS to {config}/www/ so it is served at /local/."""
    www_dir = Path(hass.config.path("www"))
    www_dir.mkdir(exist_ok=True)
    dest = www_dir / "joule-sous-vide-card.js"
    if LOVELACE_CARD_PATH.is_file():
        shutil.copy2(str(LOVELACE_CARD_PATH), str(dest))


async def _register_lovelace_resource(hass: HomeAssistant) -> None:
    """Add the card JS as a Lovelace resource if not already present."""
    try:
        resources = hass.data["lovelace"]["resources"]
        if resources is None:
            return
    except (KeyError, TypeError):
        return

    if not resources.loaded:
        await resources.async_load()
        resources.loaded = True

    for item in resources.async_items():
        if item.get("url") == LOVELACE_LOCAL_URL:
            return

    await resources.async_create_item(
        {"res_type": "module", "url": LOVELACE_LOCAL_URL}
    )
    _LOGGER.info("Registered Lovelace resource %s", LOVELACE_LOCAL_URL)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Copy the card to www/ and register it as a Lovelace resource."""
    # Copy the card JS to {config}/www/ so it is always available at
    # /local/joule-sous-vide-card.js regardless of integration state.
    await hass.async_add_executor_job(_copy_card_to_www, hass)

    # Defer resource registration until HA is fully started so the Lovelace
    # resource collection is loaded and the duplicate check is reliable.
    async def _on_ha_started(event: Event) -> None:
        await _register_lovelace_resource(hass)

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _on_ha_started)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Joule Sous Vide from a config entry."""
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
        await coordinator.api.disconnect()

    return unload_ok
