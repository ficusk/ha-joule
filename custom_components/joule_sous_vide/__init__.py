"""The Joule Sous Vide integration."""
from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, Platform
from homeassistant.core import CoreState, Event, HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import DOMAIN
from .coordinator import JouleCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.NUMBER, Platform.SELECT, Platform.SENSOR, Platform.SWITCH]

LOVELACE_CARD_URL = f"/{DOMAIN}/joule-card.js"
_LOVELACE_LOCAL_BASE = "/local/joule-sous-vide-card.js"
LOVELACE_CARD_PATH = Path(__file__).parent / "www" / "joule-card.js"

def _get_version() -> str:
    """Read the integration version from manifest.json."""
    manifest = Path(__file__).parent / "manifest.json"
    try:
        return json.loads(manifest.read_text())["version"]
    except Exception:  # noqa: BLE001
        return "0"

LOVELACE_LOCAL_URL = f"{_LOVELACE_LOCAL_BASE}?v={_get_version()}"


def _copy_card_to_www(hass: HomeAssistant) -> None:
    """Copy the card JS to {config}/www/ so it is served at /local/."""
    www_dir = Path(hass.config.path("www"))
    www_dir.mkdir(exist_ok=True)
    dest = www_dir / "joule-sous-vide-card.js"
    if LOVELACE_CARD_PATH.is_file():
        shutil.copy2(str(LOVELACE_CARD_PATH), str(dest))


def _get_lovelace_resources(hass: HomeAssistant) -> Any:
    """Get the Lovelace resources collection.

    Uses attribute access on the LovelaceData dataclass (required since HA
    2025.2 — dict-style access is deprecated and breaks in 2026.2).

    With ``lovelace`` declared in ``manifest.json`` ``dependencies``, HA
    guarantees the lovelace component's ``async_setup()`` has completed and
    ``hass.data["lovelace"]`` is populated before our ``async_setup()`` runs.
    """
    lovelace_data = hass.data.get("lovelace")
    if lovelace_data is None:
        return None
    # Attribute access — LovelaceData is a dataclass with a .resources field
    resources = getattr(lovelace_data, "resources", None)
    if resources is not None:
        return resources
    # Fallback for very old HA where lovelace_data was a plain dict
    if isinstance(lovelace_data, dict):
        return lovelace_data.get("resources")
    return None


async def _register_lovelace_resource(hass: HomeAssistant) -> bool:
    """Add the card JS as a Lovelace resource if not already present.

    Returns True if the resource is registered (or was already present).
    Only works in storage mode (the HAOS default). In YAML mode, users must
    add the resource manually.
    """
    # Check resource mode — auto-registration only works in storage mode
    lovelace_data = hass.data.get("lovelace")
    if lovelace_data is not None:
        mode = getattr(lovelace_data, "resource_mode", None)
        if mode == "yaml":
            _LOGGER.info(
                "Lovelace is in YAML mode — add the card resource manually: "
                "url: %s, type: module",
                LOVELACE_LOCAL_URL,
            )
            return False

    resources = _get_lovelace_resources(hass)
    if resources is None:
        _LOGGER.warning(
            "Lovelace resources not available — "
            "lovelace component may not be loaded yet"
        )
        return False

    try:
        if not resources.loaded:
            await resources.async_load()

        # Remove stale entries (old-versioned or unversioned) and check for current
        for item in resources.async_items():
            url = item.get("url", "")
            if url == LOVELACE_LOCAL_URL:
                _LOGGER.debug("Lovelace resource already registered")
                return True
            if url == _LOVELACE_LOCAL_BASE or (
                url.startswith(_LOVELACE_LOCAL_BASE + "?")
                and url != LOVELACE_LOCAL_URL
            ):
                item_id = item.get("id")
                if item_id:
                    await resources.async_delete_item(item_id)
                    _LOGGER.info("Removed stale Lovelace resource %s", url)

        await resources.async_create_item(
            {"res_type": "module", "url": LOVELACE_LOCAL_URL}
        )
        _LOGGER.info("Registered Lovelace resource %s", LOVELACE_LOCAL_URL)
        return True
    except Exception:  # noqa: BLE001
        _LOGGER.exception("Failed to register Lovelace resource")
        return False


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Copy the card to www/ and register it as a Lovelace resource."""
    # Copy the card JS to {config}/www/ so it is always available at
    # /local/joule-sous-vide-card.js regardless of integration state.
    await hass.async_add_executor_job(_copy_card_to_www, hass)

    # Register the Lovelace resource. If HA is already running (e.g. the
    # integration was just installed via the UI), register immediately.
    # Otherwise defer until HA has started so the resource collection is loaded.
    if hass.state is CoreState.running:
        _LOGGER.debug("HA running — registering Lovelace resource now")
        await _register_lovelace_resource(hass)
    else:
        _LOGGER.debug("HA not yet started — deferring Lovelace resource registration")

        async def _on_ha_started(event: Event) -> None:
            _LOGGER.debug("HA started event — registering Lovelace resource")
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

    # Fallback: if async_setup's deferred registration didn't work (lovelace
    # wasn't ready yet), try again now — async_setup_entry runs later in the
    # boot sequence so the lovelace resources collection is more likely loaded.
    await _register_lovelace_resource(hass)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry and disconnect from the device."""
    coordinator: JouleCoordinator = hass.data[DOMAIN][entry.entry_id]

    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator._stop_proxy_poller()
        await coordinator.api.disconnect()

    return unload_ok
