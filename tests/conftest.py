"""Shared fixtures for Joule Sous Vide tests."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.joule_sous_vide.const import CONF_MAC_ADDRESS, DOMAIN
from custom_components.joule_sous_vide.coordinator import JouleCoordinator

TEST_MAC = "AA:BB:CC:DD:EE:FF"
TEST_ENTRY_ID = "test_entry_id"


@pytest.fixture
def mock_config_entry() -> MockConfigEntry:
    """Return a mock config entry with a test MAC address."""
    return MockConfigEntry(
        domain=DOMAIN,
        data={CONF_MAC_ADDRESS: TEST_MAC},
        entry_id=TEST_ENTRY_ID,
        title=f"Joule {TEST_MAC}",
        unique_id=TEST_MAC,
    )


@pytest.fixture
def mock_ble_api():
    """Patch JouleBLEAPI in the coordinator to prevent real BLE connections.

    All BLE methods are AsyncMock since bleak methods are awaited directly.
    """
    with patch(
        "custom_components.joule_sous_vide.coordinator.JouleBLEAPI"
    ) as mock_cls:
        instance = MagicMock()
        instance.ensure_connected = AsyncMock(return_value=False)
        instance.connect = AsyncMock()
        instance.disconnect = AsyncMock()
        instance.write_message = AsyncMock()
        instance.write_message_no_response = AsyncMock()
        instance.write_to_file_char = AsyncMock()
        instance.pair = AsyncMock(return_value=False)
        instance.subscribe = AsyncMock()
        instance.read_characteristic = AsyncMock(return_value=None)
        instance.sender_address = b"\x01\x00\x00\x00\x00\x01"
        instance.recipient_address = b"\xaa\xbb\xcc\xdd\xee\xff"
        instance.recipient_address_reversed = b"\xff\xee\xdd\xcc\xbb\xaa"
        mock_cls.return_value = instance
        yield instance


@pytest.fixture(autouse=True)
def _fast_notification_timeout(monkeypatch: pytest.MonkeyPatch):
    """Patch NOTIFICATION_TIMEOUT and KEY_EXCHANGE_TIMEOUT on the class so
    all tests run fast.

    This applies before the coordinator is instantiated, so the first
    refresh during async_config_entry_first_refresh uses the patched value.
    """
    monkeypatch.setattr(JouleCoordinator, "NOTIFICATION_TIMEOUT", 0.01)
    monkeypatch.setattr(JouleCoordinator, "KEY_EXCHANGE_TIMEOUT", 0.01)


@pytest.fixture(autouse=True)
def _mock_lovelace_resources(hass: HomeAssistant):
    """Provide a fake lovelace resources collection so the card auto-registers."""
    from unittest.mock import AsyncMock

    fake_resources = MagicMock()
    fake_resources.loaded = True
    fake_resources.async_items.return_value = []
    fake_resources.async_create_item = AsyncMock()
    fake_resources.async_load = AsyncMock()
    hass.data["lovelace"] = {"resources": fake_resources}


@pytest.fixture
async def setup_integration(
    hass: HomeAssistant,
    mock_config_entry: MockConfigEntry,
    mock_ble_api: MagicMock,
) -> MockConfigEntry:
    """Set up the full integration with a mocked BLE layer.

    Returns the MockConfigEntry so tests can access entry_id and state.
    """
    mock_config_entry.add_to_hass(hass)
    await hass.config_entries.async_setup(mock_config_entry.entry_id)
    await hass.async_block_till_done()
    return mock_config_entry
