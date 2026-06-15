"""Tests for the config flow (config_flow.py)."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant import config_entries, data_entry_flow
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.joule_sous_vide.breville_cloud import BrevilleCloudKey
from custom_components.joule_sous_vide.const import (
    CONF_BLE_AUTH_KEY,
    CONF_BREVILLE_EMAIL,
    CONF_BREVILLE_PASSWORD,
    CONF_BREVILLE_SERIAL_NUMBER,
    CONF_MAC_ADDRESS,
    DOMAIN,
)
from custom_components.joule_sous_vide.joule_ble import JouleBLEError

from .conftest import TEST_MAC


def _joule_discovery(address: str = TEST_MAC) -> SimpleNamespace:
    """Return a fake HA Bluetooth discovery object for a Joule."""
    return SimpleNamespace(
        address=address,
        name="Jouletide",
        manufacturer_data={0x0159: bytes.fromhex("01c000004d93404f9e")},
        service_uuids=[],
        device=object(),
    )


async def test_form_is_shown(hass: HomeAssistant) -> None:
    """Initiating the flow shows the MAC address form with no errors."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    assert result["type"] == data_entry_flow.FlowResultType.FORM
    assert result["step_id"] == "user"
    assert result["errors"] == {}


async def test_form_lists_discovered_joules(hass: HomeAssistant) -> None:
    """Initiating the flow lists nearby Joules from HA Bluetooth discovery."""
    with patch(
        "custom_components.joule_sous_vide.config_flow.async_discovered_service_info",
        return_value=[_joule_discovery()],
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )

    assert result["type"] == data_entry_flow.FlowResultType.FORM
    assert result["step_id"] == "user"
    assert TEST_MAC in result["data_schema"].schema[CONF_MAC_ADDRESS].container


async def test_form_creates_entry_after_skipping_breville_import(
    hass: HomeAssistant,
) -> None:
    """A successful BLE connection can create an entry without cloud import."""
    with patch(
        "custom_components.joule_sous_vide.config_flow.JouleBLEAPI"
    ) as mock_cls:
        instance = MagicMock()
        instance.connect = AsyncMock()
        instance.disconnect = AsyncMock()
        mock_cls.return_value = instance

        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_MAC_ADDRESS: TEST_MAC}
        )
        assert result["type"] == data_entry_flow.FlowResultType.FORM
        assert result["step_id"] == "breville"
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {}
        )

    assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
    assert result["title"] == f"Joule {TEST_MAC}"
    assert result["data"] == {CONF_MAC_ADDRESS: TEST_MAC}
    assert result["options"] == {}


async def test_setup_imports_breville_cloud_auth_key(
    hass: HomeAssistant,
) -> None:
    """Initial setup can import and store the Breville+ BLE auth key."""
    with (
        patch(
            "custom_components.joule_sous_vide.config_flow.async_fetch_breville_ble_auth_key",
            AsyncMock(
                return_value=BrevilleCloudKey(
                    auth_key=bytes.fromhex("25ea00112233445566778899aabb75a7"),
                    circulator_id="c000004d93404f9e",
                    serial_number="164213370",
                    name="Jouletide",
                )
            ),
        ) as mock_import,
        patch(
            "custom_components.joule_sous_vide.config_flow.JouleBLEAPI"
        ) as mock_ble_cls,
        patch(
            "custom_components.joule_sous_vide.config_flow.async_get_clientsession"
        ) as mock_session,
    ):
        instance = MagicMock()
        instance.connect = AsyncMock()
        instance.disconnect = AsyncMock()
        instance.recipient_address = bytes.fromhex("c000004d93404f9e")
        mock_ble_cls.return_value = instance
        mock_session.return_value = object()

        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_MAC_ADDRESS: TEST_MAC}
        )
        assert result["type"] == data_entry_flow.FlowResultType.FORM
        assert result["step_id"] == "breville"
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_BREVILLE_EMAIL: "ficus@example.com",
                CONF_BREVILLE_PASSWORD: "secret",
                CONF_BREVILLE_SERIAL_NUMBER: "",
            },
        )

    assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
    assert result["data"] == {CONF_MAC_ADDRESS: TEST_MAC}
    assert result["options"][CONF_BLE_AUTH_KEY] == "25ea00112233445566778899aabb75a7"
    assert result["options"][CONF_BREVILLE_SERIAL_NUMBER] == "164213370"
    assert CONF_BREVILLE_EMAIL not in result["options"]
    assert CONF_BREVILLE_PASSWORD not in result["options"]
    mock_import.assert_awaited_once()
    assert mock_import.await_args.kwargs["circulator_id"] == "c000004d93404f9e"


async def test_form_shows_cannot_connect_on_ble_error(hass: HomeAssistant) -> None:
    """A JouleBLEError during validation shows a cannot_connect error on the form."""
    with patch(
        "custom_components.joule_sous_vide.config_flow.JouleBLEAPI"
    ) as mock_cls:
        instance = MagicMock()
        instance.connect = AsyncMock(side_effect=JouleBLEError("Bluetooth failed"))
        instance.disconnect = AsyncMock()
        mock_cls.return_value = instance

        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_MAC_ADDRESS: TEST_MAC}
        )

    assert result["type"] == data_entry_flow.FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}


async def test_form_shows_unknown_error_on_unexpected_exception(
    hass: HomeAssistant,
) -> None:
    """An unexpected exception during validation shows an unknown error."""
    with patch(
        "custom_components.joule_sous_vide.config_flow.JouleBLEAPI"
    ) as mock_cls:
        instance = MagicMock()
        instance.connect = AsyncMock(side_effect=RuntimeError("Unexpected crash"))
        instance.disconnect = AsyncMock()
        mock_cls.return_value = instance

        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_MAC_ADDRESS: TEST_MAC}
        )

    assert result["type"] == data_entry_flow.FlowResultType.FORM
    assert result["errors"] == {"base": "unknown"}


async def test_form_aborts_if_already_configured(
    hass: HomeAssistant,
    mock_config_entry: MockConfigEntry,
) -> None:
    """Submitting the same MAC address as an existing entry aborts the flow."""
    mock_config_entry.add_to_hass(hass)

    with patch(
        "custom_components.joule_sous_vide.config_flow.JouleBLEAPI"
    ) as mock_cls:
        instance = MagicMock()
        instance.connect = AsyncMock()
        instance.disconnect = AsyncMock()
        mock_cls.return_value = instance

        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_MAC_ADDRESS: TEST_MAC}
        )

    assert result["type"] == data_entry_flow.FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_connect_and_disconnect_called_during_validation(
    hass: HomeAssistant,
) -> None:
    """The config flow connects then immediately disconnects to validate the MAC."""
    with patch(
        "custom_components.joule_sous_vide.config_flow.JouleBLEAPI"
    ) as mock_cls:
        instance = MagicMock()
        instance.connect = AsyncMock()
        instance.disconnect = AsyncMock()
        mock_cls.return_value = instance

        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )
        await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_MAC_ADDRESS: TEST_MAC}
        )

    instance.connect.assert_called_once()
    instance.disconnect.assert_called_once()


async def test_bluetooth_discovery_confirm_creates_entry(
    hass: HomeAssistant,
) -> None:
    """A Bluetooth-discovered Joule can be confirmed without typing its address."""
    with patch(
        "custom_components.joule_sous_vide.config_flow.JouleBLEAPI"
    ) as mock_cls:
        instance = MagicMock()
        instance.connect = AsyncMock()
        instance.disconnect = AsyncMock()
        mock_cls.return_value = instance

        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": config_entries.SOURCE_BLUETOOTH},
            data=_joule_discovery(),
        )
        assert result["type"] == data_entry_flow.FlowResultType.FORM
        assert result["step_id"] == "bluetooth_confirm"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {}
        )
        assert result["type"] == data_entry_flow.FlowResultType.FORM
        assert result["step_id"] == "breville"
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {}
        )

    assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
    assert result["data"] == {CONF_MAC_ADDRESS: TEST_MAC}


async def test_options_imports_breville_cloud_auth_key(
    hass: HomeAssistant,
) -> None:
    """Breville+ options import stores only the derived BLE auth key."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_MAC_ADDRESS: TEST_MAC},
        entry_id="options_entry",
        unique_id=TEST_MAC,
    )
    entry.add_to_hass(hass)

    with (
        patch(
            "custom_components.joule_sous_vide.config_flow.async_fetch_breville_ble_auth_key",
            AsyncMock(
                return_value=BrevilleCloudKey(
                    auth_key=bytes.fromhex("25ea00112233445566778899aabb75a7"),
                    circulator_id="c000004d93404f9e",
                    serial_number="164213370",
                    name="Jouletide",
                )
            ),
        ) as mock_import,
        patch(
            "custom_components.joule_sous_vide.config_flow.JouleBLEAPI"
        ) as mock_ble_cls,
        patch(
            "custom_components.joule_sous_vide.config_flow.async_get_clientsession"
        ) as mock_session,
    ):
        mock_session.return_value = object()
        mock_ble_cls.return_value.recipient_address = bytes.fromhex("c000004d93404f9e")
        result = await hass.config_entries.options.async_init(entry.entry_id)
        assert result["type"] == data_entry_flow.FlowResultType.MENU
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], {"next_step_id": "breville"}
        )
        assert result["type"] == data_entry_flow.FlowResultType.FORM
        assert result["step_id"] == "breville"
        result = await hass.config_entries.options.async_configure(
            result["flow_id"],
            {
                CONF_BREVILLE_EMAIL: "ficus@example.com",
                CONF_BREVILLE_PASSWORD: "secret",
                CONF_BREVILLE_SERIAL_NUMBER: "",
            },
        )

    assert result["type"] == data_entry_flow.FlowResultType.CREATE_ENTRY
    assert result["data"][CONF_BLE_AUTH_KEY] == "25ea00112233445566778899aabb75a7"
    assert result["data"][CONF_BREVILLE_SERIAL_NUMBER] == "164213370"
    assert CONF_BREVILLE_EMAIL not in result["data"]
    assert CONF_BREVILLE_PASSWORD not in result["data"]
    mock_import.assert_awaited_once()
    assert mock_import.await_args.kwargs["circulator_id"] == "c000004d93404f9e"


async def test_options_shows_error_for_incomplete_breville_credentials(
    hass: HomeAssistant,
) -> None:
    """Both Breville+ email and password are required for cloud import."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_MAC_ADDRESS: TEST_MAC},
        entry_id="options_entry",
        unique_id=TEST_MAC,
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] == data_entry_flow.FlowResultType.MENU
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], {"next_step_id": "breville"}
    )
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {
            CONF_BREVILLE_EMAIL: "ficus@example.com",
            CONF_BREVILLE_PASSWORD: "",
            CONF_BREVILLE_SERIAL_NUMBER: "",
        },
    )

    assert result["type"] == data_entry_flow.FlowResultType.FORM
    assert result["errors"] == {"base": "missing_breville_credentials"}
