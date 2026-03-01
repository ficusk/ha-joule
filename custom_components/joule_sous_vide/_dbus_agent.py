"""Minimal D-Bus pairing agent for BlueZ Just Works pairing.

IMPORTANT: This file intentionally does NOT use 'from __future__ import
annotations' because dbus_fast's @method() decorator inspects function
annotations as D-Bus type signatures.  With PEP 563 (future annotations),
all annotations become strings that dbus_fast cannot resolve, causing
'service annotations must be a string constant (got None)'.
"""
import logging

from dbus_fast.service import ServiceInterface, method

_LOGGER = logging.getLogger(__name__)


class JouleAgent(ServiceInterface):
    """BlueZ pairing agent that auto-accepts Just Works pairing."""

    def __init__(self):
        super().__init__("org.bluez.Agent1")

    @method()
    def Release(self):
        _LOGGER.warning("Agent: Release")

    @method()
    def RequestConfirmation(self, device: "o", passkey: "u"):
        _LOGGER.warning("Agent: RequestConfirmation device=%s passkey=%s", device, passkey)

    @method()
    def DisplayPasskey(self, device: "o", passkey: "u", entered: "q"):
        _LOGGER.warning("Agent: DisplayPasskey device=%s passkey=%s", device, passkey)

    @method()
    def RequestAuthorization(self, device: "o"):
        _LOGGER.warning("Agent: RequestAuthorization device=%s", device)

    @method()
    def AuthorizeService(self, device: "o", uuid: "s"):
        _LOGGER.warning("Agent: AuthorizeService device=%s uuid=%s", device, uuid)

    @method()
    def Cancel(self):
        _LOGGER.warning("Agent: Cancel")
