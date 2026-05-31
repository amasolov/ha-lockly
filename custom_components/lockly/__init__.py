"""The Lockly integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import CONF_EMAIL, CONF_PASSWORD, DOMAIN, PLATFORMS
from .coordinator import LocklyCoordinator

_LOGGER = logging.getLogger(__name__)

type LocklyConfigEntry = ConfigEntry[LocklyCoordinator]


async def async_setup_entry(hass: HomeAssistant, entry: LocklyConfigEntry) -> bool:
    """Set up Lockly from a config entry."""
    coordinator = LocklyCoordinator(
        hass,
        email=entry.data[CONF_EMAIL],
        password=entry.data[CONF_PASSWORD],
    )

    await coordinator.async_setup()
    entry.runtime_data = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(coordinator.async_shutdown)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: LocklyConfigEntry) -> bool:
    """Unload a Lockly config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
