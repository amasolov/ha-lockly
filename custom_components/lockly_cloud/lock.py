"""Lock platform for the Lockly Cloud integration."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.lock import LockEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from pylockly import DoorLock

from . import LocklyConfigEntry
from .const import DOMAIN
from .coordinator import LocklyCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: LocklyConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Lockly lock entities."""
    coordinator: LocklyCoordinator = entry.runtime_data
    async_add_entities(
        LocklyLockEntity(coordinator, lock) for lock in coordinator.locks
    )


class LocklyLockEntity(CoordinatorEntity[LocklyCoordinator], LockEntity):
    """A Lockly smart lock entity."""

    _attr_has_entity_name = True
    _attr_name = None

    def __init__(
        self, coordinator: LocklyCoordinator, lock: DoorLock
    ) -> None:
        super().__init__(coordinator)
        self._lock = lock
        self._attr_unique_id = f"lockly_cloud_{lock.id}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, lock.id)},
            name=lock.name or f"Lockly {lock.model}",
            manufacturer="Lockly",
            model=lock.model,
            sw_version=lock.lock_firmware,
        )
        self._transitioning = False

    @property
    def is_locked(self) -> bool | None:
        """Return true if the lock is locked."""
        state = self.coordinator.device_states.get(self._lock.id)
        if state is None or state.lock_state is None:
            return None
        return state.lock_state.lower() in ("locked", "1", "true")

    @property
    def is_locking(self) -> bool:
        return self._transitioning and self.is_locked is not True

    @property
    def is_unlocking(self) -> bool:
        return self._transitioning and self.is_locked is not False

    async def async_lock(self, **kwargs: Any) -> None:
        """Lock the device via MQTT BLE command."""
        self._transitioning = True
        self.async_write_ha_state()
        try:
            await self.coordinator.async_lock_device(self._lock)
        finally:
            self._transitioning = False
            self.async_write_ha_state()

    async def async_unlock(self, **kwargs: Any) -> None:
        """Unlock the device via MQTT BLE command."""
        self._transitioning = True
        self.async_write_ha_state()
        try:
            await self.coordinator.async_unlock_device(self._lock)
        finally:
            self._transitioning = False
            self.async_write_ha_state()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        self._transitioning = False
        self.async_write_ha_state()
