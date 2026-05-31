"""Binary sensor platform for the Lockly Cloud integration."""

from __future__ import annotations

import logging

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
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
    """Set up Lockly binary sensor entities."""
    coordinator: LocklyCoordinator = entry.runtime_data
    async_add_entities(
        LocklyDoorSensor(coordinator, lock) for lock in coordinator.locks
    )


class LocklyDoorSensor(CoordinatorEntity[LocklyCoordinator], BinarySensorEntity):
    """Door open/closed sensor using the lock's magnetic sensor."""

    _attr_has_entity_name = True
    _attr_name = "Door"
    _attr_device_class = BinarySensorDeviceClass.DOOR

    def __init__(
        self, coordinator: LocklyCoordinator, lock: DoorLock
    ) -> None:
        super().__init__(coordinator)
        self._lock = lock
        self._attr_unique_id = f"lockly_cloud_{lock.id}_door"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, lock.id)},
        )

    @property
    def is_on(self) -> bool | None:
        """Return True if the door is open."""
        state = self.coordinator.device_states.get(self._lock.id)
        if state is None or state.door_state is None:
            return None
        return state.door_state.lower() in ("open", "1", "true")

    @callback
    def _handle_coordinator_update(self) -> None:
        self.async_write_ha_state()
