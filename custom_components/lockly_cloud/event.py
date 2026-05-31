"""Event platform for the Lockly Cloud integration.

Fires an HA event each time the lock state changes (locked/unlocked) as
detected by the MQTT deviceStateCallback. User identification is not
available from this callback; a future REST API integration could enrich
events with user details.
"""

from __future__ import annotations

import logging

from homeassistant.components.event import EventEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from pylockly import DoorLock

from . import LocklyConfigEntry
from .const import DOMAIN
from .coordinator import LocklyCoordinator

_LOGGER = logging.getLogger(__name__)

EVENT_TYPES = ["locked", "unlocked"]


async def async_setup_entry(
    hass: HomeAssistant,
    entry: LocklyConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Lockly event entities."""
    coordinator: LocklyCoordinator = entry.runtime_data
    async_add_entities(
        LocklyLockEventEntity(coordinator, lock) for lock in coordinator.locks
    )


class LocklyLockEventEntity(CoordinatorEntity[LocklyCoordinator], EventEntity):
    """Fires an event each time the lock state transitions."""

    _attr_has_entity_name = True
    _attr_name = "Lock event"
    _attr_event_types = EVENT_TYPES

    def __init__(
        self, coordinator: LocklyCoordinator, lock: DoorLock
    ) -> None:
        super().__init__(coordinator)
        self._lock = lock
        self._attr_unique_id = f"lockly_cloud_{lock.id}_event"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, lock.id)},
        )
        self._last_seen: dict | None = None

    @callback
    def _handle_coordinator_update(self) -> None:
        """Fire event when lock state transitions."""
        ev = self.coordinator.last_lock_event.get(self._lock.id)
        if ev is None or ev is self._last_seen:
            return
        self._last_seen = ev
        event_type = ev.get("event_type", "unlocked")
        self._trigger_event(
            event_type,
            {"timestamp": ev.get("timestamp")},
        )
        self.async_write_ha_state()
