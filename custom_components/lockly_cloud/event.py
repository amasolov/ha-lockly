"""Event platform for the Lockly Cloud integration."""

from __future__ import annotations

import logging

from homeassistant.components.event import EventDeviceClass, EventEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from pylockly import DoorLock, UNLOCK_TYPE_NAMES

from . import LocklyConfigEntry
from .const import DOMAIN
from .coordinator import LocklyCoordinator

_LOGGER = logging.getLogger(__name__)

EVENT_TYPES = list(UNLOCK_TYPE_NAMES.values()) + ["lock", "unknown"]


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
    """Fires an event each time the lock is operated, with user info."""

    _attr_has_entity_name = True
    _attr_name = "Lock event"
    _attr_device_class = EventDeviceClass.DOORBELL
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
        self._last_event_id: int | None = None

    @callback
    def _handle_coordinator_update(self) -> None:
        """Process new lock events from the coordinator."""
        events = self.coordinator.lock_events.get(self._lock.id, [])
        for event in events:
            if self._last_event_id is not None and event.event_id <= self._last_event_id:
                continue
            self._last_event_id = event.event_id
            self._trigger_event(
                event.event_type_name,
                {
                    "user_name": event.lock_user_name or "",
                    "user_id": event.user_id,
                    "method": event.event_type_name,
                    "event_type_code": event.event_type,
                    "time": event.time,
                },
            )
        self.async_write_ha_state()
