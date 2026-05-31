"""Event platform for the Lockly Cloud integration.

Fires an HA event each time a lock event is received, either from the
REST event log API (with user info) or from MQTT state transitions
(synthetic fallback). Event types map to Lockly unlock methods.
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

EVENT_TYPES = [
    "app",
    "keypad",
    "guest_code",
    "physical_key",
    "family_code",
    "rfid",
    "fingerprint",
    "one_time_code",
    "guest_fingerprint",
    "e_badge_unlock",
    "e_badge_lock",
    "locked",
    "unlocked",
]


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
    """Fires an event each time a lock event is received."""

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
        """Fire event when a new lock event arrives."""
        ev = self.coordinator.last_lock_event.get(self._lock.id)
        if ev is None or ev is self._last_seen:
            return
        self._last_seen = ev
        event_type = ev.get("event_type", "unlocked")
        attrs: dict = {"timestamp": ev.get("timestamp")}

        user_name = ev.get("user_name")
        if user_name:
            attrs["user_name"] = user_name
        user_id = ev.get("user_id")
        if user_id:
            attrs["user_id"] = user_id

        self._trigger_event(event_type, attrs)
        self.async_write_ha_state()
