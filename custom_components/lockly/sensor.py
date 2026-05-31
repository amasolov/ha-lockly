"""Sensor platform for the Lockly integration."""

from __future__ import annotations

import logging

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import PERCENTAGE, SIGNAL_STRENGTH_DECIBELS_MILLIWATT
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
    """Set up Lockly sensor entities."""
    coordinator: LocklyCoordinator = entry.runtime_data
    entities: list[SensorEntity] = []
    for lock in coordinator.locks:
        entities.append(LocklyBatterySensor(coordinator, lock))
        entities.append(LocklyRssiSensor(coordinator, lock))
    async_add_entities(entities)


class LocklyBatterySensor(CoordinatorEntity[LocklyCoordinator], SensorEntity):
    """Battery level sensor for a Lockly lock."""

    _attr_has_entity_name = True
    _attr_name = "Battery"
    _attr_device_class = SensorDeviceClass.BATTERY
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = PERCENTAGE

    def __init__(
        self, coordinator: LocklyCoordinator, lock: DoorLock
    ) -> None:
        super().__init__(coordinator)
        self._lock = lock
        self._attr_unique_id = f"lockly_{lock.id}_battery"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, lock.id)},
        )

    @property
    def native_value(self) -> int | None:
        state = self.coordinator.device_states.get(self._lock.id)
        if state is None:
            return None
        return state.battery

    @callback
    def _handle_coordinator_update(self) -> None:
        self.async_write_ha_state()


class LocklyRssiSensor(CoordinatorEntity[LocklyCoordinator], SensorEntity):
    """Signal strength sensor for a Lockly lock."""

    _attr_has_entity_name = True
    _attr_name = "Signal strength"
    _attr_device_class = SensorDeviceClass.SIGNAL_STRENGTH
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = SIGNAL_STRENGTH_DECIBELS_MILLIWATT
    _attr_entity_registry_enabled_default = False

    def __init__(
        self, coordinator: LocklyCoordinator, lock: DoorLock
    ) -> None:
        super().__init__(coordinator)
        self._lock = lock
        self._attr_unique_id = f"lockly_{lock.id}_rssi"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, lock.id)},
        )

    @property
    def native_value(self) -> int | None:
        state = self.coordinator.device_states.get(self._lock.id)
        if state is None:
            return None
        return state.rssi

    @callback
    def _handle_coordinator_update(self) -> None:
        self.async_write_ha_state()
