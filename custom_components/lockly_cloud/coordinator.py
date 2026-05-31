"""Data coordinator for the Lockly Cloud integration.

Manages the REST API session, MQTT connection, and device state.
Exposes the BLE query-then-command flow for lock/unlock operations.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from pylockly import DeviceState, DoorLock, LocklyAPI, LocklyMqtt
from pylockly.ble_cmd import (
    build_lock_command,
    build_query_status_command,
    derive_aes_key,
    parse_ble_response,
)
from pylockly.exceptions import LocklyError

from .const import MQTT_RECONNECT_INTERVAL, STATUS_POLL_INTERVAL

_LOGGER = logging.getLogger(__name__)


class LocklyCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinate Lockly API and MQTT communication."""

    def __init__(self, hass: HomeAssistant, email: str, password: str) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name="Lockly Cloud",
            update_interval=None,
        )
        self._email = email
        self._password = password
        self.api = LocklyAPI()
        self.mqtt = LocklyMqtt()
        self.locks: list[DoorLock] = []
        self.device_states: dict[str, DeviceState] = {}
        self._mqtt_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._command_lock = asyncio.Lock()

    async def async_setup(self) -> None:
        """Perform initial login and connect MQTT."""
        self.locks = await self.api.login(self._email, self._password)

        self.mqtt.on_device_state(self._handle_device_state)

        token = self.api.auth_token
        if token:
            try:
                await self.mqtt.connect(self._email, token)
                _LOGGER.info("Lockly MQTT connected")
            except Exception:
                _LOGGER.warning(
                    "Lockly MQTT connection failed; will retry", exc_info=True
                )
                self._schedule_mqtt_reconnect()

        self._poll_task = self.hass.async_create_background_task(
            self._poll_status_loop(), "lockly_cloud_status_poll"
        )

        self._update_data()

    async def async_shutdown(self) -> None:
        """Clean up connections."""
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
        if self._mqtt_task and not self._mqtt_task.done():
            self._mqtt_task.cancel()
        await self.mqtt.disconnect()
        await self.api.close()

    async def async_lock_device(self, lock: DoorLock) -> None:
        """Lock a device via the MQTT query-then-command BLE flow."""
        await self._send_lock_command(lock, do_lock=True)

    async def async_unlock_device(self, lock: DoorLock) -> None:
        """Unlock a device via the MQTT query-then-command BLE flow."""
        await self._send_lock_command(lock, do_lock=False)

    async def _send_lock_command(
        self, lock: DoorLock, *, do_lock: bool
    ) -> None:
        """Execute the full AES query-then-lock/unlock flow.

        1. Send QueryLockStatusCmd (opcode 1E) to obtain the random number
        2. Parse the response and extract the 16-char hex random number
        3. Build and send the lock/unlock command with the random number
        """
        if not self.mqtt.connected:
            raise HomeAssistantError("Lockly MQTT is not connected")

        async with self._command_lock:
            mc = lock.master_code
            uuid_hex = lock.uuid or lock.id
            aes_key = derive_aes_key(mc, uuid_hex)
            tz = lock.timezone or None

            query_cmd = build_query_status_command(
                mc, uuid_hex, is_hub=True, tz_name=tz
            )

            _LOGGER.debug(
                "Sending query status to %s (%s)",
                lock.name,
                lock.id,
            )
            query_resp = await self.mqtt.send_lock_command(lock.id, query_cmd)

            resp_b64 = query_resp.payload.get("commandContent", "")
            if not resp_b64:
                raise HomeAssistantError("Empty response from lock query")

            resp_bytes = base64.b64decode(resp_b64)
            parsed = parse_ble_response(resp_bytes, aes_key=aes_key)

            if parsed.get("is_error"):
                raise HomeAssistantError(
                    f"Lock query failed: error 0x{parsed.get('error_code', 0):02x}"
                )

            random_number = parsed.get("random_number", "")
            if not random_number:
                raise HomeAssistantError(
                    "Could not extract random number from query response"
                )

            _LOGGER.debug(
                "Got random number for %s, sending %s command",
                lock.name,
                "lock" if do_lock else "unlock",
            )

            lock_cmd = build_lock_command(
                mc,
                uuid_hex,
                lock=do_lock,
                pwd=lock.host_code,
                pwd_id=1,
                encrypt_type=5,
                opcode="22",
                via_hub=True,
                use_aes=True,
                random_number=random_number,
            )

            cmd_resp = await self.mqtt.send_lock_command(lock.id, lock_cmd)

            cmd_b64 = cmd_resp.payload.get("commandContent", "")
            if cmd_b64:
                cmd_bytes = base64.b64decode(cmd_b64)
                cmd_parsed = parse_ble_response(cmd_bytes, aes_key=aes_key)
                if cmd_parsed.get("is_error"):
                    raise HomeAssistantError(
                        f"Lock command failed: error 0x{cmd_parsed.get('error_code', 0):02x}"
                    )

            action = "locked" if do_lock else "unlocked"
            _LOGGER.info("Successfully %s %s", action, lock.name)

    @callback
    def _handle_device_state(self, states: list[DeviceState]) -> None:
        """Process incoming MQTT device state updates."""
        for state in states:
            existing = self.device_states.get(state.device_id)
            if existing is None:
                self.device_states[state.device_id] = state
            else:
                if state.lock_state is not None:
                    existing.lock_state = state.lock_state
                if state.door_state is not None:
                    existing.door_state = state.door_state
                if state.battery is not None:
                    existing.battery = state.battery
                if state.rssi is not None:
                    existing.rssi = state.rssi
                if state.timestamp is not None:
                    existing.timestamp = state.timestamp

        self._update_data()

    def _update_data(self) -> None:
        """Push current state to HA entities."""
        self.async_set_updated_data({
            "locks": {lock.id: lock for lock in self.locks},
            "states": dict(self.device_states),
        })

    def _schedule_mqtt_reconnect(self) -> None:
        """Schedule an MQTT reconnect attempt."""
        async def _reconnect() -> None:
            await asyncio.sleep(MQTT_RECONNECT_INTERVAL)
            try:
                token = self.api.auth_token
                if token:
                    await self.mqtt.connect(self._email, token)
                    _LOGGER.info("Lockly MQTT reconnected")
                else:
                    self.locks = await self.api.login(
                        self._email, self._password
                    )
                    token = self.api.auth_token
                    if token:
                        await self.mqtt.connect(self._email, token)
            except Exception:
                _LOGGER.warning(
                    "Lockly MQTT reconnect failed; will retry", exc_info=True
                )
                self._schedule_mqtt_reconnect()

        self._mqtt_task = self.hass.async_create_background_task(
            _reconnect(), "lockly_cloud_mqtt_reconnect"
        )

    async def _poll_status_loop(self) -> None:
        """Periodically poll hub status as a heartbeat/fallback."""
        while True:
            await asyncio.sleep(STATUS_POLL_INTERVAL)
            try:
                for lock in self.locks:
                    if lock.hub_id:
                        await self.api.get_status(lock.hub_id)
            except LocklyError as exc:
                _LOGGER.debug("Status poll failed: %s", exc)
            except asyncio.CancelledError:
                return
            except Exception:
                _LOGGER.exception("Unexpected error in status poll")

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data (called by DataUpdateCoordinator if update_interval is set)."""
        return {
            "locks": {lock.id: lock for lock in self.locks},
            "states": dict(self.device_states),
        }
