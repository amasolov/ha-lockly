"""Data coordinator for the Lockly Cloud integration.

Manages the REST API session, MQTT connection, and device state.
Exposes the BLE query-then-command flow for lock/unlock operations.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any

from datetime import datetime, timedelta, timezone

from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from pylockly import DeviceState, DoorLock, LockEvent, LocklyAPI, LocklyMqtt
from pylockly.ble_cmd import (
    build_lock_command,
    build_query_status_command,
    derive_aes_key,
    parse_ble_response,
)
from pylockly.exceptions import LocklyError

from .const import MQTT_RECONNECT_INTERVAL, STATUS_POLL_INTERVAL

EVENT_LOG_POLL_INTERVAL = 60

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
        self.lock_events: dict[str, list[LockEvent]] = {}
        self._last_event_ids: dict[str, int] = {}
        self._mqtt_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._event_poll_task: asyncio.Task[None] | None = None
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

        self._update_data()

        self.hass.async_create_background_task(
            self._fetch_initial_states(), "lockly_cloud_initial_states"
        )

        self._poll_task = self.hass.async_create_background_task(
            self._poll_status_loop(), "lockly_cloud_status_poll"
        )

        self._event_poll_task = self.hass.async_create_background_task(
            self._poll_event_log_loop(), "lockly_cloud_event_poll"
        )

    async def async_shutdown(self) -> None:
        """Clean up connections."""
        for task in (self._poll_task, self._event_poll_task, self._mqtt_task):
            if task and not task.done():
                task.cancel()
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

    async def _fetch_initial_states(self) -> None:
        """Query each lock via MQTT to populate initial state."""
        if not self.mqtt.connected:
            _LOGGER.debug("MQTT not connected; skipping initial state fetch")
            return

        await asyncio.sleep(2)

        for lock in self.locks:
            try:
                mc = lock.master_code
                uuid_hex = lock.uuid or lock.id
                aes_key = derive_aes_key(mc, uuid_hex)
                tz = lock.timezone or None

                query_cmd = build_query_status_command(
                    mc, uuid_hex, is_hub=True, tz_name=tz
                )
                _LOGGER.debug("Fetching initial state for %s", lock.name)
                resp = await self.mqtt.send_lock_command(lock.id, query_cmd)

                resp_b64 = resp.payload.get("commandContent", "")
                if not resp_b64:
                    _LOGGER.warning(
                        "Empty query response for %s; initial state unavailable",
                        lock.name,
                    )
                    continue

                resp_bytes = base64.b64decode(resp_b64)
                parsed = parse_ble_response(resp_bytes, aes_key=aes_key)

                if parsed.get("is_error"):
                    _LOGGER.warning(
                        "Query error for %s: 0x%02x",
                        lock.name,
                        parsed.get("error_code", 0),
                    )
                    continue

                state = self.device_states.get(lock.id)
                if state is None:
                    state = DeviceState(device_id=lock.id)
                    self.device_states[lock.id] = state

                if parsed.get("is_locked") is not None:
                    state.lock_state = "locked" if parsed["is_locked"] else "unlocked"
                if parsed.get("battery_pct") is not None:
                    state.battery = parsed["battery_pct"]
                if parsed.get("door_open") is not None:
                    state.door_state = "open" if parsed["door_open"] else "closed"

                _LOGGER.info(
                    "Initial state for %s: lock=%s battery=%s%% door=%s",
                    lock.name,
                    state.lock_state or "?",
                    state.battery if state.battery is not None else "?",
                    state.door_state or "?",
                )
            except asyncio.CancelledError:
                return
            except Exception:
                _LOGGER.warning(
                    "Failed to fetch initial state for %s", lock.name, exc_info=True
                )

        self._update_data()

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

    async def _poll_event_log_loop(self) -> None:
        """Periodically query lock event logs for new unlock events."""
        await asyncio.sleep(10)
        while True:
            if self.mqtt.connected:
                now = datetime.now(timezone.utc)
                start = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

                for lock in self.locks:
                    try:
                        resp = await self.mqtt.query_event_log(
                            lock.id, start, end, offset=0, limit=10
                        )
                        events = LockEvent.from_log_response(resp.payload)
                        last_seen = self._last_event_ids.get(lock.id, 0)
                        new_events = [
                            e for e in events if e.event_id > last_seen
                        ]
                        if new_events:
                            self.lock_events[lock.id] = new_events
                            self._last_event_ids[lock.id] = max(
                                e.event_id for e in new_events
                            )
                            _LOGGER.debug(
                                "%d new event(s) for %s",
                                len(new_events),
                                lock.name,
                            )
                            self._update_data()
                    except asyncio.CancelledError:
                        return
                    except Exception:
                        _LOGGER.debug(
                            "Event log poll failed for %s",
                            lock.name,
                            exc_info=True,
                        )
            try:
                await asyncio.sleep(EVENT_LOG_POLL_INTERVAL)
            except asyncio.CancelledError:
                return

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data (called by DataUpdateCoordinator if update_interval is set)."""
        return {
            "locks": {lock.id: lock for lock in self.locks},
            "states": dict(self.device_states),
        }
