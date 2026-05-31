"""Data coordinator for the Lockly Cloud integration.

Manages the REST API session, MQTT connection, and device state.
Exposes the BLE query-then-command flow for lock/unlock operations.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import time as time_mod
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

EVENT_LOG_POLL_INTERVAL = 30

_SUPPRESSED_CMD_ERRORS: frozenset[int] = frozenset({0xFF})

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
        self.last_lock_event: dict[str, dict[str, Any]] = {}
        self._prev_lock_states: dict[str, str | None] = {}
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

        for lock in self.locks:
            _LOGGER.info(
                "Lock %s: id=%s uuid=%s",
                lock.name,
                lock.id,
                lock.uuid,
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

        The APK silently ignores certain BLE error codes on lock commands
        (0xFF in particular has onErrorCmd empty and isShowErrorInfo=false),
        relying on MQTT deviceStateCallback for actual state. We mirror that
        behaviour: non-fatal errors produce a warning and optimistic state
        update rather than raising HomeAssistantError.
        """
        if not self.mqtt.connected:
            raise HomeAssistantError("Lockly MQTT is not connected")

        action_str = "lock" if do_lock else "unlock"

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
                "Got random number for %s (len=%d), sending %s command "
                "(host_code len=%d, pwd_id=1)",
                lock.name,
                len(random_number),
                action_str,
                len(lock.host_code),
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
                _LOGGER.debug(
                    "%s response for %s: cmd_type=%s is_error=%s error_code=%s",
                    action_str,
                    lock.name,
                    cmd_parsed.get("cmd_type"),
                    cmd_parsed.get("is_error"),
                    cmd_parsed.get("error_hex", "n/a"),
                )
                if cmd_parsed.get("is_error"):
                    err = cmd_parsed.get("error_code", 0)
                    if err not in _SUPPRESSED_CMD_ERRORS:
                        raise HomeAssistantError(
                            f"Lock command failed: error 0x{err:02x}"
                        )
                    _LOGGER.warning(
                        "%s command for %s returned suppressed error 0x%02x; "
                        "optimistically updating state",
                        action_str,
                        lock.name,
                        err,
                    )

            state = self.device_states.get(lock.id)
            if state is None:
                state = DeviceState(device_id=lock.id)
                self.device_states[lock.id] = state
            state.lock_state = "locked" if do_lock else "unlocked"
            self._update_data()

            _LOGGER.info("Successfully sent %s to %s", action_str, lock.name)

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
                    self._prev_lock_states[lock.id] = state.lock_state
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
        """Process incoming MQTT device state updates.

        When the lock_state transitions (e.g. unlocked -> locked), an
        immediate REST event log fetch is scheduled to get user details.
        If the REST fetch fails, a synthetic event without user info is
        stored as a fallback.
        """
        changed_device_ids: list[str] = []

        for state in states:
            existing = self.device_states.get(state.device_id)
            if existing is None:
                self.device_states[state.device_id] = state
            else:
                if state.lock_state is not None:
                    prev = self._prev_lock_states.get(state.device_id)
                    if prev is not None and prev != state.lock_state:
                        changed_device_ids.append(state.device_id)
                        _LOGGER.debug(
                            "Lock state change for %s: %s -> %s",
                            state.device_id,
                            prev,
                            state.lock_state,
                        )
                    self._prev_lock_states[state.device_id] = state.lock_state
                    existing.lock_state = state.lock_state
                if state.door_state is not None:
                    existing.door_state = state.door_state
                if state.battery is not None:
                    existing.battery = state.battery
                if state.rssi is not None:
                    existing.rssi = state.rssi
                if state.timestamp is not None:
                    existing.timestamp = state.timestamp

        if changed_device_ids:
            self.hass.async_create_background_task(
                self._fetch_events_for_devices(changed_device_ids),
                "lockly_cloud_event_fetch",
            )

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

    def _resolve_lock_id(self, device_id: str) -> str:
        """Map an MQTT device_id to the canonical lock.id used by entities."""
        for lock in self.locks:
            if lock.id == device_id or lock.uuid == device_id:
                return lock.id
        return device_id

    def _resolve_query_id(self, device_id: str) -> str:
        """Get the UUID to use for REST API queries."""
        for lock in self.locks:
            if lock.id == device_id or lock.uuid == device_id:
                return lock.uuid or lock.id
        return device_id

    async def _fetch_events_for_devices(
        self, device_ids: list[str]
    ) -> None:
        """Fetch recent events via REST for the given devices.

        Waits a few seconds for the event to propagate to the server,
        then queries the event log. Falls back to a synthetic event
        (without user info) if the REST call fails.
        """
        await asyncio.sleep(5)

        for device_id in device_ids:
            lock_id = self._resolve_lock_id(device_id)
            query_id = self._resolve_query_id(device_id)
            try:
                now_ms = int(time_mod.time() * 1000)
                _LOGGER.info(
                    "Fetching event log for %s (query_id=%s, lock_id=%s)",
                    device_id,
                    query_id,
                    lock_id,
                )
                events = await self.api.query_event_log(
                    query_id,
                    start_ms=now_ms - 120_000,
                    end_ms=now_ms,
                    limit=5,
                )
                _LOGGER.info(
                    "Event log returned %d events for %s",
                    len(events),
                    lock_id,
                )
                new_events = [
                    e for e in events
                    if e.event_id > self._last_event_ids.get(lock_id, 0)
                ]
                if new_events:
                    latest = max(new_events, key=lambda e: e.event_id)
                    self._last_event_ids[lock_id] = latest.event_id
                    self._store_rest_event(lock_id, latest)
                else:
                    _LOGGER.info(
                        "No new REST events for %s; using synthetic", lock_id
                    )
                    self._set_synthetic_event(lock_id)
            except Exception:
                _LOGGER.warning(
                    "REST event log fetch failed for %s; using synthetic",
                    lock_id,
                    exc_info=True,
                )
                self._set_synthetic_event(lock_id)

    def _set_synthetic_event(self, device_id: str) -> None:
        """Store a fallback event from MQTT state change (no user info)."""
        state = self.device_states.get(device_id)
        lock_state = state.lock_state if state else None
        event_type = "locked" if lock_state == "locked" else "unlocked"
        self.last_lock_event[device_id] = {
            "event_type": event_type,
            "user_name": "",
            "user_id": "",
            "timestamp": int(time_mod.time() * 1000),
        }
        self._update_data()

    async def _poll_event_log_loop(self) -> None:
        """Periodically poll REST event logs for all locks.

        Serves as a fallback in case the MQTT state change callback
        doesn't fire (e.g. physical key unlock that MQTT misses).
        """
        await asyncio.sleep(15)

        first_run = True
        while True:
            try:
                for lock in self.locks:
                    now_ms = int(time_mod.time() * 1000)
                    lookback = 3600_000 if first_run else (
                        EVENT_LOG_POLL_INTERVAL * 2 * 1000
                    )
                    query_id = lock.uuid or lock.id
                    events = await self.api.query_event_log(
                        query_id,
                        start_ms=now_ms - lookback,
                        end_ms=now_ms,
                        limit=10,
                    )
                    if first_run:
                        _LOGGER.info(
                            "Initial event log poll for %s (query_id=%s): "
                            "%d events found",
                            lock.name,
                            query_id,
                            len(events),
                        )
                        for ev in events[:3]:
                            _LOGGER.info(
                                "  event: id=%d type=%s(%s) user=%s time=%s",
                                ev.event_id,
                                ev.event_type,
                                ev.event_type_name,
                                ev.lock_user_name or "?",
                                ev.time,
                            )
                    new_events = [
                        e for e in events
                        if e.event_id > self._last_event_ids.get(lock.id, 0)
                    ]
                    if new_events:
                        latest = max(new_events, key=lambda e: e.event_id)
                        self._last_event_ids[lock.id] = latest.event_id
                        if not first_run:
                            self._store_rest_event(lock.id, latest)
                    elif first_run and events:
                        self._last_event_ids[lock.id] = max(
                            e.event_id for e in events
                        )
                first_run = False
            except asyncio.CancelledError:
                return
            except Exception:
                _LOGGER.warning(
                    "Event log poll failed", exc_info=True
                )

            await asyncio.sleep(EVENT_LOG_POLL_INTERVAL)

    def _store_rest_event(self, lock_id: str, event: Any) -> None:
        """Store a REST event log entry and notify entities."""
        event_type = event.event_type_name
        if event_type.startswith("unknown_"):
            event_type = "locked" if "lock" in event_type.lower() else "unlocked"

        self.last_lock_event[lock_id] = {
            "event_type": event_type,
            "user_name": event.lock_user_name or "",
            "user_id": event.user_id,
            "timestamp": event.timestamp or int(time_mod.time() * 1000),
            "event_id": event.event_id,
        }
        _LOGGER.info(
            "Lock event for %s: type=%s user=%s event_id=%d",
            lock_id,
            event_type,
            event.lock_user_name or "unknown",
            event.event_id,
        )
        self._update_data()

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data (called by DataUpdateCoordinator if update_interval is set)."""
        return {
            "locks": {lock.id: lock for lock in self.locks},
            "states": dict(self.device_states),
        }
