"""Constants for the Lockly Cloud integration."""

from homeassistant.const import Platform

DOMAIN = "lockly_cloud"

CONF_EMAIL = "email"
CONF_PASSWORD = "password"

PLATFORMS: list[Platform] = [
    Platform.LOCK,
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.EVENT,
]

MQTT_RECONNECT_INTERVAL = 30
STATUS_POLL_INTERVAL = 300
