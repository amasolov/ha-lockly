# Lockly for Home Assistant

[![HACS](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)

Control a **Lockly Secure Pro** smart lock via the Lockly cloud API. Uses REST
authentication and MQTT for real-time state updates and lock/unlock commands.

## Entities

| Entity | Type | Description |
|--------|------|-------------|
| Lock | `lock` | Lock/unlock control |
| Battery | `sensor` | Battery level (%) |
| Signal strength | `sensor` | BLE RSSI (disabled by default) |
| Door | `binary_sensor` | Door open/closed (magnetic sensor) |

## Requirements

| Requirement | Details |
|---|---|
| Home Assistant | 2026.4 or later |
| Lockly account | Same email/password as the Lockly app |
| Lockly hub | PGH260 Matter Link (or compatible WiFi hub) paired with the lock |

## Installation

### HACS (recommended)

1. Open **HACS > Integrations > three-dot menu > Custom repositories**
2. Add `https://github.com/amasolov/ha-lockly` as type **Integration**
3. Search for **Lockly** and click **Download**
4. Restart Home Assistant

### Manual

Copy `custom_components/lockly/` into your Home Assistant
`config/custom_components/` folder, then restart.

## Setup

1. Go to **Settings > Devices & Services > Add Integration**
2. Search for **Lockly**
3. Enter your Lockly account email and password
4. The integration discovers all locks on your account and creates entities for each

## How it works

The integration authenticates via the Lockly REST API (RSA + 3DES encrypted),
then maintains a persistent MQTT connection to the Lockly cloud broker for
real-time push updates. Lock/unlock commands use a challenge-response BLE
command flow (AES-ECB encrypted) forwarded through the hub via MQTT.

State changes from physical unlock, keypad entry, or app control arrive within
2-5 seconds.

## License

[MIT](LICENSE)
