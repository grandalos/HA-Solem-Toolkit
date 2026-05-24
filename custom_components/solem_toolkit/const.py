"""Constants for Solem Toolkit."""

DOMAIN = "solem_toolkit"

# Solem BLE characteristics used for command writes.
BLIP_WRITE_CHARACTERISTIC_UUID = "108b0002-eab5-bc09-d0ea-0b8f467ce8ee"
LRIP_WRITE_CHARACTERISTIC_UUID = "121f0002-af51-43db-939e-b3b5c9033447"

# Prefer LR-IP because Home Assistant can still fall back to BL-IP when services
# expose the older characteristic.
CHARACTERISTIC_UUIDS = (
    LRIP_WRITE_CHARACTERISTIC_UUID,
    BLIP_WRITE_CHARACTERISTIC_UUID,
)
CHARACTERISTIC_UUID = LRIP_WRITE_CHARACTERISTIC_UUID

# Default Bluetooth connection timeout (seconds)
DEFAULT_BLUETOOTH_TIMEOUT = 15
MIN_BLUETOOTH_TIMEOUT = 5
