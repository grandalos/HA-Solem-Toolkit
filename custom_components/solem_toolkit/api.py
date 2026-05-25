"""Solem BLE API helper.

This is a lightweight subset of the Solem API used by the scheduling integration.
It focuses on robust BLE connection handling and command writes for manual actions.
"""

from __future__ import annotations

import asyncio
import logging
import struct
from typing import Optional

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.exc import BleakDBusError
from bleak_retry_connector import (
    BleakOutOfConnectionSlotsError,
    establish_connection,
)
from homeassistant.components import bluetooth
from tenacity import retry, stop_after_attempt, wait_exponential

from homeassistant.core import HomeAssistant

from .const import (
    CHARACTERISTIC_UUID,
    CHARACTERISTIC_UUIDS,
    DEFAULT_BLUETOOTH_TIMEOUT,
    NOTIFY_CHARACTERISTIC_UUID,
    NOTIFY_CHARACTERISTIC_UUIDS,
)

_LOGGER = logging.getLogger(__name__)


class APIConnectionError(Exception):
    """Exception raised when a BLE connection or write fails."""


class SolemAPI:
    """API wrapper for the Solem BLE protocol."""

    def __init__(
        self,
        hass: HomeAssistant,
        mac_address: str,
        bluetooth_timeout: int = DEFAULT_BLUETOOTH_TIMEOUT,
    ) -> None:
        self.hass = hass
        self.mac_address = mac_address
        self.bluetooth_timeout = bluetooth_timeout

        self.characteristic_uuid: str = CHARACTERISTIC_UUID
        self.notify_characteristic_uuid: str = NOTIFY_CHARACTERISTIC_UUID
        self._resolved_characteristic_uuid: str | None = None
        self._resolved_notify_characteristic_uuid: str | None = None
        self._conn_lock = asyncio.Lock()

    async def scan_bluetooth(self) -> list[BLEDevice]:
        """Return a list of discovered BLE devices."""
        service_infos = bluetooth.async_discovered_service_info(self.hass, connectable=True)
        return [service_info.device for service_info in service_infos]

    async def _resolve_ble_device(self) -> BLEDevice:
        """Resolve a BLEDevice for the configured MAC address."""
        # Use Home Assistant's bluetooth manager first so ESPHome bluetooth
        # proxies and other remote adapters can provide the connection.
        ble_device: Optional[BLEDevice] = bluetooth.async_ble_device_from_address(
            self.hass, self.mac_address, connectable=True
        )
        if ble_device is not None:
            return ble_device

        try:
            service_info = await bluetooth.async_process_advertisements(
                self.hass,
                lambda info: (info.address or "").lower() == self.mac_address.lower(),
                {"address": self.mac_address, "connectable": True},
                bluetooth.BluetoothScanningMode.ACTIVE,
                5.0,
            )
            if service_info is not None:
                return service_info.device
        except TimeoutError:
            pass

        # Last-resort direct scan for non-HA test contexts.
        devices = await BleakScanner.discover(timeout=5.0)
        for d in devices:
            if (d.address or "").lower() == self.mac_address.lower():
                return d

        raise APIConnectionError("Device not found! Failed connecting!")

    @staticmethod
    def _client_has_characteristic(client: BleakClient, uuid: str) -> bool:
        services = getattr(client, "services", None)
        if services is None:
            return False

        for service in services:
            for characteristic in service.characteristics:
                if str(characteristic.uuid).lower() == uuid.lower():
                    return True

        return False

    def _resolve_write_characteristic_uuid(self, client: BleakClient) -> str:
        """Return the write characteristic exposed by BL-IP or LR-IP."""
        if self._resolved_characteristic_uuid:
            return self._resolved_characteristic_uuid

        for uuid in CHARACTERISTIC_UUIDS:
            if self._client_has_characteristic(client, uuid):
                self._resolved_characteristic_uuid = uuid
                _LOGGER.debug("Using Solem write characteristic %s", uuid)
                return uuid

        # Keep the configured default so older HA wrappers without service detail
        # still try LR-IP first; users can inspect services via list_characteristics.
        return self.characteristic_uuid

    def _resolve_notify_characteristic_uuid(self, client: BleakClient) -> str:
        """Return the notify characteristic exposed by BL-IP or LR-IP."""
        if self._resolved_notify_characteristic_uuid:
            return self._resolved_notify_characteristic_uuid

        for uuid in NOTIFY_CHARACTERISTIC_UUIDS:
            if self._client_has_characteristic(client, uuid):
                self._resolved_notify_characteristic_uuid = uuid
                _LOGGER.debug("Using Solem notify characteristic %s", uuid)
                return uuid

        return self.notify_characteristic_uuid

    async def _connect_client(self) -> BleakClient:
        """Establish a robust connection using bleak-retry-connector."""
        async with self._conn_lock:
            ble_device = await self._resolve_ble_device()
            try:
                client = await establish_connection(
                    BleakClient,
                    ble_device,
                    name=f"Solem - {self.mac_address}",
                    timeout=self.bluetooth_timeout,
                    max_attempts=3,
                )
                return client
            except BleakOutOfConnectionSlotsError as exc:
                raise APIConnectionError(
                    "Bluetooth adapter/proxy out of connection slots or device busy/unreachable"
                ) from exc
            except (BleakDBusError, TimeoutError, OSError) as exc:
                raise APIConnectionError("Timeout connecting to device") from exc
            except Exception as exc:  # noqa: BLE001
                raise APIConnectionError("Unexpected BLE connection error") from exc

    async def list_characteristics(self) -> dict:
        """Return discovered services/characteristics (debug helper)."""
        client = await self._connect_client()
        try:
            if not client.is_connected:
                raise APIConnectionError("Failed connecting!")

            # Home Assistant wraps BleakClient (HaBleakClientWrapper) and does not
            # expose BleakClient.get_services(). After connecting, discovered
            # services are available via the `services` attribute.
            services = getattr(client, "services", None)
            if services is None:
                # Last-resort fallback for non-HA clients / unexpected wrappers.
                inner = getattr(client, "_client", None) or getattr(client, "_bleak_client", None)
                if inner is not None and hasattr(inner, "get_services"):
                    services = await inner.get_services()
                else:
                    raise APIConnectionError("Services not available on this platform/client")
            result: dict = {}
            for svc in services:
                chars = []
                for c in svc.characteristics:
                    chars.append(
                        {
                            "uuid": str(c.uuid),
                            "properties": list(c.properties),
                            "descriptors": [str(d.uuid) for d in c.descriptors],
                        }
                    )
                result[str(svc.uuid)] = chars
            return result
        finally:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.4, min=0.4, max=2))
    async def _write_with_auth_retry(self, client: BleakClient, payload: bytes) -> None:
        """Write with a small retry loop (Solem can be picky right after connect)."""
        if not client.is_connected:
            raise APIConnectionError("Client not connected")

        characteristic_uuid = self._resolve_write_characteristic_uuid(client)
        _LOGGER.debug("Writing Solem BLE payload to %s: %s", characteristic_uuid, payload.hex())
        await client.write_gatt_char(characteristic_uuid, payload, response=False)

    async def _write_and_commit(self, command: bytes) -> None:
        """Write a command then commit it (Solem protocol)."""
        client = await self._connect_client()
        try:
            if not client.is_connected:
                raise APIConnectionError("Failed connecting!")
            await self._write_with_auth_retry(client, command)
            # Commit frame
            commit = struct.pack(">BB", 0x3B, 0x00)
            await self._write_with_auth_retry(client, commit)
        finally:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    def _parse_status_notifications(notifications: list[bytes]) -> dict:
        """Best-effort parser for status frames observed on LR-IP notifications."""
        frames: list[dict] = []
        manual_frames: list[bytes] = []

        for data in notifications:
            frame = {"hex": data.hex(), "length": len(data)}
            if len(data) >= 3:
                frame["opcode"] = f"0x{data[0]:02x}"
                frame["payload_length"] = data[1]
                frame["sequence"] = data[2]

            # Frames 0x32 and 0x3c carry the manual watering status in captures.
            if len(data) >= 15 and data[0] in (0x32, 0x3C):
                remaining_seconds = int.from_bytes(data[13:15], "big")
                station_marker = data[3] & 0x0F
                manual_active = bool(data[9]) or remaining_seconds > 0
                frame.update(
                    {
                        "manual_active": manual_active,
                        "active_station": station_marker or None,
                        "remaining_seconds": remaining_seconds,
                    }
                )
                manual_frames.append(data)

            frames.append(frame)

        active_manual = next(
            (
                frame
                for frame in frames
                if frame.get("manual_active") is True
            ),
            None,
        )
        primary_manual = next(
            (
                frame
                for frame in frames
                if "manual_active" in frame and frame.get("sequence") == 2
            ),
            None,
        )
        latest_manual = active_manual or primary_manual or next(
            (
                frame
                for frame in reversed(frames)
                if "manual_active" in frame
            ),
            None,
        )

        result = {
            "raw_notifications": [data.hex() for data in notifications],
            "frames": frames,
            "manual_active": None,
            "active_station": None,
            "remaining_seconds": None,
            "parser": "lrip_manual_status_v1",
        }
        if latest_manual:
            result.update(
                {
                    "manual_active": latest_manual["manual_active"],
                    "active_station": latest_manual["active_station"],
                    "remaining_seconds": latest_manual["remaining_seconds"],
                }
            )

        return result

    async def get_status(self, wait_seconds: float = 2.0) -> dict:
        """Query controller status and return raw notifications plus parsed hints."""
        notifications: list[bytes] = []

        def _notification_handler(_sender: int, data: bytearray) -> None:
            payload = bytes(data)
            notifications.append(payload)
            _LOGGER.debug("Solem BLE notification: %s", payload.hex())

        client = await self._connect_client()
        try:
            if not client.is_connected:
                raise APIConnectionError("Failed connecting!")

            notify_uuid = self._resolve_notify_characteristic_uuid(client)
            await client.start_notify(notify_uuid, _notification_handler)
            try:
                await self._write_with_auth_retry(client, struct.pack(">BB", 0x3B, 0x00))
                await asyncio.sleep(max(0.25, min(wait_seconds, 10.0)))
            finally:
                await client.stop_notify(notify_uuid)

            return self._parse_status_notifications(notifications)
        finally:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    async def turn_on(self) -> None:
        """Turn on controller (enable watering)."""
        command = struct.pack(">HBBBH", 0x3105, 0xA0, 0x00, 0x00, 0x0000)
        await self._write_and_commit(command)

    async def turn_off_permanent(self) -> None:
        """Disable watering permanently."""
        command = struct.pack(">HBBBH", 0x3105, 0xC0, 0x00, 0x00, 0x0000)
        await self._write_and_commit(command)

    async def turn_off_x_days(self, days: int) -> None:
        """Disable watering for X days."""
        days = max(0, min(days, 15))
        command = struct.pack(">HBBBH", 0x3105, 0xC0, 0x00, days, 0x0000)
        await self._write_and_commit(command)

    async def sprinkle_station_x_for_y_minutes(self, station: int, minutes: int) -> None:
        """Manually water a station for Y minutes."""
        station = max(1, min(station, 16))
        seconds = max(1, min(minutes, 720)) * 60
        command = struct.pack(">HBBBH", 0x3105, 0x12, station, 0x00, seconds)
        await self._write_and_commit(command)

    async def sprinkle_all_stations_for_y_minutes(self, minutes: int) -> None:
        """Manually water all stations for Y minutes each."""
        seconds = max(1, min(minutes, 720)) * 60
        command = struct.pack(">HBBBH", 0x3105, 0x11, 0x00, 0x00, seconds)
        await self._write_and_commit(command)

    async def run_program_x(self, program: int) -> None:
        """Run a controller program by id (1-3 on most devices)."""
        program = max(1, min(program, 3))
        command = struct.pack(">HBBBH", 0x3105, 0x14, 0x00, program, 0x0000)
        await self._write_and_commit(command)

    async def stop_manual_sprinkle(self) -> None:
        """Stop any running manual watering session."""
        command = struct.pack(">HBBBH", 0x3105, 0x15, 0x00, 0xFF, 0x0000)
        await self._write_and_commit(command)
