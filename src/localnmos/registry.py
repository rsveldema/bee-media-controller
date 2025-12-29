"""
NMOS Registry with MDNS support

This module provides NMOS device discovery using mDNS/DNS-SD (Zeroconf).
It discovers IS-04 and IS-05 NMOS services on the local network.
"""

import asyncio
import logging
from typing import Callable, Optional, Dict, Any
from dataclasses import dataclass
import socket

try:
    from zeroconf import ServiceBrowser, ServiceListener, Zeroconf, ServiceInfo
    from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser, AsyncServiceInfo
except ImportError:
    # Graceful degradation if zeroconf is not installed
    ServiceListener = object
    Zeroconf = None
    AsyncZeroconf = None

import aiohttp


logger = logging.getLogger(__name__)


@dataclass
class NMOSDevice:
    """Represents an NMOS device discovered on the network"""

    name: str
    device_id: str
    address: str
    port: int
    service_type: str
    version: str = "v1.3"
    api_ver: str = "v1.3"
    properties: Dict[str, Any] = None

    def __post_init__(self):
        if self.properties is None:
            self.properties = {}

    @property
    def base_url(self) -> str:
        """Get the base URL for the device API"""
        return f"http://{self.address}:{self.port}"

    @property
    def node_url(self) -> str:
        """Get the node API URL"""
        return f"{self.base_url}/x-nmos/node/{self.api_ver}"

    @property
    def connection_url(self) -> str:
        """Get the connection API URL (IS-05)"""
        return f"{self.base_url}/x-nmos/connection/{self.api_ver}"


class NMOSServiceListener(ServiceListener):
    """Listens for NMOS services advertised via mDNS"""

    def __init__(self, on_device_added: Callable, on_device_removed: Callable):
        self.on_device_added = on_device_added
        self.on_device_removed = on_device_removed
        self.devices: Dict[str, NMOSDevice] = {}

    def add_service(self, zc: Zeroconf, service_type: str, name: str) -> None:
        """Called when a service is discovered"""
        logger.info(f"Service added: {name} ({service_type})")

        # Get service info
        info = zc.get_service_info(service_type, name)
        if info:
            self._process_service_info(info, service_type, added=True)

    def remove_service(self, zc: Zeroconf, service_type: str, name: str) -> None:
        """Called when a service is removed"""
        logger.info(f"Service removed: {name}")

        if name in self.devices:
            device = self.devices.pop(name)
            if self.on_device_removed:
                self.on_device_removed(device)

    def update_service(self, zc: Zeroconf, service_type: str, name: str) -> None:
        """Called when a service is updated"""
        logger.info(f"Service updated: {name}")

        info = zc.get_service_info(service_type, name)
        if info:
            self._process_service_info(info, service_type, added=False)

    def _process_service_info(
        self, info: ServiceInfo, service_type: str, added: bool = True
    ):
        """Process service information and create/update device"""
        if not info or not info.addresses:
            return

        # Get the first IPv4 address
        address = socket.inet_ntoa(info.addresses[0])
        port = info.port

        # Parse TXT records for additional properties
        properties = {}
        if info.properties:
            for key, value in info.properties.items():
                try:
                    properties[key.decode("utf-8")] = value.decode("utf-8")
                except:
                    pass

        # Extract version from properties or service type
        api_ver = properties.get("api_ver", "v1.3")
        api_proto = properties.get("api_proto", "http")

        # Create device
        device = NMOSDevice(
            name=info.name,
            device_id=properties.get("node_id", info.name),
            address=address,
            port=port,
            service_type=service_type,
            api_ver=api_ver,
            properties=properties,
        )

        self.devices[info.name] = device

        if added and self.on_device_added:
            self.on_device_added(device)


class NMOSRegistry:
    """
    NMOS Registry with MDNS discovery support

    Discovers and manages NMOS devices on the local network using mDNS/DNS-SD.
    Supports IS-04 (Discovery and Registration) and IS-05 (Device Connection Management).
    """

    # NMOS service types for mDNS discovery
    NMOS_NODE_SERVICE = "_nmos-node._tcp.local."
    NMOS_REGISTRATION_SERVICE = "_nmos-registration._tcp.local."
    NMOS_QUERY_SERVICE = "_nmos-query._tcp.local."

    def __init__(
        self,
        device_added_callback: Optional[Callable] = None,
        device_removed_callback: Optional[Callable] = None,
    ):
        """
        Initialize the NMOS registry

        Args:
            device_added_callback: Callback function called when a device is discovered
            device_removed_callback: Callback function called when a device is removed
        """
        self.device_added_callback = device_added_callback
        self.device_removed_callback = device_removed_callback

        self.zeroconf: Optional[Zeroconf] = None
        self.async_zeroconf: Optional[AsyncZeroconf] = None
        self.browsers = []
        self.listener: Optional[NMOSServiceListener] = None
        self.devices: Dict[str, NMOSDevice] = {}
        self._running = False

    async def start(self):
        """Start the NMOS registry and begin discovering devices"""
        if self._running:
            logger.warning("Registry already running")
            return

        if Zeroconf is None:
            logger.error(
                "zeroconf library not available. Install with: pip install zeroconf"
            )
            return

        logger.info("Starting NMOS registry with MDNS discovery")
        self._running = True

        # Create Zeroconf instance
        self.async_zeroconf = AsyncZeroconf()
        self.zeroconf = self.async_zeroconf.zeroconf

        # Create service listener
        self.listener = NMOSServiceListener(
            on_device_added=self._on_device_added,
            on_device_removed=self._on_device_removed,
        )

        # Browse for NMOS services
        service_types = [
            self.NMOS_NODE_SERVICE,
            self.NMOS_REGISTRATION_SERVICE,
            self.NMOS_QUERY_SERVICE,
        ]

        for service_type in service_types:
            browser = ServiceBrowser(self.zeroconf, service_type, self.listener)
            self.browsers.append(browser)
            logger.info(f"Browsing for {service_type}")

    async def stop(self):
        """Stop the NMOS registry and cleanup resources"""
        if not self._running:
            return

        logger.info("Stopping NMOS registry")
        self._running = False

        # Cancel browsers
        for browser in self.browsers:
            browser.cancel()
        self.browsers.clear()

        # Close Zeroconf
        if self.async_zeroconf:
            await self.async_zeroconf.async_close()
            self.async_zeroconf = None
            self.zeroconf = None

        self.devices.clear()

    def _on_device_added(self, device: NMOSDevice):
        """Internal callback when a device is added"""
        logger.info(
            f"Device discovered: {device.name} at {device.address}:{device.port}"
        )
        self.devices[device.device_id] = device

        if self.device_added_callback:
            # Schedule callback in event loop
            try:
                asyncio.create_task(
                    self._call_callback(self.device_added_callback, device)
                )
            except RuntimeError:
                # If not in async context, call directly
                self.device_added_callback(device)

    def _on_device_removed(self, device: NMOSDevice):
        """Internal callback when a device is removed"""
        logger.info(f"Device removed: {device.name}")

        if device.device_id in self.devices:
            del self.devices[device.device_id]

        if self.device_removed_callback:
            try:
                asyncio.create_task(
                    self._call_callback(self.device_removed_callback, device)
                )
            except RuntimeError:
                self.device_removed_callback(device)

    async def _call_callback(self, callback: Callable, device: NMOSDevice):
        """Helper to call callbacks asynchronously"""
        if asyncio.iscoroutinefunction(callback):
            await callback(device)
        else:
            callback(device)

    def get_devices(self) -> Dict[str, NMOSDevice]:
        """Get all discovered devices"""
        return self.devices.copy()

    async def query_device(
        self, device: NMOSDevice, endpoint: str = "/self"
    ) -> Optional[Dict]:
        """
        Query a device's API endpoint

        Args:
            device: The NMOS device to query
            endpoint: API endpoint path (default: /self)

        Returns:
            JSON response or None if request fails
        """
        url = f"{device.node_url}{endpoint}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.warning(f"Failed to query {url}: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Error querying device {device.name}: {e}")

        return None
