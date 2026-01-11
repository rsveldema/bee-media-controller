"""
NMOS mDNS Service Management

This module handles mDNS/DNS-SD service discovery, registration, and announcement
for NMOS devices using Zeroconf.
"""

import asyncio
import logging
import socket
import traceback
from typing import Callable, Dict, Optional

from .nmos import NMOS_Node
from .error_log import ErrorLog

try:
    from zeroconf import ServiceBrowser, ServiceListener, Zeroconf, ServiceInfo, InterfaceChoice
    from zeroconf.asyncio import AsyncZeroconf
except ImportError:
    # Graceful degradation if zeroconf is not installed
    ServiceListener = object
    Zeroconf = None
    AsyncZeroconf = None
    InterfaceChoice = None
    ServiceInfo = None
    ServiceBrowser = None

from .logging_utils import create_logger

logger = create_logger(__name__)


class NMOSServiceListener(ServiceListener):
    """Listens for NMOS services advertised via mDNS"""

    def __init__(self, on_node_added: Callable, on_node_removed: Callable):
        self.on_node_added = on_node_added
        self.on_node_removed = on_node_removed
        self.devices: Dict[str, NMOS_Node] = {}

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
            if self.on_node_removed:
                self.on_node_removed(device)

    def get_node_by_id(self, node_id: str) -> NMOS_Node | None:
        for e in self.devices.values():
            if e.node_id == node_id:
                return e
        return None

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

        device_id = properties.get("node_id", info.name)
        if self.get_node_by_id(device_id) is not None:
            logger.info("already have device, ignoring")
            return

        # Extract version from properties or service type
        # MT48 and other devices may advertise different API versions
        api_ver = properties.get("api_ver", "v1.3")
        api_proto = properties.get("api_proto", "http")

        # Log discovered properties for debugging MT48 and other devices
        logger.debug(f"Device {info.name} properties: {properties}")

        # Create device
        devices = []

        # we're pretty sure we've found a new Node, lets allocate it
        logger.info(f"MDNS --> Discovered NMOS Node: {info.name} at {address}:{port} (API: {api_proto} {api_ver})")
        node = NMOS_Node(
            name=info.name,
            node_id=properties.get("node_id", info.name),
            address=address,
            port=port,
            service_type=service_type,
            api_ver=api_ver,
            properties=properties,
            devices=devices
        )

        # Track the node
        self.devices[info.name] = node

        # Notify if this is a new node being added (not just updated)
        if added and self.on_node_added:
            self.on_node_added(node)


class NMOSMDNSService:
    """Manages mDNS service registration, browsing, and announcements for NMOS"""

    def __init__(
        self,
        listen_ip: Optional[str] = None,
        on_node_added: Optional[Callable] = None,
        on_node_removed: Optional[Callable] = None
    ):
        self.listen_ip = listen_ip
        self.on_node_added = on_node_added
        self.on_node_removed = on_node_removed
        
        self.async_zeroconf: Optional[AsyncZeroconf] = None
        self.zeroconf: Optional[Zeroconf] = None
        self.listener: Optional[NMOSServiceListener] = None
        self.browsers = []
        self.registered_services: list[ServiceInfo] = []  # Store multiple registered services
        self.announcement_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self, service_types: list[str]):
        """
        Start mDNS service discovery and registration
        
        Args:
            service_types: List of NMOS service types to browse for
        """
        if self._running:
            logger.warning("mDNS service already running")
            return

        if Zeroconf is None:
            error_msg = "Zeroconf not available - mDNS discovery disabled"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg)
            return

        logger.info("Starting mDNS service with DNS-SD discovery")
        self._running = True

        # Create Zeroconf instance
        # mDNS operates on UDP port 5353 for DNS-SD service discovery
        # Use InterfaceChoice.All to ensure we respond to mDNS queries on all network interfaces
        self.async_zeroconf = AsyncZeroconf(interfaces=InterfaceChoice.All)
        self.zeroconf = self.async_zeroconf.zeroconf
        assert self.zeroconf is not None
        logger.info(f"Zeroconf initialized on all interfaces - listening on UDP port 5353 for mDNS/DNS-SD")

        # Create service listener
        self.listener = NMOSServiceListener(
            on_node_added=self.on_node_added,
            on_node_removed=self.on_node_removed,
        )

        # Browse for NMOS services
        for service_type in service_types:
            browser = ServiceBrowser(self.zeroconf, service_type, self.listener)
            self.browsers.append(browser)
            logger.info(f"Browsing for {service_type} via mDNS on port 5353")

    async def stop(self):
        """Stop mDNS service discovery and cleanup"""
        if not self._running:
            return

        logger.info("Stopping mDNS service")
        self._running = False

        # Stop periodic announcements
        if self.announcement_task:
            self.announcement_task.cancel()
            try:
                await self.announcement_task
            except asyncio.CancelledError:
                pass
            self.announcement_task = None

        # Unregister hosted service
        await self.unregister_service()

        # Cancel browsers
        for browser in self.browsers:
            browser.cancel()
        self.browsers.clear()

        # Close Zeroconf
        if self.async_zeroconf:
            await self.async_zeroconf.async_close()
            self.async_zeroconf = None
            self.zeroconf = None

    async def register_service(self, service_type: str, port: int, service_name_suffix: str = ""):
        """
        Register and advertise NMOS service via mDNS
        
        Args:
            service_type: mDNS service type (e.g., "_nmos-register._tcp.local.")
            port: Port number for the service
            service_name_suffix: Optional suffix for service name (default: uses service type)
        """
        try:
            # Get local hostname and IP
            hostname = socket.gethostname()
            # Use specified listen IP or auto-detect
            if self.listen_ip:
                local_ip = self.listen_ip
            else:
                local_ip = socket.gethostbyname(hostname)

            # Service configuration
            if service_name_suffix:
                service_name = f"NMOS {service_name_suffix} on {hostname}.{service_type}"
            else:
                service_name = f"NMOS Registry on {hostname}.{service_type}"

            # TXT record properties for NMOS service
            properties = {
                b"api_ver": b"v1.3",
                b"api_proto": b"http",
                b"pri": b"100",  # Priority for service discovery
            }

            # Create service info
            service_info = ServiceInfo(
                service_type,
                service_name,
                addresses=[socket.inet_aton(local_ip)],
                port=port,
                properties=properties,
                server=f"{hostname}.local."
            )

            # Register the service
            await self.async_zeroconf.async_register_service(service_info, strict=False)
            self.registered_services.append(service_info)
            logger.info(
                f"Hosting NMOS service: {service_name} at {local_ip}:{port} via mDNS for {service_type}"
            )
        except Exception as e:
            error_msg = f"Failed to register NMOS service: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            raise

    async def unregister_service(self):
        """Unregister all advertised NMOS services"""
        if self.registered_services and self.async_zeroconf:
            try:
                for service_info in self.registered_services:
                    await self.async_zeroconf.async_unregister_service(service_info)
                logger.info(f"Unregistered {len(self.registered_services)} NMOS service(s) from mDNS")
                self.registered_services.clear()
            except Exception as e:
                error_msg = f"Failed to unregister services: {e}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def start_periodic_announcements(self):
        """Start periodic mDNS service announcements"""
        if self.announcement_task:
            logger.warning("Periodic announcements already running")
            return

        self.announcement_task = asyncio.create_task(self._announce_service_periodically())
        logger.info("Periodic service announcements started")

    async def _announce_service_periodically(self):
        """Periodically announce the registration service via mDNS"""
        logger.info("Starting periodic service announcement loop")

        while self._running:
            try:
                # Wait 60 seconds between announcements (standard mDNS announcement interval)
                await asyncio.sleep(60)

                if self.registered_services and self.async_zeroconf:
                    # Update all services to trigger fresh mDNS announcements
                    for service_info in self.registered_services:
                        await self.async_zeroconf.async_update_service(service_info)
                    logger.debug(f"Announced {len(self.registered_services)} service(s) via mDNS")

            except asyncio.CancelledError:
                logger.info("Periodic service announcements cancelled")
                break
            except Exception as e:
                error_msg = f"Error announcing service: {e}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
                await asyncio.sleep(60)

    async def refresh_announcement(self):
        """Trigger an immediate mDNS service announcement"""
        if not self._running:
            logger.warning("Cannot refresh announcement: mDNS service not running")
            return
        
        try:
            if self.registered_services and self.async_zeroconf:
                for service_info in self.registered_services:
                    await self.async_zeroconf.async_update_service(service_info)
                logger.info(f"Manual announcement sent for {len(self.registered_services)} service(s) via mDNS")
        except Exception as e:
            error_msg = f"Error refreshing mDNS announcement: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
