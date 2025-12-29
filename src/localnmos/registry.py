"""
NMOS Registry with MDNS support

This module provides NMOS device discovery using mDNS/DNS-SD (Zeroconf).
It discovers IS-04 and IS-05 NMOS services on the local network.
"""

import asyncio
import logging
import sys
import time
from typing import Callable, Optional, Dict, Any
from dataclasses import dataclass
import socket
from aiohttp import web

try:
    from zeroconf import ServiceBrowser, ServiceListener, Zeroconf, ServiceInfo, InterfaceChoice
    from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser, AsyncServiceInfo
except ImportError:
    # Graceful degradation if zeroconf is not installed
    ServiceListener = object
    Zeroconf = None
    AsyncZeroconf = None
    InterfaceChoice = None

import aiohttp


logger = logging.getLogger(__name__)

# Configure logging to output to console
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)


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
    senders: list = None
    receivers: list = None

    def __post_init__(self):
        if self.properties is None:
            self.properties = {}
        if self.senders is None:
            self.senders = []
        if self.receivers is None:
            self.receivers = []

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
        # MT48 and other devices may advertise different API versions
        api_ver = properties.get("api_ver", "v1.3")
        api_proto = properties.get("api_proto", "http")
        
        # Log discovered properties for debugging MT48 and other devices
        logger.debug(f"Device {info.name} properties: {properties}")

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
    NMOS_REGISTER_SERVICE = "_nmos-register._tcp.local."
    

    # Additional service types for broader compatibility (e.g., MT48 devices)
    NMOS_CONNECTION_SERVICE = "_nmos-connection._tcp.local."  # IS-05 Connection API
    NMOS_CHANNELMAPPING_SERVICE = "_nmos-channelmapping._tcp.local."  # IS-08 Channel Mapping
    NMOS_SYSTEM_SERVICE = "_nmos-system._tcp.local."  # IS-09 System Parameters

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
        self.service_info: Optional[ServiceInfo] = None  # For hosting registration service
        self.registration_server = None  # HTTP server for device registrations
        self.registration_runner = None  # Runner for the HTTP server
        self.registration_port = 8080  # Port for registration service
        self.device_heartbeats: Dict[str, float] = {}  # Track last heartbeat time for each device
        self.heartbeat_timeout = 12.0  # Timeout in seconds (NMOS default is 12s)
        self.heartbeat_task = None  # Background task to check for expired registrations
        self.announcement_task = None  # Background task for periodic service announcements

    async def connect_sender_to_receiver(self, sender_id: str, receiver_id: str):
        """Connect sender to receiver using IS-05 API"""        
        pass

    async def disconnect_sender_from_receiver(self, sender_id: str, receiver_id: str):
        """Disconnect sender from receiver using IS-05 API"""
        pass

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
        # mDNS operates on UDP port 5353 for DNS-SD service discovery
        # Use InterfaceChoice.All to ensure we respond to mDNS queries on all network interfaces
        self.async_zeroconf = AsyncZeroconf(interfaces=InterfaceChoice.All)
        self.zeroconf = self.async_zeroconf.zeroconf
        assert self.zeroconf is not None
        logger.info(f"Zeroconf initialized on all interfaces - listening on UDP port 5353 for mDNS/DNS-SD")

        # Create service listener
        self.listener = NMOSServiceListener(
            on_device_added=self._on_device_added,
            on_device_removed=self._on_device_removed,
        )

        # Browse for NMOS services
        service_types = [
            self.NMOS_NODE_SERVICE,
            #self.NMOS_REGISTER_SERVICE,
            self.NMOS_REGISTRATION_SERVICE,
            self.NMOS_QUERY_SERVICE,
            self.NMOS_CONNECTION_SERVICE,
            self.NMOS_CHANNELMAPPING_SERVICE,
            self.NMOS_SYSTEM_SERVICE,
        ]

        for service_type in service_types:
            browser = ServiceBrowser(self.zeroconf, service_type, self.listener)
            self.browsers.append(browser)
            logger.info(f"Browsing for {service_type} via mDNS on port 5353")

        # Host registration service for other devices to discover
        try:
            await self._register_service()
            await self._start_registration_server()
            # Start heartbeat monitoring
            self.heartbeat_task = asyncio.create_task(self._monitor_heartbeats())
            logger.info("Heartbeat monitoring started")
            # Start periodic service announcements
            self.announcement_task = asyncio.create_task(self._announce_service_periodically())
            logger.info("Periodic service announcements started")
        except Exception as e:
            logger.error(f"Failed to start registration service (non-fatal): {e}")

    async def stop(self):
        """Stop the NMOS registry and cleanup resources"""
        if not self._running:
            return

        logger.info("Stopping NMOS registry")
        self._running = False

        # Stop heartbeat monitoring
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            self.heartbeat_task = None

        # Stop periodic announcements
        if self.announcement_task:
            self.announcement_task.cancel()
            try:
                await self.announcement_task
            except asyncio.CancelledError:
                pass
            self.announcement_task = None

        # Stop HTTP registration server
        await self._stop_registration_server()

        # Unregister hosted service
        await self._unregister_service()

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

    async def _register_service(self):
        """Register and advertise NMOS registration service via mDNS"""
        try:
            # Get local hostname and IP
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            
            # Service configuration
            service_name = f"NMOS Registry on {hostname}.{self.NMOS_REGISTER_SERVICE}"  
            service_type = self.NMOS_REGISTER_SERVICE
            port = 8080  # Default NMOS registration API port
            
            # TXT record properties for NMOS service
            properties = {
                b"api_ver": b"v1.3",
                b"api_proto": b"http",
                b"pri": b"100",  # Priority
            }
            
            # Create service info
            self.service_info = ServiceInfo(
                service_type,
                service_name,
                addresses=[socket.inet_aton(local_ip)],
                port=port,
                properties=properties,
                server=f"{hostname}.local."
            )
            
            # Register the service
            await self.async_zeroconf.async_register_service(self.service_info)
            logger.info(
                f"Hosting NMOS registration service: {service_name} at {local_ip}:{port} via mDNs for {service_type} "
            )
        except Exception as e:
            logger.error(f"Failed to register NMOS service: {e}")
            sys.exit(1)

    async def _unregister_service(self):
        """Unregister the advertised NMOS registration service"""
        if self.service_info and self.async_zeroconf:
            try:
                await self.async_zeroconf.async_unregister_service(self.service_info)
                logger.info("Unregistered NMOS registration service")
                self.service_info = None
            except Exception as e:
                logger.error(f"Failed to unregister service: {e}")

    async def _start_registration_server(self):
        """Start HTTP server to accept device registrations"""
        try:
            app = web.Application()
            app.router.add_post('/x-nmos/registration/{api_version}/resource', self._handle_registration)
            app.router.add_delete('/x-nmos/registration/{api_version}/resource/{resource_type}/{resource_id}', self._handle_deregistration)
            
            self.registration_runner = web.AppRunner(app)
            await self.registration_runner.setup()
            
            site = web.TCPSite(self.registration_runner, '0.0.0.0', self.registration_port)
            await site.start()
            
            logger.info(f"Registration HTTP server started on port {self.registration_port}")
        except Exception as e:
            logger.error(f"Failed to start registration server: {e}")

    async def _stop_registration_server(self):
        """Stop the HTTP registration server"""
        if self.registration_runner:
            try:
                await self.registration_runner.cleanup()
                logger.info("Registration HTTP server stopped")
                self.registration_runner = None
            except Exception as e:
                logger.error(f"Failed to stop registration server: {e}")

    async def _handle_registration(self, request):
        """Handle device registration and re-registration POST requests"""
        logger.info("Received registration request")  
        try:
            api_version = request.match_info.get('api_version', 'v1.3')
            data = await request.json()
            
            # Extract device information from registration data
            resource_type = data.get('type')
            resource_data = data.get('data', {})
            
            if resource_type == 'node':
                # Register or re-register a node
                device_id = resource_data.get('id', 'unknown')
                label = resource_data.get('label', 'Unknown Device')
                
                # Try to get address from request
                client_host = request.remote
                
                # Update heartbeat timestamp
                self.device_heartbeats[device_id] = time.time()
                
                # Check if this is a re-registration (device already exists)
                if device_id in self.devices:
                    # Re-registration: update existing device
                    logger.info(f"Device re-registered: {label} ({device_id}) from {client_host}")
                    existing_device = self.devices[device_id]
                    # Update properties with new data
                    existing_device.properties.update(resource_data)
                    return web.json_response({'status': 're-registered', 'id': device_id}, status=200)
                else:
                    # New registration
                    device = NMOSDevice(
                        name=label,
                        device_id=device_id,
                        address=client_host,
                        port=80,  # Default HTTP port, may be in data
                        service_type='_nmos-register._tcp.local.',
                        api_ver=api_version,
                        properties=resource_data
                    )
                    
                    logger.info(f"Device registered via HTTP: {label} ({device_id}) from {client_host}")
                    
                    # Add to devices and trigger callback
                    self._on_device_added(device)
                    
                    return web.json_response({'status': 'registered', 'id': device_id}, status=201)
            else:
                # Handle other resource types (senders, receivers, etc.)
                resource_id = resource_data.get('id', 'unknown')
                self.device_heartbeats[resource_id] = time.time()
                logger.info(f"Received {resource_type} registration: {resource_id}")
                return web.json_response({'status': 'registered'}, status=201)
                
        except Exception as e:
            from aiohttp import web
            logger.error(f"Error handling registration: {e}")
            return web.json_response({'error': str(e)}, status=400)

    async def _handle_deregistration(self, request):
        """Handle device deregistration DELETE requests"""
        logger.info("Received deregistration request")
        try:            
            resource_type = request.match_info.get('resource_type')
            resource_id = request.match_info.get('resource_id')
            
            logger.info(f"Deregistration request: {resource_type}/{resource_id}")
            
            # Remove from heartbeat tracking
            if resource_id in self.device_heartbeats:
                del self.device_heartbeats[resource_id]
            
            # Find and remove device if it exists
            if resource_id in self.devices:
                device = self.devices[resource_id]
                self._on_device_removed(device)
            
            return web.json_response({'status': 'deregistered'}, status=204)
            
        except Exception as e:
            from aiohttp import web
            logger.error(f"Error handling deregistration: {e}")
            return web.json_response({'error': str(e)}, status=400)

    async def _announce_service_periodically(self):
        """Periodically announce the registration service via mDNS"""
        logger.info("Starting periodic service announcement loop")
        
        while self._running:
            try:
                # Wait 60 seconds between announcements (standard mDNS announcement interval)
                await asyncio.sleep(60)
                
                if self.service_info and self.async_zeroconf:
                    # Update the service to trigger a fresh mDNS announcement
                    await self.async_zeroconf.async_update_service(self.service_info)
                    logger.debug("Service announcement sent via mDNS")
                    
            except asyncio.CancelledError:
                logger.info("Periodic service announcements cancelled")
                break
            except Exception as e:
                logger.error(f"Error announcing service: {e}")
                await asyncio.sleep(60)

    async def _monitor_heartbeats(self):
        """Monitor device heartbeats and remove devices that haven't re-registered"""
        logger.info("Starting heartbeat monitoring loop")
        
        while self._running:
            try:
                current_time = time.time()
                expired_devices = []
                
                # Check all registered devices for expired heartbeats
                for device_id, last_heartbeat in list(self.device_heartbeats.items()):
                    time_since_heartbeat = current_time - last_heartbeat
                    
                    if time_since_heartbeat > self.heartbeat_timeout:
                        logger.warning(
                            f"Device {device_id} heartbeat expired "
                            f"({time_since_heartbeat:.1f}s > {self.heartbeat_timeout}s)"
                        )
                        expired_devices.append(device_id)
                
                # Remove expired devices
                for device_id in expired_devices:
                    if device_id in self.device_heartbeats:
                        del self.device_heartbeats[device_id]
                    
                    if device_id in self.devices:
                        device = self.devices[device_id]
                        logger.info(f"Removing expired device: {device.name} ({device_id})")
                        self._on_device_removed(device)
                
                # Check every 5 seconds
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                logger.info("Heartbeat monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"Error in heartbeat monitoring: {e}")
                await asyncio.sleep(5)

    def _on_device_added(self, device: NMOSDevice):
        """Internal callback when a device is added"""
        logger.info(
            f"Device discovered: {device.name} at {device.address}:{device.port}"
        )
        self.devices[device.device_id] = device

        # Query device for senders and receivers
        try:
            asyncio.create_task(self._query_device_resources(device))
        except RuntimeError:
            # If not in async context, skip querying
            logger.warning(f"Cannot query device {device.name} - not in async context")

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

    async def _query_device_resources(self, device: NMOSDevice):
        """Query device for senders and receivers after discovery"""
        logger.info(f"Querying {device.name} for senders and receivers...")
        
        # Query senders
        device.senders = await self.query_device_senders(device)
        
        # Query receivers
        device.receivers = await self.query_device_receivers(device)
        
        logger.info(
            f"Device {device.name}: {len(device.senders)} senders, "
            f"{len(device.receivers)} receivers"
        )

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

    async def query_device_senders(self, device: NMOSDevice) -> list:
        """
        Query a device's senders
        
        Args:
            device: The NMOS device to query
            
        Returns:
            List of sender objects or empty list if query fails
        """
        result = await self.query_device(device, "/senders")
        if result and isinstance(result, list):
            logger.info(f"Device {device.name} has {len(result)} sender(s)")
            return result
        return []

    async def query_device_receivers(self, device: NMOSDevice) -> list:
        """
        Query a device's receivers
        
        Args:
            device: The NMOS device to query
            
        Returns:
            List of receiver objects or empty list if query fails
        """
        result = await self.query_device(device, "/receivers")
        if result and isinstance(result, list):
            logger.info(f"Device {device.name} has {len(result)} receiver(s)")
            return result
        return []
