"""
NMOS Registry with MDNS support

This module provides NMOS device discovery using mDNS/DNS-SD (Zeroconf).
It discovers IS-04 and IS-05 NMOS services on the local network.
"""

import asyncio
import inspect
import logging
import sys
import time
import traceback
from typing import Callable, List, Optional, Dict, Any
import socket
from aiohttp import web
from aiohttp.web_response import json_response

from .nmos import NMOS_Device, NMOS_Node

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


class NMOSServiceListener(ServiceListener):
    """Listens for NMOS services advertised via mDNS"""

    def __init__(self, on_device_added: Callable, on_device_removed: Callable):
        self.on_node_added = on_device_added
        self.on_device_removed = on_device_removed
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
            if self.on_device_removed:
                self.on_device_removed(device)


    def get_node_by_id(self, node_id:str) -> NMOS_Node|None:
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

        device_id=properties.get("node_id", info.name),
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
        devices=[]

        # we're pretty sure we've found a new Node, lets allocate it
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

        self.devices[info.name] = node

        if added and self.on_node_added:
            self.on_node_added(node)


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
        node_added_callback: Optional[Callable] = None,
        node_removed_callback: Optional[Callable] = None,
        device_added_callback: Optional[Callable] = None,
        sender_added_callback: Optional[Callable] = None,
        receiver_added_callback: Optional[Callable] = None,
    ):
        """
        Initialize the NMOS registry

        Args:
            node_added_callback: Callback function called when a node is discovered
            node_removed_callback: Callback function called when a node is removed
            device_added_callback: Callback function called when a device is registered
            sender_added_callback: Callback function called when a sender is registered
            receiver_added_callback: Callback function called when a receiver is registered
        """
        self.node_added_callback = node_added_callback
        self.node_removed_callback = node_removed_callback
        self.device_added_callback = device_added_callback
        self.sender_added_callback = sender_added_callback
        self.receiver_added_callback = receiver_added_callback

        self.zeroconf: Optional[Zeroconf] = None
        self.async_zeroconf: Optional[AsyncZeroconf] = None
        self.browsers = []
        self.listener: Optional[NMOSServiceListener] = None
        self.nodes: Dict[str, NMOS_Node] = {}
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

    async def fetch_device_channels(self, node: NMOS_Node, device: NMOS_Device):
        """Fetch channel information for a device using IS-08 Channel Mapping API"""
        try:
            # Query IS-08 API for channel mapping outputs (typical for devices with channels)
            outputs_url = f"{node.channelmapping_url}/map/outputs"
            inputs_url = f"{node.channelmapping_url}/map/inputs"
            
            channels = []
            
            async with aiohttp.ClientSession() as session:
                # Fetch output channels
                try:
                    async with session.get(outputs_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                        if response.status == 200:
                            outputs_data = await response.json()
                            for output in outputs_data:
                                # Filter channels for this specific device
                                if output.get('device_id') == device.device_id or 'device_id' not in output:
                                    channels.append({
                                        'type': 'output',
                                        'id': output.get('id'),
                                        'name': output.get('name', output.get('id')),
                                        'channel_index': output.get('channel_index'),
                                        'data': output
                                    })
                            logger.info(f"Fetched {len([c for c in channels if c['type'] == 'output'])} output channels for device {device.device_id}")
                except Exception as e:
                    logger.error(f"Could not fetch output channels from {outputs_url}: {e}")
                
                # Fetch input channels
                try:
                    async with session.get(inputs_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                        if response.status == 200:
                            inputs_data = await response.json()
                            for input_ch in inputs_data:
                                # Filter channels for this specific device
                                if input_ch.get('device_id') == device.device_id or 'device_id' not in input_ch:
                                    channels.append({
                                        'type': 'input',
                                        'id': input_ch.get('id'),
                                        'name': input_ch.get('name', input_ch.get('id')),
                                        'channel_index': input_ch.get('channel_index'),
                                        'data': input_ch
                                    })
                            logger.info(f"Fetched {len([c for c in channels if c['type'] == 'input'])} input channels for device {device.device_id}")
                except Exception as e:
                    logger.error(f"Could not fetch input channels from {inputs_url}: {e}")
            
            device.channels = channels
            return channels
            
        except Exception as e:
            logger.error(f"Error fetching channels for device {device.device_id}: {e}")
            return []

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
            on_device_added=self._on_node_added,
            on_device_removed=self._on_node_removed,
        )

        # Browse for NMOS services
        service_types = [
            self.NMOS_NODE_SERVICE,
            #self.NMOS_REGISTER_SERVICE,
            #self.NMOS_REGISTRATION_SERVICE,
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
            # for 1.3 clients
            await self._register_service(self.NMOS_REGISTER_SERVICE)
            # for 1.2 clients
            await self._register_service(self.NMOS_REGISTRATION_SERVICE)
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

        self.nodes.clear()

    async def _register_service(self, mdns_service_type: str):
        """Register and advertise NMOS registration service via mDNS"""
        try:
            # Get local hostname and IP
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)

            # Service configuration
            service_name = f"NMOS Registry on {hostname}.{mdns_service_type}"
            service_type = mdns_service_type
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
            await self.async_zeroconf.async_register_service(self.service_info, strict=False)
            logger.info(
                f"Hosting NMOS registration service: {service_name} at {local_ip}:{port} via mDNs for {service_type} "
            )
        except Exception as e:
            logger.error(f"Failed to register NMOS service: {e}")
            traceback.print_exception(e)
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
            app.router.add_post('/x-nmos/registration/{api_version}/health/nodes/{nodeId}', self._handle_health)
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

    async def _handle_health(self, request):
        """ handle health update """
        logger.info("Received health update")
        try:
            nodeId = request.match_info.get('nodeId')
            timestamp = time.time()
            self.device_heartbeats[nodeId] = timestamp
            return json_response({'health': timestamp}, status=200)
        except Exception as e:
            return self.error_json_response({'error': str(e)}, status=400)

    def error_json_response(self, err, status):
        logger.error(f"return error: {err} with status {status}")
        return json_response(err, status = status)

    def _handle_registration_node(self, request, resource_data: dict, api_version: str):
        # Register or re-register a node
        device_id = resource_data.get('id', 'unknown')
        label = resource_data.get('label', 'Unknown Device')

        # Try to get address from request
        client_host = request.remote

        # Update heartbeat timestamp
        self.device_heartbeats[device_id] = time.time()

        # Check if this is a re-registration (device already exists)
        if device_id in self.nodes:
            # Re-registration: update existing device
            logger.info(f"Device re-registered: {label} ({device_id}) from {client_host}")
            existing_device = self.nodes[device_id]
            # Update properties with new data
            existing_device.properties.update(resource_data)
            return json_response({'status': 're-registered', 'id': device_id}, status=200)

        devices = []

        # New registration
        device = NMOS_Node(
            name=label,
            node_id=device_id,
            address=client_host,
            port=80,  # Default HTTP port, may be in data
            service_type='_nmos-register._tcp.local.',
            api_ver=api_version,
            properties=resource_data,
            devices=devices
        )

        logger.info(f"Device registered via HTTP: {label} ({device_id}) from {client_host}")
        # Add to devices and trigger callback
        self._on_node_added(device)
        return json_response({'status': 'registered', 'id': device_id}, status=201)


    def get_node_by_id(self, node_id:str) -> NMOS_Node|None:
        for e in self.nodes.values():
            if e.node_id == node_id:
                return e
        return None

    def _handle_registration_device(self, request, resource_data: dict):
        device_id = resource_data.get('id', None)
        label = resource_data.get('label', 'no-label')
        description = resource_data.get('label', 'no-description')
        node_id = resource_data.get('node_id', None)
        # senders/receivers are obsolete here
        controls = resource_data.get('controls', None)

        if node_id is None:
            return self.error_json_response({'status': 'missing node id'}, status=404)
        if device_id is None:
            return self.error_json_response({'status': 'missing device id'}, status=404)

        node = self.get_node_by_id(node_id)
        if node is None:
            return self.error_json_response({'status': 'bad node id'}, status=404)
        
        # Check if device already exists in this node
        for existing_device in node.devices:
            if existing_device.device_id == device_id:
                logger.info(f"Device {device_id} already registered in node {node_id}, ignoring duplicate")
                return json_response({'status': 'already-registered'}, status=200)

        senders = []
        receivers = []
        dev = NMOS_Device(node_id=node_id, device_id=device_id, senders=senders,receivers=receivers)
        node.devices.append(dev)
        
        # Fetch channel information from IS-08 API
        asyncio.create_task(self.fetch_device_channels(node, dev))
        
        # Notify UI about device addition
        if self.device_added_callback:
            try:
                asyncio.create_task(
                    self._call_callback_with_params(self.device_added_callback, node_id, device_id)
                )
            except RuntimeError:
                self.device_added_callback(node_id, device_id)
        
        return json_response({'status': 'registered'}, status=201)

    def find_device(self, device_id: str) -> NMOS_Device | None:
        for n in self.nodes.values():
            for d in n.devices:
                if d.device_id == device_id:
                    return d
        return None

    def _handle_registration_sender(self, request, resource_data: Dict):
        parent_device_id = resource_data.get('id', None)
        sender_device_id = resource_data.get('device_id', None)
        flow_id = resource_data.get('flow_id', None)
        subscriptions = resource_data.get('subscription', None)

        if parent_device_id is None:
            return self.error_json_response({'status': 'parent id not found'}, status=400)
        if sender_device_id is None:
            return self.error_json_response({'status': 'parent id not found'}, status=400)
        if sender_device_id == parent_device_id:
            return self.error_json_response({'status': 'parent id == sender parent id'}, status=500)

        parent = self.find_device(sender_device_id)
        if parent is None:
            return self.error_json_response({'status': 'bad parent ID'}, status=400)
        
        # Check if sender already exists in this device
        for existing_sender in parent.senders:
            if hasattr(existing_sender, 'device_id') and existing_sender.device_id == parent_device_id:
                logger.info(f"Sender {parent_device_id} already registered in device {sender_device_id}, ignoring duplicate")
                return json_response({'status': 'already-registered'}, status=200)

        if subscriptions is not None:
            receiver_id = subscriptions.get("receiver_id", "missing")
            if receiver_id == "missing":
                return self.error_json_response({'status': 'missing receiver ID'}, status=400)

            if receiver_id is not None:
                receiver = self.find_device(receiver_id)
                if receiver is None:
                    return self.error_json_response({'status': f'bad receiver ID:{receiver_id}'}, status=400)

                logger.info(f"linked sender node {parent.node_id} to receiver {receiver.device_id}")
                parent.senders.append(receiver)
            else:
                logger.info("no subscriptions for sender yet")
        else:
            logger.info("no subscriptions for sender")
        
        # Notify UI about sender addition
        if self.sender_added_callback:
            try:
                asyncio.create_task(
                    self._call_callback_with_params(self.sender_added_callback, parent.node_id, sender_device_id, parent_device_id)
                )
            except RuntimeError:
                self.sender_added_callback(parent.node_id, sender_device_id, parent_device_id)

        return json_response({'status': 'registered'}, status=201)

    def _handle_registration_receiver(self, request, resource_data: dict):
        parent_device_id = resource_data.get('id', None)
        receiver_device_id = resource_data.get('device_id', None)
        flow_id = resource_data.get('flow_id', None)
        subscriptions = resource_data.get('subscription', None)

        if parent_device_id is None:
            return self.error_json_response({'status': 'parent id not found'}, status=400)
        if receiver_device_id is None:
            return self.error_json_response({'status': 'receiver device id not found'}, status=400)
        if receiver_device_id == parent_device_id:
            return self.error_json_response({'status': 'parent id == receiver parent id'}, status=500)

        parent = self.find_device(receiver_device_id)
        if parent is None:
            return self.error_json_response({'status': 'bad parent ID'}, status=400)
        
        # Check if receiver already exists in this device
        for existing_receiver in parent.receivers:
            if hasattr(existing_receiver, 'device_id') and existing_receiver.device_id == parent_device_id:
                logger.info(f"Receiver {parent_device_id} already registered in device {receiver_device_id}, ignoring duplicate")
                return json_response({'status': 'already-registered'}, status=200)

        if subscriptions is not None:
            sender_id = subscriptions.get("sender_id", "missing")
            if sender_id == "missing":
                return self.error_json_response({'status': 'missing sender ID'}, status=400)

            if sender_id is not None:
                sender = self.find_device(sender_id)
                if sender is None:
                    # Create unknown node and device for unknown sender device
                    logger.warning(f"Unknown sender_id {sender_id} for receiver, creating unknown node and device")
                    
                    # Create or get unknown node
                    unknown_node_id = f"unknown-node-{sender_id}"
                    unknown_node = self.get_node_by_id(unknown_node_id)
                    
                    if unknown_node is None:
                        # Create new unknown node
                        client_host = request.remote
                        unknown_node = NMOS_Node(
                            name="unknown",
                            node_id=unknown_node_id,
                            address=client_host,
                            port=80,
                            service_type='_nmos-register._tcp.local.',
                            api_ver="v1.3",
                            properties={},
                            devices=[]
                        )
                        self.nodes[unknown_node_id] = unknown_node
                        
                        # Notify about new node
                        if self.node_added_callback:
                            try:
                                asyncio.create_task(
                                    self._call_callback(self.node_added_callback, unknown_node)
                                )
                            except RuntimeError:
                                self.node_added_callback(unknown_node)
                    
                    # Create the sender device and add to unknown node
                    sender = NMOS_Device(
                        node_id=unknown_node_id,
                        device_id=sender_id,
                        senders=[],
                        receivers=[]
                    )
                    unknown_node.devices.append(sender)
                    
                    # Notify UI about device addition
                    if self.device_added_callback:
                        try:
                            asyncio.create_task(
                                self._call_callback_with_params(self.device_added_callback, unknown_node_id, sender_id)
                            )
                        except RuntimeError:
                            self.device_added_callback(unknown_node_id, sender_id)

                logger.info(f"linked receiver node {parent.node_id} to sender {sender.device_id}")
                parent.receivers.append(sender)
            else:
                logger.info("no subscriptions for receiver yet")
        else:
            logger.info("no subscriptions for receiver")
        
        # Notify UI about receiver addition
        if self.receiver_added_callback:
            try:
                asyncio.create_task(
                    self._call_callback_with_params(self.receiver_added_callback, parent.node_id, receiver_device_id, parent_device_id)
                )
            except RuntimeError:
                self.receiver_added_callback(parent.node_id, receiver_device_id, parent_device_id)
        
        return json_response({'status': 'registered'}, status=201)

    def _handle_registration_source(self, request, resource_data: dict):
        return json_response({'status': 'registered'}, status=201)

    def _handle_registration_flow(self, request, resource_data: dict):
        return json_response({'status': 'registered'}, status=201)

    def _handle_registration_unknown(self, request, resource_data: dict):
        return json_response({'status': 'registered'}, status=201)

    async def _handle_registration(self, request):
        """Handle device registration and re-registration POST requests"""
        try:
            api_version = request.match_info.get('api_version', 'v1.3')
            data = await request.json()

            # Extract device information from registration data
            resource_type = data.get('type')
            resource_data = data.get('data', {})

            logger.info(f"Received registration request: {resource_type}")

            match resource_type:
                case 'node':
                    return self._handle_registration_node(request, resource_data, api_version)
                case 'device':
                    return self._handle_registration_device(request, resource_data)
                case 'sender':
                    return self._handle_registration_sender(request, resource_data)
                case 'receiver':
                    return self._handle_registration_receiver(request, resource_data)
                case 'flow':
                    return self._handle_registration_flow(request, resource_data)
                case 'source':
                    return self._handle_registration_source(request, resource_data)
                case _:
                    logger.error(f"unknown resource type: {resource_type}")
                    return self._handle_registration_unknown(request, resource_data)

        except Exception as e:
            logger.error(f"Error handling registration: {e}")
            return self.error_json_response({'error': str(e)}, status=400)

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
            if resource_id in self.nodes:
                device = self.nodes[resource_id]
                self._on_node_removed(device)

            return json_response({'status': 'deregistered'}, status=204)

        except Exception as e:
            logger.error(f"Error handling deregistration: {e}")
            return json_response({'error': str(e)}, status=400)

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

                    if device_id in self.nodes:
                        device = self.nodes[device_id]
                        logger.info(f"Removing expired device: {device.name} ({device_id})")
                        self._on_node_removed(device)

                # Check every 5 seconds
                await asyncio.sleep(5)

            except asyncio.CancelledError:
                logger.info("Heartbeat monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"Error in heartbeat monitoring: {e}")
                await asyncio.sleep(5)

    def _on_node_added(self, node: NMOS_Node):
        """Internal callback when a device is added"""
        logger.info(
            f"Device discovered: {node.name} at {node.address}:{node.port}"
        )
        self.nodes[node.node_id] = node

        if self.node_added_callback:
            # Schedule callback in event loop
            try:
                asyncio.create_task(
                    self._call_callback(self.node_added_callback, node)
                )
            except RuntimeError:
                # If not in async context, call directly
                self.node_added_callback(node)

    def _on_node_removed(self, node: NMOS_Node):
        """Internal callback when a node is removed"""
        logger.info(f"Node removed: {node.name}")

        if node.node_id in self.nodes:
            del self.nodes[node.node_id]

        if self.node_removed_callback:
            try:
                asyncio.create_task(
                    self._call_callback(self.node_removed_callback, node)
                )
            except RuntimeError:
                self.node_removed_callback(node)


    async def _call_callback(self, callback: Callable, device: NMOS_Node):
        """Helper to call callbacks asynchronously"""
        if inspect.iscoroutinefunction(callback):
            await callback(device)
        else:
            callback(device)
    
    async def _call_callback_with_params(self, callback: Callable, *args):
        """Helper to call callbacks with multiple parameters asynchronously"""
        if inspect.iscoroutinefunction(callback):
            await callback(*args)
        else:
            callback(*args)
    
    async def _call_callback_with_params(self, callback: Callable, *args):
        """Helper to call callbacks with multiple parameters asynchronously"""
        if inspect.iscoroutinefunction(callback):
            await callback(*args)
        else:
            callback(*args)
