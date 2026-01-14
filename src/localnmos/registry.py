"""
NMOS Registry with MDNS support

This module provides NMOS device discovery using mDNS/DNS-SD (Zeroconf).
It discovers IS-04 and IS-05 NMOS services on the local network.
"""

import argparse
import asyncio
import inspect
import logging
from platform import node
import sys
import time
import traceback
from typing import Callable, List, Optional, Dict, Any
import socket
from aiohttp import web
from aiohttp.web_response import json_response

from .nmos import InputChannel, InputDevice, NMOS_Device, NMOS_Node, OutputChannel, OutputDevice
from .error_log import ErrorLog
from .query_api import NMOSQueryAPI
from .system_config import NMOSSystemConfig
from .mdns_service import NMOSMDNSService
from .node_discovery import assign_temporary_channels_to_devices
from .node_discovery import register_node_from_health_update
from .channel_mapping import connect_channel_mapping, disconnect_channel_mapping
from localnmos import nmos

try:
    from zeroconf import Zeroconf, ServiceInfo
    from zeroconf.asyncio import AsyncZeroconf
except ImportError:
    # Graceful degradation if zeroconf is not installed
    Zeroconf = None
    ServiceInfo = None
    AsyncZeroconf = None

import aiohttp

from .logging_utils import create_logger

logger = create_logger(__name__)


def create_argument_parser():
    """Create and configure the argument parser for NMOS Registry"""
    parser = argparse.ArgumentParser(
        description='NMOS Registry with MDNS discovery support',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python -m localnmos --listen-ip=192.168.1.100
  python -m localnmos --debug-registry
  python -m localnmos --listen-ip=10.0.0.5 --debug-registry
        '''
    )
    parser.add_argument(
        '--listen-ip',
        type=str,
        default=None,
        metavar='IP_ADDRESS',
        help='IP address to bind the registry server to (default: auto-detect for mDNS, 0.0.0.0 for HTTP)'
    )
    parser.add_argument(
        '--debug-registry',
        action='store_true',
        help='Enable debug logging for registry operations'
    )
    return parser


# Global argument parser instance
_arg_parser = create_argument_parser()
_parsed_args = None


def get_parsed_args():
    """Get parsed command line arguments, parsing them if not already done"""
    global _parsed_args
    if _parsed_args is None:
        _parsed_args = _arg_parser.parse_args()
    return _parsed_args


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
        channel_updated_callback: Optional[Callable] = None,
        listen_ip: Optional[str] = None,
    ):
        """
        Initialize the NMOS registry

        Args:
            node_added_callback: Callback function called when a node is discovered
            node_removed_callback: Callback function called when a node is removed
            device_added_callback: Callback function called when a device is registered
            sender_added_callback: Callback function called when a sender is registered
            receiver_added_callback: Callback function called when a receiver is registered
            channel_updated_callback: Callback function called when device channels are updated
            listen_ip: IP address to bind to (overrides command line --listen-ip argument)
        """
        self.node_added_callback = node_added_callback
        self.node_removed_callback = node_removed_callback
        self.device_added_callback = device_added_callback
        self.sender_added_callback = sender_added_callback
        self.receiver_added_callback = receiver_added_callback
        self.channel_updated_callback = channel_updated_callback

        # Get parsed arguments
        args = get_parsed_args()
        
        # Use provided listen_ip or fall back to command line argument
        self.listen_ip = listen_ip or args.listen_ip
        
        # Store debug flag from command line arguments
        self.debug_registry = args.debug_registry

        self.mdns_service: Optional[NMOSMDNSService] = None
        self.zeroconf: Optional[Zeroconf] = None  # Direct reference for Query API
        self.nodes: Dict[str, NMOS_Node] = {}
        self._running = False
        self.registration_server = None  # HTTP server for device registrations
        self.registration_runner = None  # Runner for the HTTP server
        self.registration_port = 8080  # Port for registration service
        self.query_api: Optional[NMOSQueryAPI] = None  # Query API server instance
        self.system_config: Optional[NMOSSystemConfig] = None  # System Configuration API server instance
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


    async def connect_channel_mapping(self, sender_node: NMOS_Node, sender_device: NMOS_Device,
                                     output_dev: OutputDevice, output_chan: OutputChannel,
                                     receiver_node: NMOS_Node, receiver_device: NMOS_Device,
                                     input_dev: InputDevice, input_chan: InputChannel):
        """Connect an output channel to an input channel using IS-08 API"""
        await connect_channel_mapping(
            sender_node,
            sender_device,
            output_dev,
            output_chan,
            receiver_node,
            receiver_device,
            input_dev,
            input_chan,
            fetch_device_channels_callback=self.fetch_device_channels,
        )

    async def disconnect_channel_mapping(self, sender_node: NMOS_Node, sender_device: NMOS_Device,
                                        output_dev: OutputDevice, output_chan: OutputChannel):
        """Disconnect an output channel mapping using IS-08 API"""
        await disconnect_channel_mapping(
            sender_node,
            sender_device,
            output_dev,
            output_chan,
            fetch_device_channels_callback=self.fetch_device_channels,
        )

    async def _fetch_is08_resources(
        self,
        resource_type: str, # inputs or outputs
        node: NMOS_Node,
        session: aiohttp.ClientSession,
        sub_resources: List[str],
        device_constructor: Callable[[str, Dict[str, Any]], Any],
    ) -> List[Any]:
        """Generic helper to fetch IS-08 resources like inputs or outputs."""

        async def _fetch_details(resource_id: str) -> Optional[Dict[str, Any]]:
            """Helper to fetch all details for one IS-08 resource concurrently."""
            base_url = f"{node.channelmapping_url}/{resource_type}/{resource_id.rstrip('/')}"
            try:
                requests = {res.strip('/'): session.get(f"{base_url}{res}") for res in sub_resources}
                responses = await asyncio.gather(*requests.values(), return_exceptions=True)

                for res in sub_resources:
                    logger.info(f"=====> trying to read: {base_url}{res}")

                results: Dict[str, Any] = {}
                for i, key in enumerate(requests.keys()):
                    resp = responses[i]
                    if isinstance(resp, Exception):
                        error_msg = f"Error fetching {key} for {resource_type.rstrip('s')} {resource_id}: {resp}"
                        ErrorLog().add_error(error_msg, exception=resp if isinstance(resp, Exception) else None)
                        return None
                    if resp.status != 200:
                        error_msg = f"Failed to fetch {key} for {resource_type.rstrip('s')} {resource_id}, status: {resp.status}"
                        ErrorLog().add_error(error_msg)
                        return None
                    results[key] = await resp.json()
                return results
            except Exception as e:
                error_msg = f"Error processing details for {resource_type.rstrip('s')} {resource_id}: {e}"
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
                return None

        resources_url = f"{node.channelmapping_url}/{resource_type}"
        devices = []
        try:
            async with session.get(resources_url) as response:
                if response.status != 200:
                    error_msg = f"Failed to fetch {resource_type} list from {resources_url}: {response.status}"
                    ErrorLog().add_error(error_msg)
                    return []
                resource_ids = await response.json()
                if not isinstance(resource_ids, list):
                    error_msg = f"Expected a list of {resource_type} IDs from {resources_url}, but got {type(resource_ids)}"
                    ErrorLog().add_error(error_msg)
                    return []

            async def _details_and_construct(resource_id: str) -> Optional[Any]:
                details = await _fetch_details(resource_id)
                if details:
                    try:
                        return device_constructor(resource_id, details)
                    except Exception as e:
                        error_msg = f"Error constructing device for {resource_type.rstrip('s')} {resource_id}: {e}"
                        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
                        traceback.print_exception(e)
                return None

            tasks = [_details_and_construct(rid) for rid in resource_ids]
            results = await asyncio.gather(*tasks)

            devices = [res for res in results if res is not None]
            return devices

        except Exception as e:
            error_msg = f"Error fetching IS-08 {resource_type} for node {node.node_id}: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return []

    async def fetch_device_is08_inputs(self, node: NMOS_Node, device: NMOS_Device, session: aiohttp.ClientSession) -> List[InputDevice]:
        """Populates the input device list by calling the IS-08 endpoints."""

        def constructor(input_id: str, details: Dict[str, Any]) -> InputDevice:
            logger.info(f"Input {input_id} channels data: {details.get('channels', [])}")
            channels_data = details.get("channels", [])
            # Validate that channels is a list of dicts, not a dict or other type
            if not isinstance(channels_data, list):
                logger.warning(f"Input {input_id} channels data is not a list: {type(channels_data)}")
                channels_data = []
            input_channels = [
                InputChannel(id=ch.get("id", ""), label=ch.get("label", ""))
                for ch in channels_data
                if isinstance(ch, dict)  # Skip non-dict items
            ]
            
            # Safely extract properties, validating each level
            properties = details.get("properties", {})
            if not isinstance(properties, dict):
                properties = {}
            name = properties.get("name", "")
            description = properties.get("description", "")
            
            # Safely extract parent info
            parent = details.get("parent", {})
            if not isinstance(parent, dict):
                parent = {}
            parent_id = parent.get("id", "")
            parent_type = parent.get("type", "")
            
            # Safely extract caps
            caps = details.get("caps", {})
            if not isinstance(caps, dict):
                caps = {}
            reordering = caps.get("reordering", False)
            block_size = caps.get("block_size", 0)
            
            logger.info(f"retrieved input device {name}: {input_channels}, parent_id = {parent_id}, parent_type = {parent_type}")
            return InputDevice(
                id=input_id,
                name=name,
                description=description,
                reordering=reordering,
                block_size=block_size,
                parent_id=parent_id,
                parent_type=parent_type,
                channels=input_channels,
            )

        return await self._fetch_is08_resources(
            "inputs",
            node,
            session,
            ["/channels", "/properties", "/caps", "/parent"],
            constructor,
        )

    async def fetch_device_is08_outputs(self, node: NMOS_Node, device: NMOS_Device, session: aiohttp.ClientSession) -> List[OutputDevice]:
        """Populates the output device list by calling the IS-08 endpoints."""

        def constructor(output_id: str, details: Dict[str, Any]) -> OutputDevice:
            logger.info(f"Output {output_id} channels data: {details.get('channels', [])}")
            channels_data = details.get("channels", [])
            # Validate that channels is a list of dicts, not a dict or other type
            if not isinstance(channels_data, list):
                logger.warning(f"Output {output_id} channels data is not a list: {type(channels_data)}")
                channels_data = []
            output_channels = [
                OutputChannel(id=ch.get("id", ""), label=ch.get("label", ""), mapped_device=None, mapped_channel=None)
                for ch in channels_data
                if isinstance(ch, dict)  # Skip non-dict items
            ]
            
            # Safely extract properties, validating each level
            properties = details.get("properties", {})
            if not isinstance(properties, dict):
                properties = {}
            name = properties.get("name", "")
            description = properties.get("description", "")
            
            # Safely extract caps
            caps = details.get("caps", {})
            if not isinstance(caps, dict):
                caps = {}
            routable_inputs = caps.get("routable_inputs", [])
            if not isinstance(routable_inputs, list):
                routable_inputs = []
            
            # Source ID is typically a string, not a dict
            source_id = details.get("sourceid", "")
            if not isinstance(source_id, str):
                source_id = ""
            
            logger.info(f"retrieved output device {name}: {output_channels}")
            return OutputDevice(
                id=output_id,
                name=name,
                description=description,
                source_id=source_id,
                routable_inputs=routable_inputs,
                channels=output_channels,
            )

        return await self._fetch_is08_resources(
            "outputs",
            node,
            session,
            ["/channels", "/properties", "/caps", "/sourceid"],
            constructor,
        )


    async def fetch_device_is08_mapping(self, node: NMOS_Node, device: NMOS_Device, session: aiohttp.ClientSession, inputs: List[InputDevice], outputs: List[OutputDevice]):
        """
       Calls the /map/active endpoint to retrieve the mapping from outputs to inputs.
       Each item in the 'outputs' list is potentially mapped to an input device and channel index according to IS-08's channel mapping API.
        """
        mapping_url = f"{node.channelmapping_url}/map/active"
        try:
            async with session.get(mapping_url) as response:
                if response.status != 200:
                    error_msg = f"Failed to fetch channel mapping from {mapping_url}: {response.status}"
                    ErrorLog().add_error(error_msg)
                    return

                response_data = await response.json()
                
                logger.info(f"Active mapping response: {response_data}")

                # Extract the actual map from the response (it's nested under "map" key)
                active_map = response_data.get("map", {})
                
                if not active_map:
                    logger.warning(f"No 'map' key found in response: {response_data}")
                    return

                # Create lookup for input devices by ID
                input_devices_map: Dict[str, InputDevice] = {}
                for in_dev in inputs:
                    input_devices_map[in_dev.id] = in_dev
                    input_devices_map[in_dev.id.rstrip('/')] = in_dev

                # Create lookup for output devices by ID
                output_devices_map: Dict[str, OutputDevice] = {}
                for out_dev in outputs:
                    output_devices_map[out_dev.id] = out_dev
                    output_devices_map[out_dev.id.rstrip('/')] = out_dev

                # Apply the mapping
                # Structure: map -> output_device_id -> channel_id -> {input: input_device_id, channel_index: int}
                for output_dev_id, channels_map in active_map.items():
                    logger.info(f"Processing output device '{output_dev_id}' with channels: {list(channels_map.keys())}")
                    
                    # Find the output device
                    output_device = output_devices_map.get(output_dev_id) or output_devices_map.get(output_dev_id.rstrip('/'))
                    if not output_device:
                        logger.warning(f"Output device '{output_dev_id}' not found. Available: {list(output_devices_map.keys())}")
                        continue
                    
                    # Process each channel in this output device
                    for channel_id, mapping_info in channels_map.items():
                        input_dev_id = mapping_info.get("input")
                        channel_index = mapping_info.get("channel_index")
                        
                        logger.info(f"  Channel '{channel_id}': input_dev='{input_dev_id}', channel_index={channel_index}")
                        
                        # Find the output channel in this device
                        output_channel = None
                        for idx, out_chan in enumerate(output_device.channels):
                            # Match by ID or by index (channel_id might be a string index like "0", "1")
                            if out_chan.id == channel_id or out_chan.id.rstrip('/') == channel_id:
                                output_channel = out_chan
                                logger.debug(f"  Matched channel by ID: {out_chan.label}")
                                break
                            # If channel has no ID, match by numeric index
                            if not out_chan.id and str(idx) == channel_id:
                                output_channel = out_chan
                                logger.debug(f"  Matched channel by index {idx}: {out_chan.label}")
                                break
                        
                        if not output_channel:
                            logger.warning(f"  Output channel '{channel_id}' not found in device {output_device.name}")
                            continue
                        
                        # If there's a mapping, find the input device and channel
                        if input_dev_id:
                            input_device = input_devices_map.get(input_dev_id) or input_devices_map.get(input_dev_id.rstrip('/'))
                            
                            if input_device and channel_index is not None:
                                # Get the input channel by index
                                if 0 <= channel_index < len(input_device.channels):
                                    input_channel = input_device.channels[channel_index]
                                    output_channel.mapped_device = input_channel
                                    logger.info(f"  ✓✓✓ Mapped {output_device.name}/{output_channel.label} -> {input_device.name}/{input_channel.label}")
                                else:
                                    logger.warning(f"  Channel index {channel_index} out of range for input device {input_device.name} (has {len(input_device.channels)} channels)")
                            else:
                                if not input_device:
                                    logger.warning(f"  Input device '{input_dev_id}' not found. Available: {list(input_devices_map.keys())}")
                                else:
                                    logger.warning(f"  No channel_index specified for mapping")
                        else:
                            logger.debug(f"  No input mapping for {output_device.name}/{output_channel.label}")
        except Exception as e:
            error_msg = f"Error fetching or processing IS-08 mapping for device {device.device_id}: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


    async def fetch_device_channels(self, node: NMOS_Node, device: NMOS_Device):
        """Fetch channel information for a device using IS-08 Channel Mapping API"""
        try:
            logger.info(f"---------------------- Fetching channels for device {device.device_id} from node {node.node_id}")

            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                all_inputs = await self.fetch_device_is08_inputs(node, device, session)
                all_outputs = await self.fetch_device_is08_outputs(node, device, session)
                
                # Store all channels temporarily in the node
                # Clear existing temporary storage for this fetch
                node.temp_channel_inputs = all_inputs
                node.temp_channel_outputs = all_outputs
                
                logger.info(f"Stored {len(all_inputs)} inputs and {len(all_outputs)} outputs temporarily in node {node.node_id}")
                
                # Assign channels to devices based on source matching
                assign_temporary_channels_to_devices(node)
                
                # Get the channels that were assigned to this device
                inputs = device.is08_input_channels
                outputs = device.is08_output_channels
                
                logger.info(f"Device {device.device_id} has {len(device.sources)} sources, assigned {len(inputs)} inputs and {len(outputs)} outputs")
                
                await self.fetch_device_is08_mapping(node, device, session, inputs, outputs)

            logger.debug(f"---------------------- Found channels: {inputs}, {outputs}")

            # Notify UI about channel updates
            if self.channel_updated_callback:
                self._call_callback_with_params(self.channel_updated_callback, node.node_id, device.device_id)

        except Exception as e:
            error_msg = f"Error fetching channels for device {device.device_id}: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def refresh_mdns_announcement(self):
        """Trigger an immediate mDNS service announcement"""
        if self.mdns_service:
            await self.mdns_service.refresh_announcement()
        else:
            logger.warning("Cannot refresh announcement: mDNS service not initialized")

    async def start(self):
        """Start the NMOS registry and begin discovering devices"""
        if self._running:
            logger.warning("Registry already running")
            return

        if Zeroconf is None:
            error_msg = "zeroconf library not available. Install with: pip install zeroconf"
            ErrorLog().add_error(error_msg)
            return

        logger.info("Starting NMOS registry with MDNS discovery")
        self._running = True

        # Create and start mDNS service
        self.mdns_service = NMOSMDNSService(
            listen_ip=self.listen_ip,
            on_node_added=self._on_node_added,
            on_node_removed=self._on_node_removed
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

        await self.mdns_service.start(service_types)
        # Keep direct reference for Query API
        self.zeroconf = self.mdns_service.zeroconf

        # Host registration service for other devices to discover
        try:
            # Start HTTP servers first, then advertise them via mDNS
            await self._start_registration_server()
            await self._start_query_server()
            await self._start_system_config_server()
            
            # Now advertise the services via mDNS (for 1.3 clients)
            await self.mdns_service.register_service(self.NMOS_REGISTER_SERVICE, 8080, "Registration API")
            # for 1.2 clients
            await self.mdns_service.register_service(self.NMOS_REGISTRATION_SERVICE, 8080, "Registration API")
            # Advertise Query API
            await self.mdns_service.register_service(self.NMOS_QUERY_SERVICE, 8081, "Query API")
            # Advertise System Configuration API
            await self.mdns_service.register_service(self.NMOS_SYSTEM_SERVICE, 8082, "System API")
            
            # Start heartbeat monitoring
            self.heartbeat_task = asyncio.create_task(self._monitor_heartbeats())
            logger.info("Heartbeat monitoring started")
            # Start periodic service announcements
            await self.mdns_service.start_periodic_announcements()
        except Exception as e:
            error_msg = f"Failed to start registration service (non-fatal): {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

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

        # Stop HTTP registration server
        await self._stop_registration_server()

        # Stop HTTP Query API server
        await self._stop_query_server()

        # Stop HTTP System Configuration API server
        await self._stop_system_config_server()

        # Stop mDNS service
        if self.mdns_service:
            await self.mdns_service.stop()
            self.mdns_service = None
            self.zeroconf = None

        self.nodes.clear()

    async def _start_registration_server(self):
        """Start HTTP server to accept device registrations"""
        try:
            app = web.Application()
            app.router.add_post('/x-nmos/registration/{api_version}/health/nodes/{nodeId}', self._handle_health)
            app.router.add_post('/x-nmos/registration/{api_version}/resource', self._handle_registration)
            app.router.add_delete('/x-nmos/registration/{api_version}/resource/{resource_type}/{resource_id}', self._handle_deregistration)

            self.registration_runner = web.AppRunner(app)
            await self.registration_runner.setup()

            # Bind to specified IP or all interfaces
            bind_address = self.listen_ip or '0.0.0.0'
            site = web.TCPSite(self.registration_runner, bind_address, self.registration_port)
            await site.start()

            logger.info(f"Registration HTTP server started on {bind_address}:{self.registration_port}")
        except Exception as e:
            error_msg = f"Failed to start registration server: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _stop_registration_server(self):
        """Stop the HTTP registration server"""
        if self.registration_runner:
            try:
                await self.registration_runner.cleanup()
                logger.info("Registration HTTP server stopped")
                self.registration_runner = None
            except Exception as e:
                error_msg = f"Failed to stop registration server: {e}"
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _start_query_server(self):
        """Start NMOS Query API server"""
        try:
            self.query_api = NMOSQueryAPI(
                nodes=self.nodes,
                zeroconf=self.zeroconf,
                listen_ip=self.listen_ip,
                port=8081,
                service_type=self.NMOS_QUERY_SERVICE
            )
            await self.query_api.start()
        except Exception as e:
            error_msg = f"Failed to start Query API server: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _stop_query_server(self):
        """Stop the HTTP Query API server"""
        if self.query_api:
            try:
                await self.query_api.stop()
                self.query_api = None
            except Exception as e:
                error_msg = f"Failed to stop Query API server: {e}"
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _start_system_config_server(self):
        """Start NMOS System Configuration API server (IS-09)"""
        try:
            self.system_config = NMOSSystemConfig(
                zeroconf=self.zeroconf,
                listen_ip=self.listen_ip,
                port=8082,
                service_type=self.NMOS_SYSTEM_SERVICE
            )
            await self.system_config.start()
        except Exception as e:
            error_msg = f"Failed to start System Configuration API server: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _stop_system_config_server(self):
        """Stop the HTTP System Configuration API server"""
        if self.system_config:
            try:
                await self.system_config.stop()
                self.system_config = None
            except Exception as e:
                error_msg = f"Failed to stop System Configuration API server: {e}"
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _handle_health(self, request: web.Request):
        """ handle health update """
        try:
            nodeId = request.match_info.get('nodeId')
            logger.info(f"Received health update for node {nodeId} from {request.remote}")
            timestamp = time.time()
            self.device_heartbeats[nodeId] = timestamp

            if nodeId not in self.nodes:
                logger.info(f"Node {nodeId} not found in registry, registering from health update")
                await register_node_from_health_update(
                    request, nodeId, self.nodes, self._on_node_added
                )
                logger.info(f"After registration, nodes are: {list(self.nodes.keys())}")
            else:
                logger.debug(f"node already registered: {nodeId}")
            
            return json_response({'health': timestamp}, status=200)
        except Exception as e:
            error_msg = f"Error handling health update: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return self.error_json_response({'error': str(e)}, status=400)

    def error_json_response(self, err, status):
        error_msg = f"return error: {err} with status {status}"
        ErrorLog().add_error(error_msg)
        return json_response(err, status = status)

    def _handle_registration_node(self, request, resource_data: dict, api_version: str):
        # Register or re-register a node
        node_id = resource_data.get('id', 'unknown')
        label = resource_data.get('label', 'Unknown Device')

        # Try to get address from request
        client_host = request.remote

        # Extract port from API endpoints in registration data
        # NMOS nodes typically provide their API endpoints in the 'api' field
        client_port = 80  # Default fallback
        if 'api' in resource_data:
            api_info = resource_data['api']
            if 'endpoints' in api_info and len(api_info['endpoints']) > 0:
                # Parse the first endpoint URL to get the port
                endpoint = api_info['endpoints'][0]
                if 'port' in endpoint:
                    client_port = endpoint['port']
                elif 'host' in endpoint:
                    # Try to extract port from host string (e.g., "192.168.1.100:8080")
                    host_str = endpoint['host']
                    if ':' in host_str:
                        try:
                            client_port = int(host_str.split(':')[1])
                        except (ValueError, IndexError):
                            pass

        # Update heartbeat timestamp
        self.device_heartbeats[node_id] = time.time()

        # Check if this is a re-registration (device already exists)
        if node_id in self.nodes:
            # Re-registration: update existing device
            logger.info(f"Device re-registered: {label} ({node_id}) from {client_host}")
            existing_device = self.nodes[node_id]
            # Update properties with new data
            existing_device.properties.update(resource_data)
            return json_response({'status': 're-registered', 'id': node_id}, status=200)

        devices = []

        # New registration
        node = NMOS_Node(
            name=label,
            node_id=node_id,
            address=client_host,
            port=client_port,  # Default HTTP port, may be in data
            service_type='_nmos-register._tcp.local.',
            api_ver=api_version,
            properties=resource_data,
            devices=devices
        )

        logger.info(f"Device registered via HTTP: {label} ({node_id}) from {client_host}:{client_port}")
        # Add to devices and trigger callback
        self._on_node_added(node)
        return json_response({'status': 'registered', 'id': node_id}, status=201)


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
        dev = NMOS_Device(node_id=node_id, device_id=device_id, senders=senders, receivers=receivers, sources=[], is08_input_channels=[], is08_output_channels=[], label=label, description=description)
        node.devices.append(dev)

        # Notify UI about device addition
        if self.device_added_callback:
            self._call_callback_with_params(self.device_added_callback, node_id, device_id)

        return json_response({'status': 'registered'}, status=201)

    def find_device(self, device_id: str) -> NMOS_Device | None:
        for n in self.nodes.values():
            for d in n.devices:
                if d.device_id == device_id:
                    return d
        return None

    def _handle_registration_sender(self, request, resource_data: Dict):
        sender_id = resource_data.get('id', None)
        device_id = resource_data.get('device_id', None)
        label = resource_data.get('label', '')
        description = resource_data.get('description', '')
        flow_id = resource_data.get('flow_id', '')
        transport = resource_data.get('transport', '')
        manifest_href = resource_data.get('manifest_href', '')

        if sender_id is None:
            return self.error_json_response({'status': 'missing sender id'}, status=400)
        if device_id is None:
            return self.error_json_response({'status': 'missing device id'}, status=400)

        parent_device = self.find_device(device_id)
        if parent_device is None:
            return self.error_json_response({'status': f'sender-registration: bad device ID: {device_id}'}, status=400)

        # Check if sender already exists in this device
        for existing_sender in parent_device.senders:
            if existing_sender.sender_id == sender_id:
                logger.info(f"Sender {sender_id} already registered in device {device_id}, ignoring duplicate")
                return json_response({'status': 'already-registered'}, status=200)

        # Create NMOS_Sender object
        nmos_sender = nmos.NMOS_Sender(
            sender_id=sender_id,
            label=label,
            description=description,
            flow_id=flow_id,
            transport=transport,
            device_id=device_id,
            manifest_href=manifest_href
        )

        # Add sender to the device
        parent_device.senders.append(nmos_sender)
        logger.info(f"Added sender {sender_id} ({label}) to device {device_id}")

        # Notify UI about sender addition
        if self.sender_added_callback:
            self._call_callback_with_params(self.sender_added_callback, parent_device.node_id, device_id, sender_id)

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
            return self.error_json_response({'status': f'receiver-registration: bad device parent ID: {receiver_device_id}'}, status=400)

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
                            self._call_callback(self.node_added_callback, unknown_node)

                    # Create the sender device and add to unknown node
                    sender = NMOS_Device(
                        node_id=unknown_node_id,
                        device_id=sender_id,
                        senders=[],
                        receivers=[],
                        sources=[],
                        is08_input_channels=[],
                        is08_output_channels=[]
                    )
                    unknown_node.devices.append(sender)

                    # Notify UI about device addition
                    if self.device_added_callback:
                        self._call_callback_with_params(self.device_added_callback, unknown_node_id, sender_id)

                if self.debug_registry:
                    logger.info(f"linked receiver node {parent.node_id} to sender {sender.device_id}")
                parent.receivers.append(sender)
            else:
                logger.info("no subscriptions for receiver yet")
        else:
            logger.info("no subscriptions for receiver")

        # Notify UI about receiver addition
        if self.receiver_added_callback:
            self._call_callback_with_params(self.receiver_added_callback, parent.node_id, receiver_device_id, parent_device_id)

        return json_response({'status': 'registered'}, status=201)

    def _handle_registration_source(self, request, resource_data: dict):
        # Create a nmos.NMOS_Source object and add it to the parent device
        source_id = resource_data.get('id', None)
        label = resource_data.get('label', '')
        description = resource_data.get('description', '')
        format_type = resource_data.get('format', '')
        device_id = resource_data.get('device_id', None)
        parents = resource_data.get('parents', [])

        if source_id is None:
            return self.error_json_response({'status': 'missing source id'}, status=400)
        if device_id is None:
            return self.error_json_response({'status': 'missing device id'}, status=400)

        # Find the parent device
        parent_device = self.find_device(device_id)
        if parent_device is None:
            return self.error_json_response({'status': f'source-registration: bad device ID: {device_id}'}, status=400)

        # Check if source already exists in this device
        for existing_source in parent_device.sources:
            if existing_source.source_id == source_id:
                logger.info(f"Source {source_id} already registered in device {device_id}, ignoring duplicate")
                return json_response({'status': 'already-registered'}, status=200)

        # Create NMOS_Source object
        nmos_source = nmos.NMOS_Source(
            source_id=source_id,
            label=label,
            description=description,
            format=format_type,
            device_id=device_id,
            parents=parents
        )

        # Add source to the device
        parent_device.sources.append(nmos_source)
        logger.info(f"Added source {source_id} ({label}) to device {device_id}")

        # Find the node and fetch IS-08 channel information for all devices
        # Since IS-08 returns all inputs/outputs at the node level, we need to
        # re-fetch channels for all devices to properly associate them
        node = self.get_node_by_id(parent_device.node_id)
        if node is not None:
            # Fetch channels for all devices on this node
            for device in node.devices:
                asyncio.create_task(self.fetch_device_channels(node, device))

        return json_response({'status': 'registered'}, status=201)

    def _handle_registration_flow(self, request, resource_data: dict):
        # Register an NMOS Flow
        # Flows represent logical streams of media content
        flow_id = resource_data.get('id', None)
        label = resource_data.get('label', '')
        description = resource_data.get('description', '')
        format_type = resource_data.get('format', '')
        source_id = resource_data.get('source_id', None)
        device_id = resource_data.get('device_id', None)

        if flow_id is None:
            return self.error_json_response({'status': 'missing flow id'}, status=400)

        # Flows are typically associated with senders, but we can track them for reference
        # In NMOS, a sender sends a flow, and a flow is derived from a source
        logger.info(f"Flow registered: {flow_id} ({label}), source_id={source_id}, device_id={device_id}")

        # Note: Flows are not currently stored in the data model as they're primarily
        # referenced by senders. This handler acknowledges the registration.
        # If flow storage is needed in the future, add a flows list to NMOS_Device
        # and create an NMOS_Flow dataclass in nmos.py

        return json_response({'status': 'registered'}, status=201)

    def _handle_registration_unknown(self, request, resource_data: dict):
        return json_response({'status': 'registered'}, status=201)

    async def _handle_registration(self, request: aiohttp.ClientRequest):
        """Handle device registration and re-registration POST requests"""
        resource_type = 'unknown'
        resource_data = {}
        try:
            api_version = request.match_info.get('api_version', 'v1.3')
            data = await request.json()

            # Extract device information from registration data
            resource_type = data.get('type')
            resource_data = data.get('data', {})

            if self.debug_registry:
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
                    error_msg = f"unknown resource type: {resource_type}"
                    ErrorLog().add_error(error_msg)
                    return self._handle_registration_unknown(request, resource_data)

        except Exception as e:
            error_msg = f"Error handling registration: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            msg = str(e) + " for resource type " + resource_type + " with data " + str(await request.text())
            return self.error_json_response({'error': msg}, status=400)

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
            error_msg = f"Error handling deregistration: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=400)

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
                error_msg = f"Error in heartbeat monitoring: {e}"
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
                await asyncio.sleep(5)

    def _on_node_added(self, node: NMOS_Node):
        """Internal callback when a device is added"""
        # Filter out infrastructure nodes (query and system APIs)
        if node.service_type in ('_nmos-query._tcp.local.', '_nmos-system._tcp.local.'):
            logger.info(f"Ignoring infrastructure node: {node.name} (service_type={node.service_type})")
            return
        
        logger.info(
            f"Device discovered: {node.name} at {node.address}:{node.port}"
        )
        if node.node_id not in self.nodes:
            self.nodes[node.node_id] = node

        if self.node_added_callback:
            # Schedule callback in event loop
            self._call_callback(self.node_added_callback, node)

    def _on_node_removed(self, node: NMOS_Node):
        """Internal callback when a node is removed"""
        logger.info(f"Node removed: {node.name}")

        if node.node_id in self.nodes:
            del self.nodes[node.node_id]

        if self.node_removed_callback:
            self._call_callback(self.node_removed_callback, node)


    def _call_callback(self, callback: Callable, device: NMOS_Node):
        """Helper to call callbacks (handles both sync and async)"""
        if inspect.iscoroutinefunction(callback):
            # For async callbacks, schedule as a task
            try:
                asyncio.create_task(callback(device))
            except RuntimeError:
                # No event loop running, skip async callback
                logger.warning("Cannot call async callback: no event loop running")
        else:
            # For sync callbacks, call directly
            callback(device)

    def _call_callback_with_params(self, callback: Callable, *args):
        """Helper to call callbacks with parameters (handles both sync and async)"""
        if inspect.iscoroutinefunction(callback):
            # For async callbacks, schedule as a task
            try:
                asyncio.create_task(callback(*args))
            except RuntimeError:
                # No event loop running, skip async callback
                logger.warning("Cannot call async callback: no event loop running")
        else:
            # For sync callbacks, call directly
            callback(*args)
