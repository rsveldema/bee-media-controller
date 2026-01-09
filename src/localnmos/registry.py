"""
NMOS Registry with MDNS support

This module provides NMOS device discovery using mDNS/DNS-SD (Zeroconf).
It discovers IS-04 and IS-05 NMOS services on the local network.
"""

import argparse
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

from .nmos import InputChannel, InputDevice, NMOS_Device, NMOS_Node, OutputChannel, OutputDevice
from .error_log import ErrorLog

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

    async def connect_channel_mapping(self, sender_node: NMOS_Node, sender_device: NMOS_Device,
                                     output_dev: OutputDevice, output_chan: OutputChannel,
                                     receiver_node: NMOS_Node, receiver_device: NMOS_Device,
                                     input_dev: InputDevice, input_chan: InputChannel):
        """Connect an output channel to an input channel using IS-08 API"""
        try:
            # Build the IS-08 channel mapping API URL for activations
            activations_url = f"{sender_node.channelmapping_url}/map/activations"
            
            # Find the input channel index within its device
            input_channel_index = None
            for idx, chan in enumerate(input_dev.channels):
                if chan is input_chan or chan.id == input_chan.id or (not chan.id and chan.label == input_chan.label):
                    input_channel_index = idx
                    break
            
            if input_channel_index is None:
                error_msg = f"Could not find channel index for input {input_chan.label} (ID: {input_chan.id}) in device {input_dev.id}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg)
                return
            
            # Find the output channel key (ID or index as string)
            # According to IS-08 spec, the key is the channel ID if present, otherwise use the channel index
            output_chan_key = output_chan.id
            if not output_chan_key:
                # Find the output channel index
                for idx, chan in enumerate(output_dev.channels):
                    if chan is output_chan or (not chan.id and chan.label == output_chan.label):
                        output_chan_key = str(idx)
                        break
                if not output_chan_key:
                    error_msg = f"Could not determine output channel key for {output_chan.label}"
                    logger.error(error_msg)
                    ErrorLog().add_error(error_msg)
                    return
            
            logger.info(f"Mapping output {output_dev.id}/{output_chan_key} (label: {output_chan.label}) to input {input_dev.id} channel index {input_channel_index} (label: {input_chan.label})")
            
            # Prepare the activation request according to IS-08 spec
            # Use immediate activation with the output-to-input channel mapping
            # Strip trailing slashes from device IDs
            output_dev_id_clean = output_dev.id.rstrip('/')
            input_dev_id_clean = input_dev.id.rstrip('/')
            
            activation_data = {
                "activation": {
                    "mode": "activate_immediate"
                },
                "action:": { # note the extra ':' here is intentional as per IS-08 spec
                    output_dev_id_clean: {
                        output_chan_key: {
                            "input": input_dev_id_clean,
                            "channel_index": input_channel_index
                        }
                    }
                }
            }
            
            async with aiohttp.ClientSession() as session:
                # POST request to /map/activations for immediate activation
                async with session.post(activations_url, json=activation_data) as response:
                    if response.status in [200, 202]:
                        logger.info(f"Successfully mapped output channel {output_chan.label} to input channel {input_chan.label}")
                        # Refresh the channel data
                        await self.fetch_device_channels(sender_node, sender_device)
                    else:
                        error_text = await response.text()
                        error_msg = f"Failed to set channel mapping: {response.status} - {error_text}: tried to post: {activation_data}"
                        logger.error(error_msg)
                        ErrorLog().add_error(error_msg)
        except Exception as e:
            error_msg = f"Error connecting channel mapping: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def disconnect_channel_mapping(self, sender_node: NMOS_Node, sender_device: NMOS_Device,
                                        output_dev: OutputDevice, output_chan: OutputChannel):
        """Disconnect an output channel mapping using IS-08 API"""
        try:
            # Build the IS-08 channel mapping API URL for activations
            activations_url = f"{sender_node.channelmapping_url}/map/activations"
            
            # Find the output channel key (ID or index as string)
            output_chan_key = output_chan.id
            if not output_chan_key:
                # Find the output channel index
                for idx, chan in enumerate(output_dev.channels):
                    if chan is output_chan or (not chan.id and chan.label == output_chan.label):
                        output_chan_key = str(idx)
                        break
                if not output_chan_key:
                    error_msg = f"Could not determine output channel key for {output_chan.label}"
                    logger.error(error_msg)
                    ErrorLog().add_error(error_msg)
                    return
            
            # Strip trailing slashes from device IDs
            output_dev_id_clean = output_dev.id.rstrip('/')
            
            # Prepare the activation request to clear the mapping
            # Setting input to null clears the mapping
            activation_data = {
                "activation": {
                    "mode": "activate_immediate"
                },
                "action:": {  # note the extra ':' here is intentional as per IS-08 spec
                    output_dev_id_clean: {
                        output_chan_key: {
                            "input": None,
                            "channel_index": None
                        }
                    }
                }
            }
            
            async with aiohttp.ClientSession() as session:
                # POST request to /map/activations for immediate activation
                async with session.post(activations_url, json=activation_data) as response:
                    if response.status in [200, 202]:
                        logger.info(f"Successfully cleared mapping for output channel {output_chan.label}")
                        # Refresh the channel data
                        await self.fetch_device_channels(sender_node, sender_device)
                    else:
                        error_text = await response.text()
                        error_msg = f"Failed to clear channel mapping: {response.status} - {error_text}"
                        logger.error(error_msg)
                        ErrorLog().add_error(error_msg)
        except Exception as e:
            error_msg = f"Error disconnecting channel mapping: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

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
            base_url = f"{node.channelmapping_url}/{resource_type}/{resource_id}"
            try:
                requests = {res.strip('/'): session.get(f"{base_url}{res.strip('/')}") for res in sub_resources}
                responses = await asyncio.gather(*requests.values(), return_exceptions=True)

                for res in sub_resources:
                    logger.info(f"=====> trying to read: {base_url}{res.strip('/')}")

                results: Dict[str, Any] = {}
                for i, key in enumerate(requests.keys()):
                    resp = responses[i]
                    if isinstance(resp, Exception):
                        error_msg = f"Error fetching {key} for {resource_type.rstrip('s')} {resource_id}: {resp}"
                        logger.error(error_msg)
                        ErrorLog().add_error(error_msg, exception=resp if isinstance(resp, Exception) else None)
                        return None
                    if resp.status != 200:
                        error_msg = f"Failed to fetch {key} for {resource_type.rstrip('s')} {resource_id}, status: {resp.status}"
                        logger.error(error_msg)
                        ErrorLog().add_error(error_msg)
                        return None
                    results[key] = await resp.json()
                return results
            except Exception as e:
                error_msg = f"Error processing details for {resource_type.rstrip('s')} {resource_id}: {e}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
                return None

        resources_url = f"{node.channelmapping_url}/{resource_type}"
        devices = []
        try:
            async with session.get(resources_url) as response:
                if response.status != 200:
                    error_msg = f"Failed to fetch {resource_type} list from {resources_url}: {response.status}"
                    logger.error(error_msg)
                    ErrorLog().add_error(error_msg)
                    return []
                resource_ids = await response.json()
                if not isinstance(resource_ids, list):
                    error_msg = f"Expected a list of {resource_type} IDs from {resources_url}, but got {type(resource_ids)}"
                    logger.error(error_msg)
                    ErrorLog().add_error(error_msg)
                    return []

            async def _details_and_construct(resource_id: str) -> Optional[Any]:
                details = await _fetch_details(resource_id)
                if details:
                    try:
                        return device_constructor(resource_id, details)
                    except Exception as e:
                        error_msg = f"Error constructing device for {resource_type.rstrip('s')} {resource_id}: {e}"
                        logger.error(error_msg)
                        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
                        traceback.print_exception(e)
                return None

            tasks = [_details_and_construct(rid) for rid in resource_ids]
            results = await asyncio.gather(*tasks)

            devices = [res for res in results if res is not None]
            return devices

        except Exception as e:
            error_msg = f"Error fetching IS-08 {resource_type} for node {node.node_id}: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return []

    async def fetch_device_is08_inputs(self, node: NMOS_Node, device: NMOS_Device, session: aiohttp.ClientSession) -> List[InputDevice]:
        """Populates the input device list by calling the IS-08 endpoints."""

        def constructor(input_id: str, details: Dict[str, Any]) -> InputDevice:
            logger.info(f"Input {input_id} channels data: {details.get('channels', [])}")
            input_channels = [
                InputChannel(id=ch.get("id", ""), label=ch.get("label", ""))
                for ch in details.get("channels", [])
            ]
            logger.info(f"retrieved input device {details.get('properties', {}).get('name', '')}: {input_channels}")
            return InputDevice(
                id=input_id,
                name=details.get("properties", {}).get("name", ""),
                description=details.get("properties", {}).get("description", ""),
                reordering=details.get("caps", {}).get("reordering", False),
                block_size=details.get("caps", {}).get("block_size", 0),
                parent_id=details.get("parent", {}).get("id", ""),
                parent_type=details.get("parent", {}).get("type", ""),
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
            output_channels = [
                OutputChannel(id=ch.get("id", ""), label=ch.get("label", ""), mapped_device=None, mapped_channel=None)
                for ch in details.get("channels", [])
            ]
            logger.info(f"retrieved output device {details.get("properties", {}).get("name", "")}: {output_channels}")
            return OutputDevice(
                id=output_id,
                name=details.get("properties", {}).get("name", ""),
                description=details.get("properties", {}).get("description", ""),
                source_id=details.get("sourceid", {}), # sourceid returns a plain string
                routable_inputs=details.get("caps", {}).get("routable_inputs", []),
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
                    logger.error(error_msg)
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
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


    async def fetch_device_channels(self, node: NMOS_Node, device: NMOS_Device):
        """Fetch channel information for a device using IS-08 Channel Mapping API"""
        try:
            logger.info(f"---------------------- Fetching channels for device {device.device_id} from node {node.node_id}")

            async with aiohttp.ClientSession() as session:
                inputs = await self.fetch_device_is08_inputs(node, device, session)
                outputs = await self.fetch_device_is08_outputs(node, device, session)
                await self.fetch_device_is08_mapping(node, device, session, inputs, outputs)

                device.is08_input_channels = inputs
                device.is08_output_channels = outputs

            logger.debug(f"---------------------- Found channels: {inputs}, {outputs}")

            # Notify UI about channel updates
            if self.channel_updated_callback:
                try:
                    asyncio.create_task(
                        self._call_callback_with_params(self.channel_updated_callback, node.node_id, device.device_id)
                    )
                except RuntimeError:
                    self.channel_updated_callback(node.node_id, device.device_id)

        except Exception as e:
            error_msg = f"Error fetching channels for device {device.device_id}: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def start(self):
        """Start the NMOS registry and begin discovering devices"""
        if self._running:
            logger.warning("Registry already running")
            return

        if Zeroconf is None:
            error_msg = "zeroconf library not available. Install with: pip install zeroconf"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg)
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
            on_node_added=self._on_node_added,
            on_node_removed=self._on_node_removed,
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
            error_msg = f"Failed to start registration service (non-fatal): {e}"
            logger.error(error_msg)
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
            # Use specified listen IP or auto-detect
            if self.listen_ip:
                local_ip = self.listen_ip
                logger.info(f"Using specified listen IP: {local_ip}")
            else:
                local_ip = socket.gethostbyname(hostname)
                logger.info(f"Auto-detected IP: {local_ip}")

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
            error_msg = f"Failed to register NMOS service: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
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
                error_msg = f"Failed to unregister service: {e}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

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
            logger.error(error_msg)
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
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

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
        error_msg = f"return error: {err} with status {status}"
        logger.error(error_msg)
        ErrorLog().add_error(error_msg)
        return json_response(err, status = status)

    def _handle_registration_node(self, request, resource_data: dict, api_version: str):
        # Register or re-register a node
        device_id = resource_data.get('id', 'unknown')
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
            port=client_port,  # Default HTTP port, may be in data
            service_type='_nmos-register._tcp.local.',
            api_ver=api_version,
            properties=resource_data,
            devices=devices
        )

        logger.info(f"Device registered via HTTP: {label} ({device_id}) from {client_host}:{client_port}")
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
        dev = NMOS_Device(node_id=node_id, device_id=device_id, senders=senders, receivers=receivers, is08_input_channels=[], is08_output_channels=[], label=label, description=description)
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
            return self.error_json_response({'status': f'sender-registration: bad device parent ID: {sender_device_id}'}, status=400)

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

                if self.debug_registry:
                    logger.info(f"linked sender node {parent.node_id} to receiver {receiver.device_id}")
                parent.senders.append(receiver)
            else:
                if self.debug_registry:
                    logger.info("no subscriptions for sender yet")
        else:
            if self.debug_registry:
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
                        receivers=[],
                        is08_input_channels=[],
                        is08_output_channels=[]
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

                if self.debug_registry:
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
                    logger.error(error_msg)
                    ErrorLog().add_error(error_msg)
                    return self._handle_registration_unknown(request, resource_data)

        except Exception as e:
            error_msg = f"Error handling registration: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
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
            error_msg = f"Error handling deregistration: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
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
                error_msg = f"Error announcing service: {e}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
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
                error_msg = f"Error in heartbeat monitoring: {e}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
                await asyncio.sleep(5)

    def _on_node_added(self, node: NMOS_Node):
        """Internal callback when a device is added"""
        logger.info(
            f"Device discovered: {node.name} at {node.address}:{node.port}"
        )
        if node.node_id not in self.nodes:
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
