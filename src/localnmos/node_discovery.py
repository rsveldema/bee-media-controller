"""
NMOS Node Discovery and Auto-Registration

This module handles automatic discovery and registration of NMOS nodes
when they send health updates before formal registration.
"""

import traceback
from typing import Callable, Dict
from aiohttp import web
import aiohttp

from .nmos import NMOS_Node, NMOS_Device, NMOS_Sender, NMOS_Receiver, NMOS_Source, InputDevice, InputChannel, OutputDevice, OutputChannel
from .error_log import ErrorLog
from .logging_utils import create_logger

logger = create_logger(__name__)


NMOS_STANDARD_PORT = 1936


async def fetch_and_add_sources(node: NMOS_Node, session: aiohttp.ClientSession, node_id: str) -> None:
    """
    Fetch sources from the IS-04 Node API and add them to the corresponding devices.
    
    Args:
        node: The NMOS node to fetch sources for
        session: The aiohttp ClientSession to use for requests
        node_id: The ID of the node
    """
    try:
        sources_url = f"{node.node_url}/sources"
        logger.info(f"Fetching sources from {sources_url}")
        
        async with session.get(sources_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status == 200:
                sources_data = await response.json()
                logger.info(f"Retrieved {len(sources_data)} sources from node {node_id}")
                
                # Process each source and add to the corresponding device
                for source_data in sources_data:
                    source_id = source_data.get('id', '')
                    label = source_data.get('label', '')
                    description = source_data.get('description', '')
                    format_type = source_data.get('format', '')
                    device_id = source_data.get('device_id', '')
                    parents = source_data.get('parents', [])
                    
                    # Create NMOS_Source object
                    nmos_source = NMOS_Source(
                        source_id=source_id,
                        label=label,
                        description=description,
                        format=format_type,
                        device_id=device_id,
                        parents=parents
                    )
                    
                    # Find the device this source belongs to and add it
                    for device in node.devices:
                        if device.device_id == device_id:
                            device.sources.append(nmos_source)
                            logger.info(f"Added source {source_id} ({label}) to device {device_id}")
                            break
            else:
                logger.warning(f"Failed to fetch sources from {sources_url}: HTTP {response.status}")
    except Exception as e:
        error_msg = f"Error fetching sources for {node_id}: {e}"
        logger.error(error_msg)
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def fetch_and_add_receivers(node: NMOS_Node, session: aiohttp.ClientSession, node_id: str) -> None:
    """
    Fetch receivers from the IS-04 Node API and add them to the corresponding devices.
    
    Args:
        node: The NMOS node to fetch receivers for
        session: The aiohttp ClientSession to use for requests
        node_id: The ID of the node
    """
    try:
        receivers_url = f"{node.node_url}/receivers"
        logger.info(f"Fetching receivers from {receivers_url}")
        
        async with session.get(receivers_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status == 200:
                receivers_data = await response.json()
                logger.info(f"Retrieved {len(receivers_data)} receivers from node {node_id}")
                
                # Process each receiver and add to the corresponding device
                for receiver_data in receivers_data:
                    receiver_id = receiver_data.get('id', '')
                    label = receiver_data.get('label', '')
                    description = receiver_data.get('description', '')
                    format_type = receiver_data.get('format', '')
                    transport = receiver_data.get('transport', '')
                    device_id = receiver_data.get('device_id', '')
                    
                    # Create NMOS_Receiver object
                    nmos_receiver = NMOS_Receiver(
                        receiver_id=receiver_id,
                        label=label,
                        description=description,
                        format=format_type,
                        transport=transport,
                        device_id=device_id
                    )
                    
                    # Find the device this receiver belongs to and add it
                    for device in node.devices:
                        if device.device_id == device_id:
                            device.receivers.append(nmos_receiver)
                            logger.info(f"Added receiver {receiver_id} ({label}) to device {device_id}")
                            break
            else:
                logger.warning(f"Failed to fetch receivers from {receivers_url}: HTTP {response.status}")
    except Exception as e:
        error_msg = f"Error fetching receivers for {node_id}: {e}"
        logger.error(error_msg)
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def fetch_and_add_senders(node: NMOS_Node, session: aiohttp.ClientSession, node_id: str) -> None:
    """
    Fetch senders from the IS-04 Node API and add them to the corresponding devices.
    
    Args:
        node: The NMOS node to fetch senders for
        session: The aiohttp ClientSession to use for requests
        node_id: The ID of the node
    """
    try:
        senders_url = f"{node.node_url}/senders"
        logger.info(f"Fetching senders from {senders_url}")
        
        async with session.get(senders_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status == 200:
                senders_data = await response.json()
                logger.info(f"Retrieved {len(senders_data)} senders from node {node_id}")
                
                # Process each sender and add to the corresponding device
                for sender_data in senders_data:
                    sender_id = sender_data.get('id', '')
                    label = sender_data.get('label', '')
                    description = sender_data.get('description', '')
                    flow_id = sender_data.get('flow_id', '')
                    transport = sender_data.get('transport', '')
                    device_id = sender_data.get('device_id', '')
                    manifest_href = sender_data.get('manifest_href', '')
                    
                    # Create NMOS_Sender object
                    nmos_sender = NMOS_Sender(
                        sender_id=sender_id,
                        label=label,
                        description=description,
                        flow_id=flow_id,
                        transport=transport,
                        device_id=device_id,
                        manifest_href=manifest_href
                    )
                    
                    # Find the device this sender belongs to and add it
                    for device in node.devices:
                        if device.device_id == device_id:
                            device.senders.append(nmos_sender)
                            logger.info(f"Added sender {sender_id} ({label}) to device {device_id}")
                            break
            else:
                logger.warning(f"Failed to fetch senders from {senders_url}: HTTP {response.status}")
    except Exception as e:
        error_msg = f"Error fetching senders for {node_id}: {e}"
        logger.error(error_msg)
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def fetch_and_add_is08_inputs(node: NMOS_Node, session: aiohttp.ClientSession, node_id: str) -> None:
    """
    Fetch IS-08 Channel Mapping inputs for all devices.
    
    Args:
        node: The NMOS node to fetch IS-08 inputs for
        session: The aiohttp ClientSession to use for requests
        node_id: The ID of the node
    """
    # Fetch inputs for each device
    for device in node.devices:
        logger.info(f"Fetching IS-08 inputs for device {device.device_id}")
        try:
            inputs_url = f"{node.channelmapping_url}/inputs"
            async with session.get(inputs_url, timeout=aiohttp.ClientTimeout(total=5)) as inputs_resp:
                if inputs_resp.status == 200:
                    input_ids = await inputs_resp.json()
                    logger.info(f"Found {len(input_ids)} inputs")
                    
                    # Fetch details for each input
                    for input_id in input_ids:
                        input_id_str = input_id.rstrip('/')
                        properties_url = f"{node.channelmapping_url}/inputs/{input_id_str}/properties"
                        channels_url = f"{node.channelmapping_url}/inputs/{input_id_str}/channels"
                        
                        try:
                            async with session.get(properties_url, timeout=aiohttp.ClientTimeout(total=3)) as prop_resp:
                                properties = await prop_resp.json() if prop_resp.status == 200 else {}
                            
                            async with session.get(channels_url, timeout=aiohttp.ClientTimeout(total=3)) as chan_resp:
                                channels_data = await chan_resp.json() if chan_resp.status == 200 else []
                            
                            input_channels = [
                                InputChannel(id=ch.get("id", ""), label=ch.get("label", ""))
                                for ch in channels_data
                            ]

                            logger.info(f"Output {input_id_str} has {len(input_channels)} channels")
                            
                            input_device = InputDevice(
                                id=input_id_str,
                                name=properties.get("name", ""),
                                description=properties.get("description", ""),
                                reordering=False,
                                block_size=0,
                                parent_id="",
                                parent_type="",
                                channels=input_channels
                            )
                            device.is08_input_channels.append(input_device)
                            logger.info(f"Added input device {input_id_str} with {len(input_channels)} channels")
                        except Exception as e:
                            logger.warning(f"Error fetching input {input_id_str}: {e}")
        except Exception as e:
            logger.warning(f"Error fetching inputs for device {device.device_id}: {e}")


async def fetch_and_add_is08_outputs(node: NMOS_Node, session: aiohttp.ClientSession, node_id: str) -> None:
    """
    Fetch IS-08 Channel Mapping outputs for all devices.
    
    Args:
        node: The NMOS node to fetch IS-08 outputs for
        session: The aiohttp ClientSession to use for requests
        node_id: The ID of the node
    """
    # Fetch outputs for each device
    for device in node.devices:
        logger.info(f"Fetching IS-08 outputs for device {device.device_id}")
        try:
            outputs_url = f"{node.channelmapping_url}/outputs"
            async with session.get(outputs_url, timeout=aiohttp.ClientTimeout(total=5)) as outputs_resp:
                if outputs_resp.status == 200:
                    output_ids = await outputs_resp.json()
                    logger.info(f"Found {len(output_ids)} outputs")
                    
                    # Fetch details for each output
                    for output_id in output_ids:
                        output_id_str = output_id.rstrip('/')
                        properties_url = f"{node.channelmapping_url}/outputs/{output_id_str}/properties"
                        channels_url = f"{node.channelmapping_url}/outputs/{output_id_str}/channels"
                        
                        try:
                            async with session.get(properties_url, timeout=aiohttp.ClientTimeout(total=3)) as prop_resp:
                                properties = await prop_resp.json() if prop_resp.status == 200 else {}
                            
                            async with session.get(channels_url, timeout=aiohttp.ClientTimeout(total=3)) as chan_resp:
                                channels_data = await chan_resp.json() if chan_resp.status == 200 else []
                            
                            output_channels = [
                                OutputChannel(id=ch.get("id", ""), label=ch.get("label", ""), mapped_device=None, mapped_channel=None)
                                for ch in channels_data
                            ]

                            logger.info(f"Output {output_id_str} has {len(output_channels)} channels")
                            
                            output_device = OutputDevice(
                                id=output_id_str,
                                name=properties.get("name", ""),
                                description=properties.get("description", ""),
                                source_id="",
                                routable_inputs=[],
                                channels=output_channels
                            )
                            device.is08_output_channels.append(output_device)
                            logger.info(f"Added output device {output_id_str} with {len(output_channels)} channels")
                        except Exception as e:
                            logger.warning(f"Error fetching output {output_id_str}: {e}")
        except Exception as e:
            logger.warning(f"Error fetching outputs for device {device.device_id}: {e}")


async def fetch_and_add_is08_channels(node: NMOS_Node, session: aiohttp.ClientSession, node_id: str) -> None:
    """
    Check if IS-08 Channel Mapping is supported and fetch inputs/outputs for all devices.
    
    Args:
        node: The NMOS node to fetch IS-08 channels for
        session: The aiohttp ClientSession to use for requests
        node_id: The ID of the node
    """
    logger.info(f"Checking IS-08 Channel Mapping support for {node_id}")
    try:
        # Test if IS-08 is available by checking the inputs endpoint
        is08_check_url = f"{node.channelmapping_url}/inputs"
        logger.info(f"Testing IS-08 support at {is08_check_url}")
        
        async with session.get(is08_check_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status == 200:
                logger.info(f"IS-08 Channel Mapping is supported for node {node_id}")
                
                # Fetch inputs for each device
                await fetch_and_add_is08_inputs(node, session, node_id)
                
                # Fetch outputs for each device
                await fetch_and_add_is08_outputs(node, session, node_id)
            else:
                logger.info(f"IS-08 Channel Mapping not supported for node {node_id} (status: {response.status})")
    except Exception as e:
        logger.info(f"IS-08 Channel Mapping not available for node {node_id}: {e}")


async def fetch_and_add_devices(node: NMOS_Node, session: aiohttp.ClientSession, node_id: str) -> None:
    """
    Fetch devices from the IS-04 Node API and add them to the node.
    
    Args:
        node: The NMOS node to fetch devices for
        session: The aiohttp ClientSession to use for requests
        node_id: The ID of the node
    """
    logger.info(f"About to fetch devices for {node_id}")
    try:
        devices_url = f"{node.node_url}/devices"
        logger.info(f"Fetching devices from {devices_url}")
        
        async with session.get(devices_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            logger.info(f"Devices response status: {response.status}")
            if response.status == 200:
                devices_data = await response.json()
                logger.info(f"Retrieved {len(devices_data)} devices from node {node_id}")
                
                # Create NMOS_Device objects for each device
                for device_data in devices_data:
                    device_id = device_data.get('id', '')
                    label = device_data.get('label', '')
                    description = device_data.get('description', '')
                    
                    device = NMOS_Device(
                        node_id=node_id,
                        device_id=device_id,
                        senders=[],
                        receivers=[],
                        sources=[],
                        is08_input_channels=[],
                        is08_output_channels=[],
                        label=label,
                        description=description
                    )
                    node.devices.append(device)
                    logger.info(f"Added device {device_id} ({label}) to node {node_id}")
            else:
                logger.warning(f"Failed to fetch devices from {devices_url}: HTTP {response.status}")
    except Exception as e:
        error_msg = f"Error fetching devices for {node_id}: {e}"
        logger.error(error_msg)
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def register_node_from_health_update(
    request: web.Request,
    node_id: str,
    nodes: Dict[str, NMOS_Node],
    on_node_added_callback: Callable
) -> None:
    """
    Register all information about a newly discovered node from a health update.
    
    This function is called when a node sends a health update but hasn't been
    formally registered yet. It fetches the node's devices, sources, senders,
    and receivers using the IS-04 Node API.
    
    Args:
        request: The HTTP request containing the health update
        node_id: The ID of the node to register
        nodes: Dictionary of registered nodes
        on_node_added_callback: Callback to invoke when node is added
    """
    logger.info(f"Node {node_id} not registered yet, creating registration from health update")
    client_host = request.remote
    
    # Create a basic node registration
    node = NMOS_Node(
        name=f"Node {node_id}",
        node_id=node_id,
        address=client_host,
        port=NMOS_STANDARD_PORT,  # Default port
        service_type='_nmos-register._tcp.local.',
        api_ver='v1.3',
        properties={'id': node_id},
        devices=[]
    )
    
    # Add to nodes dictionary immediately so callbacks can reference it
    nodes[node_id] = node
    
    # Fetch node information from IS-04 Node API
    logger.info(f"Starting node information fetch for {node_id}")
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Fetch devices from the node API
        await fetch_and_add_devices(node, session, node_id)
        
        logger.info(f"Finished devices fetch, moving to senders for {node_id}")
        
        # Fetch senders from the node API
        await fetch_and_add_senders(node, session, node_id)
        
        # Fetch receivers from the node API
        await fetch_and_add_receivers(node, session, node_id)
        
        # Fetch sources from the node API
        await fetch_and_add_sources(node, session, node_id)
        
        # Check if IS-08 Channel Mapping is supported and fetch inputs/outputs
        await fetch_and_add_is08_channels(node, session, node_id)
    
    # Trigger callback
    if on_node_added_callback:
        if callable(on_node_added_callback):
            on_node_added_callback(node)
    
    logger.info(f"Auto-registered node {node_id} from health update at {client_host}")
