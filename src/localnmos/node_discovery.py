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


async def find_working_nmos_port(client_host: str, trial_ports: list) -> int:
    """
    Try to find a working NMOS port by testing connectivity to the node API.
    
    Args:
        client_host: The host address to test
        trial_ports: List of ports to try in order
        
    Returns:
        The first working port, or None if none work
    """
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for port in trial_ports:
            if port is None:
                continue
            
            try:
                # Try to connect to the node API self endpoint
                test_url = f"http://{client_host}:{port}/x-nmos/node/v1.3/self"
                logger.debug(f"Testing NMOS API at {test_url}")
                
                async with session.get(test_url, timeout=aiohttp.ClientTimeout(total=2)) as response:
                    if response.status == 200:
                        logger.info(f"Found working NMOS API on port {port}")
                        return port
            except Exception as e:
                logger.debug(f"Port {port} failed: {e}")
                continue
    
    logger.warning(f"Could not find working NMOS port for {client_host} (tried {trial_ports})")
    return None


def assign_temporary_channels_to_devices(node: NMOS_Node) -> None:
    """
    Assign temporarily stored channels to devices based on source matching.
    
    For inputs: match parent_id to device ID or source ID
    For outputs: match source_id to device ID or source ID
    
    Args:
        node: The NMOS node with temporary channels to assign
    """
    # Build mappings for both device IDs and source IDs
    device_id_to_device = {}
    source_id_to_device = {}
    
    for device in node.devices:
        device_id_to_device[device.device_id] = device
        for source in device.sources:
            source_id_to_device[source.source_id] = device
    
    # Assign input channels based on parent_id matching
    for input_device in node.temp_channel_inputs:
        matched_device = None
        match_type = None
        
        # Try matching parent_id as device ID first
        if input_device.parent_id in device_id_to_device:
            matched_device = device_id_to_device[input_device.parent_id]
            match_type = "device_id"
        # Then try matching as source ID
        elif input_device.parent_id in source_id_to_device:
            matched_device = source_id_to_device[input_device.parent_id]
            match_type = "source_id"
        
        if matched_device:
            # Check if this input already exists in the device (avoid duplicates)
            existing_ids = {inp.id for inp in matched_device.is08_input_channels}
            if input_device.id not in existing_ids:
                matched_device.is08_input_channels.append(input_device)
                logger.info(f"Assigned input {input_device.id} to device {matched_device.device_id} (matched parent_id={input_device.parent_id} as {match_type})")
            else:
                logger.debug(f"Skipped duplicate input {input_device.id} for device {matched_device.device_id}")
        else:
            # Fallback strategies for unmatched devices (e.g., RTP devices with empty parent_id)
            fallback_device = None
            fallback_reason = None
            
            # Strategy 1: For RTP devices, try to find a device with "rtp" in its label
            if 'rtp' in input_device.id.lower() or 'rtp' in input_device.name.lower():
                for device in node.devices:
                    if 'rtp' in device.label.lower():
                        fallback_device = device
                        fallback_reason = "RTP name matching"
                        break
            
            # Strategy 2: If still no match, assign to all devices (cross-device mapping supported)
            if not fallback_device and len(node.devices) > 0:
                # Assign to all devices since IS-08 supports cross-device mappings
                for device in node.devices:
                    existing_ids = {inp.id for inp in device.is08_input_channels}
                    if input_device.id not in existing_ids:
                        device.is08_input_channels.append(input_device)
                logger.info(f"Assigned input {input_device.id} to all {len(node.devices)} devices (fallback: empty parent_id)")
                continue
            
            if fallback_device:
                existing_ids = {inp.id for inp in fallback_device.is08_input_channels}
                if input_device.id not in existing_ids:
                    fallback_device.is08_input_channels.append(input_device)
                    logger.info(f"Assigned input {input_device.id} to device {fallback_device.device_id} (fallback: {fallback_reason})")
            else:
                logger.warning(f"Could not assign input {input_device.id} - parent_id '{input_device.parent_id}' (type={input_device.parent_type}) not found. Available device IDs: {list(device_id_to_device.keys())}, source IDs: {list(source_id_to_device.keys())}")
            logger.warning(f"Could not assign input {input_device.id} - parent_id '{input_device.parent_id}' (type={input_device.parent_type}) not found. Available device IDs: {list(device_id_to_device.keys())}, source IDs: {list(source_id_to_device.keys())}")
    
    # Assign output channels based on source_id matching  
    for output_device in node.temp_channel_outputs:
        matched_device = None
        match_type = None
        
        # Try matching source_id as device ID first
        if output_device.source_id in device_id_to_device:
            matched_device = device_id_to_device[output_device.source_id]
            match_type = "device_id"
        # Then try matching as source ID
        elif output_device.source_id in source_id_to_device:
            matched_device = source_id_to_device[output_device.source_id]
            match_type = "source_id"
        
        if matched_device:
            # Check if this output already exists in the device (avoid duplicates)
            existing_ids = {out.id for out in matched_device.is08_output_channels}
            if output_device.id not in existing_ids:
                matched_device.is08_output_channels.append(output_device)
                logger.info(f"Assigned output {output_device.id} to device {matched_device.device_id} (matched source_id={output_device.source_id} as {match_type})")
            else:
                logger.debug(f"Skipped duplicate output {output_device.id} for device {matched_device.device_id}")
        else:
            # Fallback strategies for unmatched devices (e.g., RTP devices with empty source_id)
            fallback_device = None
            fallback_reason = None
            
            # Strategy 1: For RTP devices, try to find a device with "rtp" in its label
            if 'rtp' in output_device.id.lower() or 'rtp' in output_device.name.lower():
                for device in node.devices:
                    if 'rtp' in device.label.lower():
                        fallback_device = device
                        fallback_reason = "RTP name matching"
                        break
            
            # Strategy 2: If still no match, assign to all devices (cross-device mapping supported)
            if not fallback_device and len(node.devices) > 0:
                # Assign to all devices since IS-08 supports cross-device mappings
                for device in node.devices:
                    existing_ids = {out.id for out in device.is08_output_channels}
                    if output_device.id not in existing_ids:
                        device.is08_output_channels.append(output_device)
                logger.info(f"Assigned output {output_device.id} to all {len(node.devices)} devices (fallback: empty source_id)")
                continue
            
            if fallback_device:
                existing_ids = {out.id for out in fallback_device.is08_output_channels}
                if output_device.id not in existing_ids:
                    fallback_device.is08_output_channels.append(output_device)
                    logger.info(f"Assigned output {output_device.id} to device {fallback_device.device_id} (fallback: {fallback_reason})")
            else:
                logger.warning(f"Could not assign output {output_device.id} - source_id '{output_device.source_id}' not found. Available device IDs: {list(device_id_to_device.keys())}, source IDs: {list(source_id_to_device.keys())}")
    
    # Note: We do NOT clear temp storage here because the caller may still need these lists
    # (e.g., fetch_device_is08_mapping receives all_inputs/all_outputs which reference temp storage)
    logger.info(f"Completed channel assignment for node {node.node_id}")


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
                
                # After adding all sources, assign temporary channels to devices
                assign_temporary_channels_to_devices(node)
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
                    found = False
                    for device in node.devices:
                        if device.device_id == device_id:
                            device.senders.append(nmos_sender)
                            logger.info(f"Added sender {sender_id} ({label}) to device {device_id}")
                            found = True
                            break
                    assert found, f"Device {device_id} not found for sender {sender_id}"
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
    # Fetch inputs once at the node level (not per device)
    logger.info(f"Fetching IS-08 inputs for node {node_id}")
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
                    parent_url = f"{node.channelmapping_url}/inputs/{input_id_str}/parent"
                    
                    try:
                        async with session.get(properties_url, timeout=aiohttp.ClientTimeout(total=3)) as prop_resp:
                            properties = await prop_resp.json() if prop_resp.status == 200 else {}
                        
                        async with session.get(channels_url, timeout=aiohttp.ClientTimeout(total=3)) as chan_resp:
                            channels_data = await chan_resp.json() if chan_resp.status == 200 else []
                        
                        async with session.get(parent_url, timeout=aiohttp.ClientTimeout(total=3)) as parent_resp:
                            parent_data = await parent_resp.json() if parent_resp.status == 200 else {}
                        
                        input_channels = [
                            InputChannel(id=ch.get("id", ""), label=ch.get("label", ""))
                            for ch in channels_data
                        ]

                        logger.info(f"Input {input_id_str} has {len(input_channels)} channels")
                        
                        input_device = InputDevice(
                            id=input_id_str,
                            name=properties.get("name", ""),
                            description=properties.get("description", ""),
                            reordering=False,
                            block_size=0,
                            parent_id=parent_data.get("id", ""),
                            parent_type=parent_data.get("type", ""),
                            channels=input_channels
                        )
                        
                        # Store temporarily in node until sources are available for matching
                        node.temp_channel_inputs.append(input_device)
                        logger.info(f"Stored input device {input_id_str} with {len(input_channels)} channels (parent_id={input_device.parent_id}) temporarily in node")
                    except Exception as e:
                        logger.warning(f"Error fetching input {input_id_str}: {e}")
    except Exception as e:
        logger.warning(f"Error fetching inputs for node {node_id}: {e}")


async def fetch_and_add_is08_outputs(node: NMOS_Node, session: aiohttp.ClientSession, node_id: str) -> None:
    """
    Fetch IS-08 Channel Mapping outputs for all devices.
    
    Args:
        node: The NMOS node to fetch IS-08 outputs for
        session: The aiohttp ClientSession to use for requests
        node_id: The ID of the node
    """
    # Fetch outputs once at the node level (not per device)
    logger.info(f"Fetching IS-08 outputs for node {node_id}")
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
                    sourceid_url = f"{node.channelmapping_url}/outputs/{output_id_str}/sourceid"
                    
                    try:
                        async with session.get(properties_url, timeout=aiohttp.ClientTimeout(total=3)) as prop_resp:
                            properties = await prop_resp.json() if prop_resp.status == 200 else {}
                        
                        async with session.get(channels_url, timeout=aiohttp.ClientTimeout(total=3)) as chan_resp:
                            channels_data = await chan_resp.json() if chan_resp.status == 200 else []
                        
                        async with session.get(sourceid_url, timeout=aiohttp.ClientTimeout(total=3)) as source_resp:
                            source_id = await source_resp.text() if source_resp.status == 200 else ""
                            # Remove quotes if present
                            source_id = source_id.strip('"')
                        
                        output_channels = [
                            OutputChannel(id=ch.get("id", ""), label=ch.get("label", ""), mapped_device=None, mapped_channel=None)
                            for ch in channels_data
                        ]

                        logger.info(f"Output {output_id_str} has {len(output_channels)} channels")
                        
                        output_device = OutputDevice(
                            id=output_id_str,
                            name=properties.get("name", ""),
                            description=properties.get("description", ""),
                            source_id=source_id,
                            routable_inputs=[],
                            channels=output_channels
                        )
                        
                        # Store temporarily in node until sources are available for matching
                        node.temp_channel_outputs.append(output_device)
                        logger.info(f"Stored output device {output_id_str} with {len(output_channels)} channels (source_id={source_id}) temporarily in node")
                    except Exception as e:
                        logger.warning(f"Error fetching output {output_id_str}: {e}")
    except Exception as e:
        logger.warning(f"Error fetching outputs for node {node_id}: {e}")


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
    
    # Check if this is an infrastructure node (query or system API) - skip these
    if node_id in nodes:
        existing_node = nodes[node_id]
        if existing_node.service_type in ('_nmos-query._tcp.local.', '_nmos-system._tcp.local.'):
            logger.info(f"Skipping infrastructure node {node_id} (service_type={existing_node.service_type})")
            return
    
    client_host = request.remote
    # try request.url.port first, then fall back to standard NMOS port, and if that fails, 
    trial_ports = [NMOS_STANDARD_PORT, 8080, request.url.port, 443, 8443]
    port = await find_working_nmos_port(client_host, trial_ports)
    if port is None:
        logger.error(f"Could not determine working NMOS port for node {node_id} at {client_host}, skipping registration")
        return
    
    logger.info(f"Using port {port} for NMOS API of node {node_id} at {client_host}")
    # Create a basic node registration
    node = NMOS_Node(
        name=f"Node {node_id}",
        node_id=node_id,
        address=client_host,
        port=port,  # Use the port from the request or default
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
