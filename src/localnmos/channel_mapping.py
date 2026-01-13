"""
Channel Mapping Module

This module provides IS-08 channel mapping functionality for connecting
and disconnecting audio channels between NMOS devices.
"""

import traceback
import aiohttp

from .nmos import (
    InputChannel,
    InputDevice,
    NMOS_Device,
    NMOS_Node,
    OutputChannel,
    OutputDevice,
)
from .error_log import ErrorLog
from .logging_utils import create_logger

logger = create_logger(__name__)


async def connect_channel_mapping(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    output_dev: OutputDevice,
    output_chan: OutputChannel,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    input_dev: InputDevice,
    input_chan: InputChannel,
    fetch_device_channels_callback=None,
):
    """Connect an output channel to an input channel using IS-08 API
    
    Args:
        sender_node: The NMOS node containing the output device
        sender_device: The NMOS device with the output
        output_dev: The output device
        output_chan: The output channel to map from
        receiver_node: The NMOS node containing the input device
        receiver_device: The NMOS device with the input
        input_dev: The input device
        input_chan: The input channel to map to
        fetch_device_channels_callback: Optional callback to refresh channel data after mapping
    """


    if sender_node == receiver_node:
        # Same node, use local mapping via IS-08
        await is_08_connect_channel_mapping(
            sender_node,
            sender_device,
            output_dev,
            output_chan,
            receiver_node,
            receiver_device,
            input_dev,
            input_chan,
            fetch_device_channels_callback,
        )
        return
    
    if output_chan == None and input_chan == None:
        # No specific channels, use pure IS-05 connection without IS-08 mapping
        await is_05_connect_devices(
            sender_node,
            sender_device,
            receiver_node,
            receiver_device,
        )
        return
    
    # different nodes, first connect the sender and receiver and then find the RTP channel 
    if output_chan != None and input_chan == None:
        # Map specific output channel to RTP, receiver gets all channels
        await is_05_and_is_08_sender_channel_mapping(
            sender_node,
            sender_device,
            output_dev,
            output_chan,
            receiver_node,
            receiver_device,
            fetch_device_channels_callback,
        )
        return
    
    if output_chan == None and input_chan != None:
        # Sender sends all channels, map RTP to specific input channel on receiver
        await is_05_and_is_08_receiver_channel_mapping(
            sender_node,
            sender_device,
            receiver_node,
            receiver_device,
            input_dev,
            input_chan,
            fetch_device_channels_callback,
        )
        return

    if output_chan != None and input_chan != None:
        # Cross-node connection: use IS-05 to connect sender/receiver, then IS-08 for channel mapping
        await is_05_and_is_08_connect_channel_mapping(
            sender_node,
            sender_device,
            output_dev,
            output_chan,
            receiver_node,
            receiver_device,
            input_dev,
            input_chan,
            fetch_device_channels_callback,
        )
        return
    
    assert False, "Unreachable code in connect_channel_mapping"


async def is_05_connect_devices(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
):
    """
    Connect sender device to receiver device using IS-05 Connection Management API only.
    No channel-level mapping is performed (IS-08 not used).
    
    This establishes a direct RTP stream connection between the devices.
    
    Args:
        sender_node: The NMOS node containing the sender
        sender_device: The sender device to connect
        receiver_node: The NMOS node containing the receiver
        receiver_device: The receiver device to connect to
    """
    try:
        # Build IS-05 connection API URLs
        sender_connection_url = f"{sender_node.connection_url}/single/senders/{sender_device.device_id}"
        receiver_connection_url = f"{receiver_node.connection_url}/single/receivers/{receiver_device.device_id}"
        
        logger.info(f"Connecting sender {sender_device.device_id} to receiver {receiver_device.device_id} using IS-05")
        
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Get the sender's active transport parameters
            async with session.get(f"{sender_connection_url}/active") as response:
                if response.status != 200:
                    error_msg = f"Failed to get sender active parameters: {response.status}"
                    ErrorLog().add_error(error_msg)
                    return
                sender_active = await response.json()
            
            # Stage the receiver to point to the sender
            # Get sender's multicast address or prepare for unicast
            sender_transport = sender_active.get("transport_params", [{}])[0]
            
            receiver_patch_data = {
                "master_enable": True,
                "activation": {"mode": "activate_immediate"},
                "transport_params": [{
                    "source_ip": sender_transport.get("source_ip"),
                    "multicast_ip": sender_transport.get("multicast_ip"),
                    "destination_port": sender_transport.get("destination_port", 5004),
                    "rtp_enabled": True,
                }]
            }
            
            # Remove None values
            receiver_patch_data["transport_params"][0] = {
                k: v for k, v in receiver_patch_data["transport_params"][0].items() if v is not None
            }
            
            async with session.patch(f"{receiver_connection_url}/staged", json=receiver_patch_data) as response:
                if response.status not in [200, 202]:
                    error_text = await response.text()
                    error_msg = f"Failed to stage receiver connection: {response.status} - {error_text}"
                    ErrorLog().add_error(error_msg)
                    return
            
            # Activate the sender if not already active
            sender_patch_data = {
                "master_enable": True,
                "activation": {"mode": "activate_immediate"},
            }
            
            async with session.patch(f"{sender_connection_url}/staged", json=sender_patch_data) as response:
                if response.status not in [200, 202]:
                    error_text = await response.text()
                    error_msg = f"Failed to activate sender: {response.status} - {error_text}"
                    ErrorLog().add_error(error_msg)
                    return
            
            logger.info(f"Successfully connected sender {sender_device.device_id} to receiver {receiver_device.device_id}")
            
    except Exception as e:
        error_msg = f"Error in IS-05 device connection: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def is_05_and_is_08_sender_channel_mapping(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    output_dev: OutputDevice,
    output_chan: OutputChannel,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    fetch_device_channels_callback=None,
):
    """
    Connect sender to receiver with IS-05, then map a specific output channel
    to RTP on sender using IS-08. Receiver gets all channels without specific mapping.
    
    Steps:
    1. Connect sender to receiver using IS-05 (establishes RTP stream)
    2. Find RTP device on sender node
    3. Map output_chan to RTP channel on sender using IS-08
    
    Args:
        sender_node: The NMOS node containing the sender
        sender_device: The sender device
        output_dev: The output device containing the channel
        output_chan: The specific output channel to map to RTP
        receiver_node: The NMOS node containing the receiver
        receiver_device: The receiver device
        fetch_device_channels_callback: Optional callback to refresh channel data
    """
    try:
        # Step 1: Connect sender to receiver using IS-05
        await is_05_connect_devices(
            sender_node,
            sender_device,
            receiver_node,
            receiver_device,
        )
        
        # Step 2: Find RTP output device on sender node
        rtp_output_dev_sender = await find_rtp_device(sender_node, sender_device, is_input=False)
        if not rtp_output_dev_sender:
            error_msg = f"Could not find RTP output device on sender node {sender_node.node_id}"
            ErrorLog().add_error(error_msg)
            return
        
        # Step 3: Map output_chan to first available RTP channel on sender
        if rtp_output_dev_sender.channels:
            rtp_chan_sender = rtp_output_dev_sender.channels[0]
            await is_08_connect_channel_mapping(
                sender_node,
                sender_device,
                rtp_output_dev_sender,
                rtp_chan_sender,
                sender_node,
                sender_device,
                output_dev,
                output_chan,
                fetch_device_channels_callback,
            )
            logger.info(f"Mapped output channel {output_chan.label} to RTP on sender, receiver gets all channels")
        else:
            error_msg = "No RTP channels available on sender"
            ErrorLog().add_error(error_msg)
            
    except Exception as e:
        error_msg = f"Error in IS-05/IS-08 sender channel mapping: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def is_05_and_is_08_receiver_channel_mapping(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    input_dev: InputDevice,
    input_chan: InputChannel,
    fetch_device_channels_callback=None,
):
    """
    Connect sender to receiver with IS-05, then map RTP to a specific input channel
    on receiver using IS-08. Sender sends all channels without specific mapping.
    
    Steps:
    1. Connect sender to receiver using IS-05 (establishes RTP stream)
    2. Find RTP device on receiver node
    3. Map RTP channel to input_chan on receiver using IS-08
    
    Args:
        sender_node: The NMOS node containing the sender
        sender_device: The sender device
        receiver_node: The NMOS node containing the receiver
        receiver_device: The receiver device
        input_dev: The input device containing the channel
        input_chan: The specific input channel to map RTP to
        fetch_device_channels_callback: Optional callback to refresh channel data
    """
    try:
        # Step 1: Connect sender to receiver using IS-05
        await is_05_connect_devices(
            sender_node,
            sender_device,
            receiver_node,
            receiver_device,
        )
        
        # Step 2: Find RTP input device on receiver node
        rtp_input_dev_receiver = await find_rtp_device(receiver_node, receiver_device, is_input=True)
        if not rtp_input_dev_receiver:
            error_msg = f"Could not find RTP input device on receiver node {receiver_node.node_id}"
            ErrorLog().add_error(error_msg)
            return
        
        # Step 3: Map first available RTP channel on receiver to input_chan
        if rtp_input_dev_receiver.channels:
            rtp_chan_receiver = rtp_input_dev_receiver.channels[0]
            
            # Find an output device to use as the source for the mapping
            # We need to map from RTP input to the specific input channel
            # In IS-08, we map outputs to inputs, so we need to find an output on the RTP device
            # or use a virtual output that represents the RTP stream
            
            # For RTP devices, typically there's a corresponding output device that represents
            # the decoded RTP stream that can be routed to inputs
            rtp_output_dev_receiver = await find_rtp_device(receiver_node, receiver_device, is_input=False)
            
            if rtp_output_dev_receiver and rtp_output_dev_receiver.channels:
                rtp_output_chan = rtp_output_dev_receiver.channels[0]
                await is_08_connect_channel_mapping(
                    receiver_node,
                    receiver_device,
                    rtp_output_dev_receiver,
                    rtp_output_chan,
                    receiver_node,
                    receiver_device,
                    input_dev,
                    input_chan,
                    fetch_device_channels_callback,
                )
                logger.info(f"Mapped RTP to input channel {input_chan.label} on receiver, sender sends all channels")
            else:
                error_msg = "No RTP output device/channels available on receiver for routing"
                ErrorLog().add_error(error_msg)
        else:
            error_msg = "No RTP channels available on receiver"
            ErrorLog().add_error(error_msg)
            
    except Exception as e:
        error_msg = f"Error in IS-05/IS-08 receiver channel mapping: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def is_05_and_is_08_connect_channel_mapping(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    output_dev: OutputDevice,
    output_chan: OutputChannel,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    input_dev: InputDevice,
    input_chan: InputChannel,
    fetch_device_channels_callback=None,
):
    """
    Connect channels across different nodes using IS-05 for RTP connection
    and IS-08 for channel mapping on each end.
    
    Steps:
    1. Connect sender to receiver using IS-05 (establishes RTP stream)
    2. Find RTP device on sender node
    3. Find RTP device on receiver node
    4. Map output_chan to RTP channel on sender using IS-08
    5. Map RTP channel to input_chan on receiver using IS-08
    """
    try:
        # Step 1: Connect sender to receiver using IS-05
        # Build connection API URLs
        sender_connection_url = f"{sender_node.connection_url}/single/senders/{sender_device.device_id}"
        receiver_connection_url = f"{receiver_node.connection_url}/single/receivers/{receiver_device.device_id}"
        
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Get receiver transport parameters to configure sender
            async with session.get(f"{receiver_connection_url}/transportfile") as response:
                if response.status != 200:
                    error_msg = f"Failed to get receiver transport file: {response.status}"
                    ErrorLog().add_error(error_msg)
                    return
                receiver_sdp = await response.text()
            
            # Activate sender with receiver's transport parameters
            sender_patch_data = {
                "master_enable": True,
                "activation": {"mode": "activate_immediate"},
                "transport_params": [{"rtp_enabled": True}]
            }
            
            async with session.patch(f"{sender_connection_url}/staged", json=sender_patch_data) as response:
                if response.status not in [200, 202]:
                    error_text = await response.text()
                    error_msg = f"Failed to activate sender: {response.status} - {error_text}"
                    ErrorLog().add_error(error_msg)
                    return
            
            # Activate receiver pointing to sender
            receiver_patch_data = {
                "master_enable": True,
                "activation": {"mode": "activate_immediate"},
                "transport_params": [{"rtp_enabled": True}]
            }
            
            async with session.patch(f"{receiver_connection_url}/staged", json=receiver_patch_data) as response:
                if response.status not in [200, 202]:
                    error_text = await response.text()
                    error_msg = f"Failed to activate receiver: {response.status} - {error_text}"
                    ErrorLog().add_error(error_msg)
                    return
            
            logger.info(f"IS-05 connection established between sender {sender_device.device_id} and receiver {receiver_device.device_id}")
        
        # Step 2-5: Find RTP devices and map channels using IS-08
        # Find RTP input device on sender node (for receiving RTP streams to map to outputs)
        rtp_output_dev_sender = await find_rtp_device(sender_node, sender_device, is_input=False)
        if not rtp_output_dev_sender:
            error_msg = f"Could not find RTP output device on sender node {sender_node.node_id}"
            ErrorLog().add_error(error_msg)
            return
        
        # Find RTP output device on receiver node (for sending to RTP streams from inputs)
        rtp_input_dev_receiver = await find_rtp_device(receiver_node, receiver_device, is_input=True)
        if not rtp_input_dev_receiver:
            error_msg = f"Could not find RTP input device on receiver node {receiver_node.node_id}"
            ErrorLog().add_error(error_msg)
            return
        
        # Map output_chan to first available RTP channel on sender (output to RTP)
        if rtp_output_dev_sender.channels:
            rtp_chan_sender = rtp_output_dev_sender.channels[0]
            await is_08_connect_channel_mapping(
                sender_node,
                sender_device,
                rtp_output_dev_sender,
                rtp_chan_sender,
                sender_node,
                sender_device,
                output_dev,
                output_chan,
                fetch_device_channels_callback,
            )
            logger.info(f"Mapped output channel {output_chan.label} to RTP on sender")
        
        # Map first available RTP channel on receiver to input_chan (RTP to input)
        if rtp_input_dev_receiver.channels:
            rtp_chan_receiver = rtp_input_dev_receiver.channels[0]
            await is_08_connect_channel_mapping(
                receiver_node,
                receiver_device,
                output_dev,
                rtp_chan_receiver,
                receiver_node,
                receiver_device,
                input_dev,
                input_chan,
                fetch_device_channels_callback,
            )
            logger.info(f"Mapped RTP on receiver to input channel {input_chan.label}")
        
        logger.info(f"Cross-node channel mapping complete: {output_chan.label} -> {input_chan.label}")
        
    except Exception as e:
        error_msg = f"Error in IS-05/IS-08 cross-node connection: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def find_rtp_device(node: NMOS_Node, device: NMOS_Device, is_input: bool):
    """
    Find an RTP device on the given node.
    
    Args:
        node: The NMOS node to search
        device: The NMOS device context
        is_input: True to find RTP input device, False for RTP output device
    
    Returns:
        The RTP InputDevice or OutputDevice, or None if not found
    """
    try:
        if is_input:
            devices = device.is08_input_channels if device.is08_input_channels else []
            for dev in devices:
                if 'rtp' in dev.name.lower() or dev.parent_type == 'rtp':
                    return dev
        else:
            devices = device.is08_output_channels if device.is08_output_channels else []
            for dev in devices:
                if 'rtp' in dev.name.lower() or 'network' in dev.name.lower():
                    return dev
        
        warning_msg = f"No RTP {'input' if is_input else 'output'} device found on node {node.node_id}"
        logger.warning(warning_msg)
        ErrorLog().add_error(warning_msg)
        return None
    except Exception as e:
        error_msg = f"Error finding RTP device: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
        return None


async def is_08_connect_channel_mapping(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    output_dev: OutputDevice,
    output_chan: OutputChannel,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    input_dev: InputDevice,
    input_chan: InputChannel,
    fetch_device_channels_callback=None,
):

    try:
        # Build the IS-08 channel mapping API URL for activations
        activations_url = f"{sender_node.channelmapping_url}/map/activations"

        # Find the input channel index within its device
        input_channel_index = None
        for idx, chan in enumerate(input_dev.channels):
            if (
                chan is input_chan
                or chan.id == input_chan.id
                or (not chan.id and chan.label == input_chan.label)
            ):
                input_channel_index = idx
                break

        if input_channel_index is None:
            error_msg = f"Could not find channel index for input {input_chan.label} (ID: {input_chan.id}) in device {input_dev.id}"
            ErrorLog().add_error(error_msg)
            return

        # Find the output channel key (ID or index as string)
        # According to IS-08 spec, the key is the channel ID if present, otherwise use the channel index
        output_chan_key = output_chan.id
        if not output_chan_key:
            # Find the output channel index
            for idx, chan in enumerate(output_dev.channels):
                if chan is output_chan or (
                    not chan.id and chan.label == output_chan.label
                ):
                    output_chan_key = str(idx)
                    break
            if not output_chan_key:
                error_msg = (
                    f"Could not determine output channel key for {output_chan.label}"
                )
                ErrorLog().add_error(error_msg)
                return

        logger.info(
            f"Mapping output {output_dev.id}/{output_chan_key} (label: {output_chan.label}) to input {input_dev.id} channel index {input_channel_index} (label: {input_chan.label})"
        )

        # Prepare the activation request according to IS-08 spec
        # Use immediate activation with the output-to-input channel mapping
        # Strip trailing slashes from device IDs
        output_dev_id_clean = output_dev.id.rstrip("/")
        input_dev_id_clean = input_dev.id.rstrip("/")

        activation_data = {
            "activation": {"mode": "activate_immediate"},
            "action:": {  # note the extra ':' here is intentional as per IS-08 spec
                output_dev_id_clean: {
                    output_chan_key: {
                        "input": input_dev_id_clean,
                        "channel_index": input_channel_index,
                    }
                }
            },
        }

        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # POST request to /map/activations for immediate activation
            async with session.post(activations_url, json=activation_data) as response:
                if response.status in [200, 202]:
                    logger.info(
                        f"Successfully mapped output channel {output_chan.label} to input channel {input_chan.label}"
                    )
                    # Refresh the channel data if callback provided
                    if fetch_device_channels_callback:
                        await fetch_device_channels_callback(sender_node, sender_device)
                else:
                    error_text = await response.text()
                    error_msg = f"Failed to set channel mapping: {response.status} - {error_text}: tried to post: {activation_data}"
                    ErrorLog().add_error(error_msg)
    except Exception as e:
        error_msg = f"Error connecting channel mapping: {e}"
        ErrorLog().add_error(
            error_msg, exception=e, traceback_str=traceback.format_exc()
        )

async def disconnect_channel_mapping(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    output_dev: OutputDevice,
    output_chan: OutputChannel,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    input_dev: InputDevice,
    input_chan: InputChannel,
    fetch_device_channels_callback=None,
):
    """Disconnect channel mapping, mirroring the connection logic
    
    Args:
        sender_node: The NMOS node containing the output device
        sender_device: The NMOS device with the output
        output_dev: The output device
        output_chan: The output channel to disconnect
        receiver_node: The NMOS node containing the input device
        receiver_device: The NMOS device with the input
        input_dev: The input device
        input_chan: The input channel
        fetch_device_channels_callback: Optional callback to refresh channel data after disconnection
    """
    if sender_node == receiver_node:
        # Same node, use local IS-08 to clear mapping
        await is_08_disconnect_channel_mapping(
            sender_node,
            sender_device,
            output_dev,
            output_chan,
            receiver_node,
            receiver_device,
            input_dev,
            input_chan,
            fetch_device_channels_callback,
        )
        return
    
    if output_chan == None and input_chan == None:
        # No specific channels, disconnect IS-05 connection
        await is_05_disconnect_devices(
            sender_node,
            sender_device,
            receiver_node,
            receiver_device,
        )
        return
    
    # Different nodes with channel-specific disconnection
    if output_chan != None and input_chan == None:
        # Clear mapping on sender side only
        await is_05_and_is_08_sender_channel_disconnect(
            sender_node,
            sender_device,
            output_dev,
            output_chan,
            receiver_node,
            receiver_device,
            fetch_device_channels_callback,
        )
        return
    
    if output_chan == None and input_chan != None:
        # Clear mapping on receiver side only
        await is_05_and_is_08_receiver_channel_disconnect(
            sender_node,
            sender_device,
            receiver_node,
            receiver_device,
            input_dev,
            input_chan,
            fetch_device_channels_callback,
        )
        return
    
    if output_chan != None and input_chan != None:
        # Clear mappings on both sides
        await is_05_and_is_08_disconnect_channel_mapping(
            sender_node,
            sender_device,
            output_dev,
            output_chan,
            receiver_node,
            receiver_device,
            input_dev,
            input_chan,
            fetch_device_channels_callback,
        )
        return
    
    assert False, "Unreachable code in disconnect_channel_mapping"


async def is_05_disconnect_devices(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
):
    """
    Disconnect sender from receiver using IS-05 Connection Management API.
    
    Args:
        sender_node: The NMOS node containing the sender
        sender_device: The sender device to disconnect
        receiver_node: The NMOS node containing the receiver
        receiver_device: The receiver device to disconnect from
    """
    try:
        # Build IS-05 connection API URLs
        sender_connection_url = f"{sender_node.connection_url}/single/senders/{sender_device.device_id}"
        receiver_connection_url = f"{receiver_node.connection_url}/single/receivers/{receiver_device.device_id}"
        
        logger.info(f"Disconnecting sender {sender_device.device_id} from receiver {receiver_device.device_id} using IS-05")
        
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Disable the sender
            sender_patch_data = {
                "master_enable": False,
                "activation": {"mode": "activate_immediate"},
            }
            
            async with session.patch(f"{sender_connection_url}/staged", json=sender_patch_data) as response:
                if response.status not in [200, 202]:
                    error_text = await response.text()
                    error_msg = f"Failed to disable sender: {response.status} - {error_text}"
                    ErrorLog().add_error(error_msg)
                    return
            
            # Disable the receiver
            receiver_patch_data = {
                "master_enable": False,
                "activation": {"mode": "activate_immediate"},
            }
            
            async with session.patch(f"{receiver_connection_url}/staged", json=receiver_patch_data) as response:
                if response.status not in [200, 202]:
                    error_text = await response.text()
                    error_msg = f"Failed to disable receiver: {response.status} - {error_text}"
                    ErrorLog().add_error(error_msg)
                    return
            
            logger.info(f"Successfully disconnected sender {sender_device.device_id} from receiver {receiver_device.device_id}")
            
    except Exception as e:
        error_msg = f"Error in IS-05 device disconnection: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def is_05_and_is_08_sender_channel_disconnect(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    output_dev: OutputDevice,
    output_chan: OutputChannel,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    fetch_device_channels_callback=None,
):
    """
    Clear IS-08 mapping on sender side for a specific output channel.
    
    Args:
        sender_node: The NMOS node containing the sender
        sender_device: The sender device
        output_dev: The output device containing the channel
        output_chan: The specific output channel to clear
        receiver_node: The NMOS node containing the receiver
        receiver_device: The receiver device
        fetch_device_channels_callback: Optional callback to refresh channel data
    """
    try:
        # Find RTP output device on sender node
        rtp_output_dev_sender = await find_rtp_device(sender_node, sender_device, is_input=False)
        if not rtp_output_dev_sender:
            error_msg = f"Could not find RTP output device on sender node {sender_node.node_id}"
            ErrorLog().add_error(error_msg)
            return
        
        # Clear mapping for the RTP channel on sender
        if rtp_output_dev_sender.channels:
            rtp_chan_sender = rtp_output_dev_sender.channels[0]
            await is_08_disconnect_channel_mapping(
                sender_node,
                sender_device,
                rtp_output_dev_sender,
                rtp_chan_sender,
                sender_node,
                sender_device,
                output_dev,
                output_chan,
                fetch_device_channels_callback,
            )
            logger.info(f"Cleared mapping for output channel {output_chan.label} on sender")
            
    except Exception as e:
        error_msg = f"Error in IS-08 sender channel disconnect: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def is_05_and_is_08_receiver_channel_disconnect(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    input_dev: InputDevice,
    input_chan: InputChannel,
    fetch_device_channels_callback=None,
):
    """
    Clear IS-08 mapping on receiver side for a specific input channel.
    
    Args:
        sender_node: The NMOS node containing the sender
        sender_device: The sender device
        receiver_node: The NMOS node containing the receiver
        receiver_device: The receiver device
        input_dev: The input device containing the channel
        input_chan: The specific input channel to clear
        fetch_device_channels_callback: Optional callback to refresh channel data
    """
    try:
        # Find RTP output device on receiver node (for routing)
        rtp_output_dev_receiver = await find_rtp_device(receiver_node, receiver_device, is_input=False)
        
        if rtp_output_dev_receiver and rtp_output_dev_receiver.channels:
            rtp_output_chan = rtp_output_dev_receiver.channels[0]
            await is_08_disconnect_channel_mapping(
                receiver_node,
                receiver_device,
                rtp_output_dev_receiver,
                rtp_output_chan,
                receiver_node,
                receiver_device,
                input_dev,
                input_chan,
                fetch_device_channels_callback,
            )
            logger.info(f"Cleared mapping for input channel {input_chan.label} on receiver")
        else:
            error_msg = "No RTP output device/channels available on receiver"
            ErrorLog().add_error(error_msg)
            
    except Exception as e:
        error_msg = f"Error in IS-08 receiver channel disconnect: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def is_05_and_is_08_disconnect_channel_mapping(
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    output_dev: OutputDevice,
    output_chan: OutputChannel,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    input_dev: InputDevice,
    input_chan: InputChannel,
    fetch_device_channels_callback=None,
):
    """
    Clear IS-08 mappings on both sender and receiver sides.
    
    Args:
        sender_node: The NMOS node containing the sender
        sender_device: The sender device
        output_dev: The output device
        output_chan: The output channel to clear
        receiver_node: The NMOS node containing the receiver
        receiver_device: The receiver device
        input_dev: The input device
        input_chan: The input channel to clear
        fetch_device_channels_callback: Optional callback to refresh channel data
    """
    try:
        # Clear mapping on sender side
        rtp_output_dev_sender = await find_rtp_device(sender_node, sender_device, is_input=False)
        if rtp_output_dev_sender and rtp_output_dev_sender.channels:
            rtp_chan_sender = rtp_output_dev_sender.channels[0]
            await is_08_disconnect_channel_mapping(
                sender_node,
                sender_device,
                rtp_output_dev_sender,
                rtp_chan_sender,
                sender_node,
                sender_device,
                output_dev,
                output_chan,
                fetch_device_channels_callback,
            )
            logger.info(f"Cleared mapping for output channel {output_chan.label} on sender")
        
        # Clear mapping on receiver side
        rtp_output_dev_receiver = await find_rtp_device(receiver_node, receiver_device, is_input=False)
        if rtp_output_dev_receiver and rtp_output_dev_receiver.channels:
            rtp_output_chan = rtp_output_dev_receiver.channels[0]
            await is_08_disconnect_channel_mapping(
                receiver_node,
                receiver_device,
                rtp_output_dev_receiver,
                rtp_output_chan,
                receiver_node,
                receiver_device,
                input_dev,
                input_chan,
                fetch_device_channels_callback,
            )
            logger.info(f"Cleared mapping for input channel {input_chan.label} on receiver")
        
        logger.info(f"Cross-node channel disconnect complete: {output_chan.label} -> {input_chan.label}")
        
    except Exception as e:
        error_msg = f"Error in IS-05/IS-08 cross-node disconnect: {e}"
        ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())


async def is_08_disconnect_channel_mapping(   
    sender_node: NMOS_Node,
    sender_device: NMOS_Device,
    output_dev: OutputDevice,
    output_chan: OutputChannel,
    receiver_node: NMOS_Node,
    receiver_device: NMOS_Device,
    input_dev: InputDevice,
    input_chan: InputChannel,
    fetch_device_channels_callback=None,
):

    """Disconnect an output channel mapping using IS-08 API
    
    Args:
        sender_node: The NMOS node containing the output device
        sender_device: The NMOS device with the output
        output_dev: The output device
        output_chan: The output channel to disconnect
        receiver_node: The NMOS node containing the input device (for symmetry with connect)
        receiver_device: The NMOS device with the input (for symmetry with connect)
        input_dev: The input device (for symmetry with connect)
        input_chan: The input channel (for symmetry with connect)
        fetch_device_channels_callback: Optional callback to refresh channel data after disconnection
    """
    try:
        # Build the IS-08 channel mapping API URL for activations
        activations_url = f"{sender_node.channelmapping_url}/map/activations"

        # Find the output channel key (ID or index as string)
        output_chan_key = output_chan.id
        if not output_chan_key:
            # Find the output channel index
            for idx, chan in enumerate(output_dev.channels):
                if chan is output_chan or (
                    not chan.id and chan.label == output_chan.label
                ):
                    output_chan_key = str(idx)
                    break
            if not output_chan_key:
                error_msg = (
                    f"Could not determine output channel key for {output_chan.label}"
                )
                ErrorLog().add_error(error_msg)
                return

        # Strip trailing slashes from device IDs
        output_dev_id_clean = output_dev.id.rstrip("/")

        # Prepare the activation request to clear the mapping
        # Setting input to null clears the mapping
        activation_data = {
            "activation": {"mode": "activate_immediate"},
            "action:": {  # note the extra ':' here is intentional as per IS-08 spec
                output_dev_id_clean: {
                    output_chan_key: {"input": None, "channel_index": None}
                }
            },
        }

        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # POST request to /map/activations for immediate activation
            async with session.post(activations_url, json=activation_data) as response:
                if response.status in [200, 202]:
                    logger.info(
                        f"Successfully cleared mapping for output channel {output_chan.label}"
                    )
                    # Refresh the channel data if callback provided
                    if fetch_device_channels_callback:
                        await fetch_device_channels_callback(sender_node, sender_device)
                else:
                    error_text = await response.text()
                    error_msg = f"Failed to clear channel mapping: {response.status} - {error_text}"
                    ErrorLog().add_error(error_msg)
    except Exception as e:
        error_msg = f"Error disconnecting channel mapping: {e}"
        ErrorLog().add_error(
            error_msg, exception=e, traceback_str=traceback.format_exc()
        )
