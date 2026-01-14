"""
Node Details Handler
Handles displaying detailed information about NMOS nodes when selected.
"""

import traceback
import toga

from localnmos import logging_utils
from .nmos import NMOS_Node

logger = logging_utils.create_logger("localnmos.node_details_handler")


class NodeDetailsHandler:
    """Handles node selection and details display"""
    
    def __init__(self, main_window: toga.MainWindow):
        """
        Initialize the node details handler.
        
        Args:
            main_window: The main application window for displaying dialogs
        """
        self.main_window = main_window
    
    async def on_node_select(self, widget):
        """Handler for when a node is selected in the list"""
        if not widget.selection:
            logger.debug("on_node_select called but no selection")
            return
        
        try:
            # For DetailedList, widget.selection returns a Row object
            # The actual NMOS_Node is in the 'title' attribute of the Row
            selected_nmos_node: NMOS_Node = widget.selection.title
            
            logger.info(f"Node selected: {selected_nmos_node.name}")
            
            # Build detailed information about the node
            details = self._build_node_details(selected_nmos_node)
            
            # Show the details in a dialog
            await self.main_window.dialog(toga.InfoDialog(
                f"Node Details: {selected_nmos_node.name}",
                "\n".join(details)
            ))
        except Exception as e:
            # Show error dialog with details
            await self.main_window.dialog(toga.ErrorDialog(
                "Error",
                f"Failed to show node details: {e}\n\n{traceback.format_exc()}"
            ))
    
    def _build_node_details(self, selected_nmos_node: NMOS_Node) -> list:
        """
        Build a list of detail strings for the selected node.
        
        Args:
            selected_nmos_node: The NMOS node to get details for
            
        Returns:
            List of formatted detail strings
        """
        details = []
        details.append(f"Node: {selected_nmos_node.name}")
        details.append(f"Node ID: {selected_nmos_node.node_id}")
        details.append(f"Address: {selected_nmos_node.address}:{selected_nmos_node.port}")
        details.append("")
        
        # Use the selected node directly (already has all the data)
        details.append(f"Devices: {len(selected_nmos_node.devices)}")
        for device in selected_nmos_node.devices:
            details.append(f"  • Device: {device.label} (ID: {device.device_id})")
            
            # Show sources
            if hasattr(device, 'sources') and device.sources:
                details.append(f"    Sources: {len(device.sources)}")
                for source in device.sources:
                    if hasattr(source, 'source_id'):
                        details.append(f"      - {source.label} (ID: {source.source_id})")
            
            # Show senders
            if hasattr(device, 'senders') and device.senders:
                details.append(f"    Senders: {len(device.senders)}")
                for sender in device.senders:
                    if hasattr(sender, 'sender_id'):
                        details.append(f"      - {sender.label} (ID: {sender.sender_id})")
            
            # Show receivers
            if hasattr(device, 'receivers') and device.receivers:
                details.append(f"    Receivers: {len(device.receivers)}")
                for receiver in device.receivers:
                    if hasattr(receiver, 'receiver_id'):
                        details.append(f"      - {receiver.label} (ID: {receiver.receiver_id})")
            
            # Show channels (IS-08)
            if hasattr(device, 'is08_input_channels') and device.is08_input_channels:
                details.append(f"    Input Channels:")
                for chan_dev in device.is08_input_channels:
                    details.append(f"      Input Device: {chan_dev.name}")
                    for channel in chan_dev.channels:
                        details.append(f"        - Channel {channel.label} (ID: {channel.id})")
            
            if hasattr(device, 'is08_output_channels') and device.is08_output_channels:
                details.append(f"    Output Channels:")
                for chan_dev in device.is08_output_channels:
                    details.append(f"      Output Device: {chan_dev.name}")
                    for channel in chan_dev.channels:
                        mapped_info = ""
                        if channel.mapped_device:
                            mapped_info = f" → mapped to {channel.mapped_device.id}"
                        details.append(f"        - Channel {channel.label} (ID: {channel.id}){mapped_info}")
            
            details.append("")
        
        return details
