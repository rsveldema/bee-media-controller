"""
Local NMOS
"""

from abc import ABC, abstractmethod
import asyncio
import socket
import sys
import time
import uuid
from typing import List
from datetime import datetime
from enum import Enum
import netifaces
import aiohttp
import toga
from toga.style.pack import Pack, ROW, COLUMN
from toga.app import AppStartupMethod, OnExitHandler, OnRunningHandler
from toga.constants import Baseline
from toga.fonts import SANS_SERIF
from toga.colors import WHITE, rgb
from toga.sources import ListSource, ListSourceT, Row, Source

from localnmos import logging_utils
from localnmos.ui_model import (
    UI_NMOS_ConnectionMatrix,
    UI_NMOS_Row_Node,
    UI_NMOS_Column_Node,
)

from .registry import NMOSRegistry
from .nmos import NMOS_Node
from .error_log import ErrorLog
from .matrix_canvas import RoutingMatrixCanvas
from .node_details_handler import NodeDetailsHandler
from .listen_ip_selector import ListenIPSelector
from . import channel_mapping
from .listen_ip_selector import ListenIPSelector

# Build date - automatically set when the module is imported
BUILD_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

logger = logging_utils.create_logger("localnmos.app")


class LocalNMOS(toga.App):
    def get_local_ip(self):
        """Get the host's local IP address"""
        try:
            # Connect to an external address to determine local IP
            # We don't actually send data, just use the connection to find our local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return "Unable to determine IP"

    async def about(self, widget=None):
        """Show about dialog with IP address information"""
        ip_address = self.get_local_ip()
        await self.main_window.dialog(toga.InfoDialog(
            "About LocalNMOS",
            f"{self.formal_name}\n\n"
            f"Version: {self.version}\n"
            f"Build Date: {BUILD_DATE}\n"
            f"Host IP Address: {ip_address}\n\n"
            f"Local NMOS node discovery and routing matrix."
        ))


    def draw_routing_matrix(self):
        self.matrix_canvas.draw()
        # Update the canvas container size after drawing
        if hasattr(self, 'canvas_container') and hasattr(self.matrix_canvas, 'required_width'):
            self.canvas_container.style = Pack(
                width=self.matrix_canvas.required_width,
                height=self.matrix_canvas.required_height
            )

    def refresh_matrix_command(self, widget):
        """Handler for the Refresh Routing Matrix menu command"""
        print("Refreshing routing matrix...")
        self.draw_routing_matrix()
        self.update_error_status()

    async def show_error_log_command(self, widget):
        """Handler for the Show Error Log menu command"""
        error_log = ErrorLog()
        errors = error_log.get_all_errors()
        
        if not errors:
            await self.main_window.dialog(toga.InfoDialog(
                "Error Log",
                "No errors have been logged."
            ))
            return
        
        # Format all errors for display
        error_text = f"Total Errors: {len(errors)}\n\n"
        for i, error in enumerate(errors[-50:], 1):  # Show last 50 errors
            error_text += f"{i}. {error}\n"
        
        # Show in a scrollable dialog
        await self.main_window.dialog(toga.InfoDialog(
            f"Error Log ({len(errors)} errors)",
            error_text
        ))

    def update_error_status(self):
        """Update the error status label with the last error"""
        if not hasattr(self, 'error_status_label'):
            return
            
        error_log = ErrorLog()
        last_error = error_log.get_last_error()
        
        if last_error:
            error_count = error_log.get_error_count()
            # Truncate error message to fit in status bar
            error_msg = str(last_error)
            if len(error_msg) > 60:
                error_msg = error_msg[:57] + "..."
            self.error_status_label.text = f"⚠ {error_count} error(s): {error_msg}"
            self.error_status_label.style.color = rgb(200, 50, 50)
        else:
            self.error_status_label.text = "No errors"
            self.error_status_label.style.color = rgb(100, 100, 100)

    def zoom_in(self, widget):
        """Handler for zoom in button"""
        if hasattr(self, 'matrix_canvas'):
            self.matrix_canvas.set_zoom_factor(min(self.matrix_canvas.zoom_factor * 1.2, 5.0))
            self.draw_routing_matrix()

    def zoom_out(self, widget):
        """Handler for zoom out button"""
        if hasattr(self, 'matrix_canvas'):
            self.matrix_canvas.set_zoom_factor(max(self.matrix_canvas.zoom_factor / 1.2, 0.2))
            self.draw_routing_matrix()

    def refresh_mdns(self, widget):
        """Handler for refresh button - triggers immediate MDNS announcement"""
        print("Triggering MDNS refresh...")
        if self.registry:
            # Schedule the async task on the event loop
            asyncio.create_task(self.registry.refresh_mdns_announcement())
        else:
            print("Registry not initialized yet")

    def startup(self):
        """Construct and show the Toga application."""
        self.model = UI_NMOS_ConnectionMatrix()
        self.registry = None  # Will be initialized in on_running
        self.node_details_handler = None  # Will be initialized after main_window is created
        self.listen_ip_selector = None  # Will be initialized after main_window is created

        # Create menu commands
        error_log_command = toga.Command(
            self.show_error_log_command,
            text="Show Error Log",
            tooltip="Show all errors from the registry",
            group=toga.Group.VIEW
        )
        self.commands.add(error_log_command)

        # Create main container with toolbar
        container_box = toga.Box(direction=COLUMN)

        # Create toolbar
        toolbar = toga.Box(direction=ROW, style=Pack(padding=5))
        
        zoom_in_button = toga.Button(
            "⊕",
            on_press=self.zoom_in,
            style=Pack(padding=5)
        )
        zoom_out_button = toga.Button(
            "⊖",
            on_press=self.zoom_out,
            style=Pack(padding=5)
        )
        
        refresh_button = toga.Button(
            "↻",
            on_press=self.refresh_mdns,
            style=Pack(padding=5)
        )
        
        select_ip_button = toga.Button(
            "⚡",
            on_press=lambda widget: asyncio.create_task(self.listen_ip_selector.select_listen_ip(widget)),
            style=Pack(padding=5)
        )
        
        self.zoom_label = toga.Label(
            "Zoom: 100%",
            style=Pack(padding=5)
        )
        
        toolbar.add(zoom_in_button)
        toolbar.add(zoom_out_button)
        toolbar.add(refresh_button)
        toolbar.add(select_ip_button)
        toolbar.add(self.zoom_label)

        main_box = toga.Box(direction=ROW, style=Pack(flex=1))

        nodes_box = toga.Box(direction=COLUMN, style=Pack(width=300))
        
        # Create the main_window first (we'll set content later)
        self.main_window = toga.MainWindow(title=self.formal_name, size=(1600, 1200))
        
        # Now initialize the node details handler with the main window
        self.node_details_handler = NodeDetailsHandler(self.main_window)
        
        # Initialize the listen IP selector with the main window
        # We'll set the registry reference later in on_running
        self.listen_ip_selector = ListenIPSelector(
            self.main_window,
            None,  # registry will be set in on_running
            self.restart_registry_with_new_ip
        )
        
        self.listbox = toga.DetailedList(
            data=[],
            on_select=self.node_details_handler.on_node_select,
            style=Pack(flex=1)
        )
        nodes_box.add(toga.Label("NMOS Nodes (MDNS Discovery)"))
        nodes_box.add(self.listbox)

        # Add error status label
        self.error_status_label = toga.Label(
            "No errors",
            style=Pack(padding=5, font_size=9, color=rgb(100, 100, 100))
        )
        nodes_box.add(self.error_status_label)

        self.canvas = toga.Canvas(
            on_resize=self.on_resize, on_press=self.on_press,
            style=Pack(flex=1)
        )

        # Initialize the matrix canvas drawing class
        self.matrix_canvas = RoutingMatrixCanvas(self.canvas, self.registry, self.model)

        # Create a container box for the canvas with explicit size
        self.canvas_container = toga.Box(
            style=Pack(width=800, height=600)
        )
        self.canvas_container.add(self.canvas)

        self.draw_routing_matrix()

        # Wrap canvas container in a ScrollContainer to make it scrollable
        scroll_container = toga.ScrollContainer(
            content=self.canvas_container,
            horizontal=True,
            vertical=True,
            style=Pack(flex=1)
        )

        main_box.add(nodes_box)
        main_box.add(toga.Divider())
        main_box.add(scroll_container)

        # Add toolbar and main content to container
        container_box.add(toolbar)
        container_box.add(main_box)

        # Set the content and show (main_window was already created earlier)
        self.main_window.content = container_box
        self.main_window.show()

        self.update_error_status()
        self.loop.call_soon_threadsafe(self.sync_task, "Hi")

    def on_node_added(self, node: NMOS_Node):
        """Callback when an NMOS node is discovered"""
        # Schedule UI updates on the main thread to avoid hangs
        self.loop.call_soon_threadsafe(self._on_node_added_ui, node)

    def _on_node_added_ui(self, node: NMOS_Node):
        """UI update for node added (runs on main thread)"""
        print(f"GUI - Node discovered: {node.name} at {node.address}:{node.port}")
        
        # Ensure model has the latest reference
        if self.registry:
            self.model.nodes = self.registry.nodes
        
        # Get the updated nodes list
        nodes_list = self.model.get_nodes()
        print(f"GUI - Total nodes in model: {len(nodes_list)}")
        
        # Refresh the listbox to show the new node
        self.listbox.data.clear()
        for node_item in nodes_list:
            self.listbox.data.append(node_item)
        
        # Redraw the matrix with the new node
        self.draw_routing_matrix()

    def on_node_removed(self, node: NMOS_Node):
        """Callback when an NMOS node is removed from the network"""
        # Schedule UI updates on the main thread to avoid hangs
        self.loop.call_soon_threadsafe(self._on_node_removed_ui, node)

    def _on_node_removed_ui(self, node: NMOS_Node):
        """UI update for node removed (runs on main thread)"""
        print(f"node removed: {node.name}")
        
        # Refresh the listbox to remove the node
        self.listbox.data = self.model.get_nodes()
        
        self.draw_routing_matrix()

    def on_device_added(self, node_id: str, device_id: str):
        """Callback when a device is registered to a node"""
        self.loop.call_soon_threadsafe(self._on_device_added_ui, node_id, device_id)

    def _on_device_added_ui(self, node_id: str, device_id: str):
        """UI update for device added (runs on main thread)"""
        print(f"Device added: {device_id} to node {node_id}")
        self.draw_routing_matrix()


    def on_sender_added(self, node_id: str, device_id: str, sender_id: str):
        """Callback when a sender is registered to a device"""
        print(f"Sender added: {sender_id} to device {device_id} on node {node_id}")
        self.loop.call_soon_threadsafe(self.draw_routing_matrix)

    def on_receiver_added(self, node_id: str, device_id: str, receiver_id: str):
        """Callback when a receiver is registered to a device"""
        print(f"Receiver added: {receiver_id} to device {device_id} on node {node_id}")
        self.loop.call_soon_threadsafe(self.draw_routing_matrix)

    def on_channel_updated(self, node_id: str, device_id: str):
        """Callback when device channels are updated"""
        self.loop.call_soon_threadsafe(self._on_channel_updated_ui, node_id, device_id)

    def _on_channel_updated_ui(self, node_id: str, device_id: str):
        """UI update for channel changes (runs on main thread)"""
        print(f"Channels updated for device {device_id} on node {node_id}")
        self.draw_routing_matrix()

    def sync_task(self, arg: str):
        print(f"running sync task: {arg}")
        asyncio.create_task(self.async_task("from sync task"))

    async def async_task(self, arg):
        print(f"running async task: {arg}")
        
        # Periodically update error status
        while True:
            await asyncio.sleep(2)  # Update every 2 seconds
            try:
                self.loop.call_soon_threadsafe(self.update_error_status)
            except:
                pass  # Ignore errors if app is closing

    async def restart_registry_with_new_ip(self, new_ip: str):
        """Restart the registry with a new listen IP address"""
        if not self.registry:
            return
        
        print(f"Restarting registry with new listen IP: {new_ip}")
        
        # Stop the existing registry
        await self.registry.stop()
        
        # Update the listen IP
        self.registry.listen_ip = new_ip
        
        # Restart the registry
        await self.registry.start()
        
        print(f"Registry restarted successfully on {new_ip}")

    async def on_running(self):
        print(f"on_running")

        # Initialize NMOS registry with MDNS discovery
        self.registry = NMOSRegistry(
            node_added_callback=self.on_node_added,
            node_removed_callback=self.on_node_removed,
            device_added_callback=self.on_device_added,
            sender_added_callback=self.on_sender_added,
            receiver_added_callback=self.on_receiver_added,
            channel_updated_callback=self.on_channel_updated,
        )
        await self.registry.start()
        self.model.nodes = self.registry.nodes
        
        # Update the matrix canvas with the registry reference
        if self.matrix_canvas:
            print(f"DEBUG: Setting matrix_canvas.registry to {self.registry}")
            self.matrix_canvas.registry = self.registry
            print(f"DEBUG: matrix_canvas.registry is now {self.matrix_canvas.registry}")
        else:
            print(f"DEBUG: self.matrix_canvas is None, cannot set registry")
        
        # Update the listen IP selector with the registry reference
        if self.listen_ip_selector:
            self.listen_ip_selector.registry = self.registry
        
        print("NMOS Registry started - discovering nodes via MDNS...")

    def on_resize(self, widget, width, height, **kwargs):
        # On resize, recalculate margins to center the matrix
        if widget.context:
            # Calculate matrix dimensions


            self.draw_routing_matrix()        

    def _is_channel_connected(self, sender_node, sender_device, sender_obj, sender_channel,
                               receiver_node, receiver_device, receiver_obj, receiver_channel):
        """Check if there's an active connection between sender and receiver channels"""
        # Use the same logic as the matrix canvas _is_connected method
        if not self.registry:
            return False
        
        # Get the actual NMOS node objects
        s_node = sender_node.node if hasattr(sender_node, 'node') else None
        r_node = receiver_node.node if hasattr(receiver_node, 'node') else None
        
        if not s_node or not r_node:
            return False
        
        # For now, check if both are on the same node and have channel mappings
        if s_node.node_id == r_node.node_id:
            # Same node - check IS-08 channel mappings
            nmos_node = self.registry.nodes.get(s_node.node_id)
            if nmos_node:
                s_device = sender_device.device if hasattr(sender_device, 'device') else None
                if s_device:
                    for device in nmos_node.devices:
                        if device.device_id == s_device.device_id:
                            # Check if sender's output channels are mapped to receiver's input
                            if receiver_channel and sender_channel:
                                r_chan = receiver_channel.channel if hasattr(receiver_channel, 'channel') else None
                                s_chan = sender_channel.channel if hasattr(sender_channel, 'channel') else None
                                if r_chan and s_chan:
                                    for output_dev in device.is08_output_channels:
                                        for out_ch in output_dev.channels:
                                            # Match output channel by ID (if not empty) or label
                                            id_match = out_ch.id and s_chan.id and out_ch.id == s_chan.id
                                            label_match = out_ch.label == s_chan.label
                                            if id_match or label_match:
                                                # Check if this output channel has a mapped InputChannel
                                                if out_ch.mapped_device:
                                                    # Compare the mapped InputChannel with the receiver channel
                                                    # Match by ID (if not empty) or label
                                                    input_id_match = (out_ch.mapped_device.id and r_chan.id and 
                                                                    out_ch.mapped_device.id == r_chan.id)
                                                    input_label_match = out_ch.mapped_device.label == r_chan.label
                                                    if input_id_match or input_label_match:
                                                        return True
        
        return False

    async def on_press(self, widget, x, y, **kwargs):
        """When clicking on the routing matrix at (x, y), toggle the channel mapping between output and input channels.
        Uses IS-08 channel mapping API to update the routing.
        """
        if not hasattr(self, 'matrix_canvas'):
            return
        
        # Get the cell information at the clicked position
        sender_info, receiver_info = self.matrix_canvas.get_cell_at_position(x, y)
        
        if sender_info is None or receiver_info is None:
            # Click was outside the matrix
            return
        
        # Unpack sender information
        sender_node, sender_device, sender_obj, sender_channel = sender_info
        
        # Unpack receiver information
        receiver_node, receiver_device, receiver_obj, receiver_channel = receiver_info
        
        # Log the click information
        sender_node_name = sender_node.get_name() if sender_node else "Unknown"
        sender_device_name = sender_device.get_name() if sender_device else "Unknown"
        sender_name = sender_obj.get_name() if sender_obj else "Unknown"
        sender_channel_name = sender_channel.get_name() if sender_channel else "No channel"
        
        receiver_node_name = receiver_node.get_name() if receiver_node else "Unknown"
        receiver_device_name = receiver_device.get_name() if receiver_device else "Unknown"
        receiver_name = receiver_obj.get_name() if receiver_obj else "Unknown"
        receiver_channel_name = receiver_channel.get_name() if receiver_channel else "No channel"
        
        print(f"Matrix cell clicked:")
        print(f"  Sender: {sender_node_name} / {sender_device_name} / {sender_name} / {sender_channel_name}")
        print(f"  Receiver: {receiver_node_name} / {receiver_device_name} / {receiver_name} / {receiver_channel_name}")
        
        # Check if a connection exists
        is_connected = self._is_channel_connected(
            sender_node, sender_device, sender_obj, sender_channel,
            receiver_node, receiver_device, receiver_obj, receiver_channel
        )
        
        # Get the actual NMOS objects from the UI model wrappers
        sender_nmos_node = sender_node.node if hasattr(sender_node, 'node') else None
        sender_nmos_device = sender_device.device if hasattr(sender_device, 'device') else None
        receiver_nmos_node = receiver_node.node if hasattr(receiver_node, 'node') else None
        receiver_nmos_device = receiver_device.device if hasattr(receiver_device, 'device') else None
        
        if not all([sender_nmos_node, sender_nmos_device, receiver_nmos_node, receiver_nmos_device]):
            print("Error: Could not extract NMOS objects from UI model")
            return
        
        # Extract output/input devices and channels
        # For channels, we have the channel object directly
        sender_output_chan = sender_channel.channel if hasattr(sender_channel, 'channel') else None
        receiver_input_chan = receiver_channel.channel if hasattr(receiver_channel, 'channel') else None
        
        # Find the output device containing the sender channel
        sender_output_dev = None
        if sender_output_chan and sender_nmos_device.is08_output_channels:
            for out_dev in sender_nmos_device.is08_output_channels:
                for chan in out_dev.channels:
                    # Match by ID (if not empty) or label
                    id_match = chan.id and sender_output_chan.id and chan.id == sender_output_chan.id
                    label_match = chan.label == sender_output_chan.label
                    if id_match or label_match:
                        sender_output_dev = out_dev
                        break
                if sender_output_dev:
                    break
        
        # Find the input device containing the receiver channel
        receiver_input_dev = None
        if receiver_input_chan and receiver_nmos_device.is08_input_channels:
            for in_dev in receiver_nmos_device.is08_input_channels:
                for chan in in_dev.channels:
                    # Match by ID (if not empty) or label
                    id_match = chan.id and receiver_input_chan.id and chan.id == receiver_input_chan.id
                    label_match = chan.label == receiver_input_chan.label
                    if id_match or label_match:
                        receiver_input_dev = in_dev
                        break
                if receiver_input_dev:
                    break
        
        # Debug logging
        print(f"Extracted devices/channels:")
        print(f"  sender_output_dev: {sender_output_dev.id if sender_output_dev else 'None'}")
        print(f"  sender_output_chan: {sender_output_chan.id if sender_output_chan else 'None'}")
        print(f"  receiver_input_dev: {receiver_input_dev.id if receiver_input_dev else 'None'}")
        print(f"  receiver_input_chan: {receiver_input_chan.id if receiver_input_chan else 'None'}")
        
        if not sender_output_dev or not receiver_input_dev:
            print("Error: Could not find IS-08 output or input device for the selected channels")
            return
        
        try:
            if is_connected:
                # Disconnect the mapping
                print(f"Disconnecting channel mapping...")
                await channel_mapping.disconnect_channel_mapping(
                    sender_nmos_node,
                    sender_nmos_device,
                    sender_output_dev,
                    sender_output_chan,
                    receiver_nmos_node,
                    receiver_nmos_device,
                    receiver_input_dev,
                    receiver_input_chan,
                    fetch_device_channels_callback=self.registry.fetch_device_channels if self.registry else None
                )
                print("Channel mapping disconnected successfully")
            else:
                # Connect the mapping
                print(f"Connecting channel mapping...")
                await channel_mapping.connect_channel_mapping(
                    sender_nmos_node,
                    sender_nmos_device,
                    sender_output_dev,
                    sender_output_chan,
                    receiver_nmos_node,
                    receiver_nmos_device,
                    receiver_input_dev,
                    receiver_input_chan,
                    fetch_device_channels_callback=self.registry.fetch_device_channels if self.registry else None
                )
                print("Channel mapping connected successfully")
            
            # Ensure registry is set on matrix canvas before refreshing
            if self.registry and hasattr(self, 'matrix_canvas'):
                self.matrix_canvas.registry = self.registry
            
            # Refresh the matrix to show updated connections
            self.draw_routing_matrix()
            
        except Exception as e:
            print(f"Error toggling channel mapping: {e}")
            import traceback
            traceback.print_exc()
            



    async def on_exit(self):
        """Cleanup when the app is closing"""
        print("Shutting down NMOS registry...")
        if self.registry:
            await self.registry.stop()
        return True


def main():
    return LocalNMOS("LocalNMOS", "com.sh.localNMOS")