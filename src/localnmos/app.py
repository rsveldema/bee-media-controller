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

# Build date - automatically set when the module is imported
BUILD_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

logger = logging_utils.create_logger("localnmos.app")


class LocalNMOS(toga.App):
    def get_network_interfaces(self):
        """Get all network interfaces with their IP addresses using netifaces"""
        interfaces = []
        try:
            for iface in netifaces.interfaces():
                # Skip loopback and tunnel interfaces
                if iface.lower() in ['lo', 'loopback', 'sit0'] or 'loopback' in iface.lower():
                    continue
                
                addrs = netifaces.ifaddresses(iface)
                # Get IPv4 addresses - skip interfaces without IPv4
                if netifaces.AF_INET not in addrs:
                    continue
                
                for addr_info in addrs[netifaces.AF_INET]:
                    ip_addr = addr_info.get('addr')
                    # Skip if no IP, loopback IPs, or invalid IPs like 0.0.0.0
                    if ip_addr and not ip_addr.startswith('127.') and ip_addr != '0.0.0.0':
                        interfaces.append((iface, ip_addr))
        except Exception as e:
            print(f"Error getting network interfaces: {e}")
        return interfaces

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

    async def select_listen_ip(self, widget):
        """Handler for select listen IP button - opens dialog to choose network interface"""
        interfaces = self.get_network_interfaces()
        
        if not interfaces:
            await self.main_window.dialog(toga.InfoDialog(
                "No Interfaces Found",
                "No network interfaces with IP addresses were found."
            ))
            return
        
        # Build selection options
        options = []
        for iface, ip in interfaces:
            options.append(f"{iface}: {ip}")
        
        # Create a custom dialog using a window
        dialog = toga.Window(title="Select Listen IP", size=(400, 300))
        
        dialog_box = toga.Box(direction=COLUMN, style=Pack(padding=10))
        
        dialog_box.add(toga.Label(
            "Select the network interface to listen on:",
            style=Pack(padding_bottom=10)
        ))
        
        # Create selection widget
        selection = toga.Selection(
            items=options,
            style=Pack(padding_bottom=10)
        )
        dialog_box.add(selection)
        
        # Show current setting
        current_ip = self.registry.listen_ip if self.registry else "Not set"
        dialog_box.add(toga.Label(
            f"Current listen IP: {current_ip}",
            style=Pack(padding_bottom=10, font_size=9)
        ))
        
        async def on_select(widget):
            """Apply the selected interface"""
            selected = selection.value
            if selected:
                # Extract IP from selection (format: "interface: IP")
                selected_ip = selected.split(": ")[1]
                
                # Update registry listen_ip and restart
                if self.registry:
                    old_ip = self.registry.listen_ip
                    print(f"Listen IP changing from {old_ip} to {selected_ip}")
                    
                    dialog.close()
                    
                    # Show progress message
                    try:
                        # Restart registry with new IP
                        await self.restart_registry_with_new_ip(selected_ip)
                        
                        await self.main_window.dialog(toga.InfoDialog(
                            "Listen IP Updated",
                            f"Listen IP successfully changed to {selected_ip}.\n\n"
                            f"The registry has been restarted and is now listening on the new interface."
                        ))
                    except Exception as e:
                        await self.main_window.dialog(toga.ErrorDialog(
                            "Error Updating Listen IP",
                            f"Failed to restart registry with new IP: {e}"
                        ))
                else:
                    dialog.close()
        
        def on_cancel(widget):
            """Cancel the dialog"""
            dialog.close()
        
        # Add buttons
        button_box = toga.Box(direction=ROW, style=Pack(padding_top=10))
        select_button = toga.Button(
            "Select",
            on_press=on_select,
            style=Pack(padding=5, flex=1)
        )
        cancel_button = toga.Button(
            "Cancel",
            on_press=on_cancel,
            style=Pack(padding=5, flex=1)
        )
        button_box.add(select_button)
        button_box.add(cancel_button)
        dialog_box.add(button_box)
        
        dialog.content = dialog_box
        dialog.show()

    def startup(self):
        """Construct and show the Toga application."""
        self.model = UI_NMOS_ConnectionMatrix()
        self.registry = None  # Will be initialized in on_running

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
            on_press=self.select_listen_ip,
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
        self.listbox = toga.DetailedList(
            data=[],
            on_select=self.on_node_select,
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
            flex=1, on_resize=self.on_resize, on_press=self.on_press
        )

        # Initialize the matrix canvas drawing class
        self.matrix_canvas = RoutingMatrixCanvas(self.canvas, self.registry, self.model)

        self.draw_routing_matrix()

        main_box.add(nodes_box)
        main_box.add(toga.Divider())
        main_box.add(self.canvas)

        # Add toolbar and main content to container
        container_box.add(toolbar)
        container_box.add(main_box)

        self.main_window = toga.MainWindow(title=self.formal_name, size=(1600, 1200))
        self.main_window.content = container_box
        self.main_window.show()

        self.update_error_status()
        self.loop.call_soon_threadsafe(self.sync_task, "Hi")

    async def on_node_select(self, widget):
        """Handler for when a node is selected in the list"""
        if not widget.selection:
            return
        
        try:
            # For DetailedList, widget.selection returns the NMOS_Node object directly
            selected_nmos_node: NMOS_Node = widget.selection

            logger.info(f"Node selected: {selected_nmos_node.name}")
            
            # Build detailed information about the node
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
            
            # Show the details in a dialog
            await self.main_window.dialog(toga.InfoDialog(
                f"Node Details: {selected_nmos_node.name}",
                "\n".join(details)
            ))
        except Exception as e:
            import traceback
            # Show error dialog with details
            await self.main_window.dialog(toga.ErrorDialog(
                "Error",
                f"Failed to show node details: {e}\n\n{traceback.format_exc()}"
            ))

    def on_node_added(self, node: NMOS_Node):
        """Callback when an NMOS node is discovered"""
        # Schedule UI updates on the main thread to avoid hangs
        self.loop.call_soon_threadsafe(self._on_node_added_ui, node)

    def _on_node_added_ui(self, node: NMOS_Node):
        """UI update for node added (runs on main thread)"""
        print(f"GUI - Node discovered: {node.name} at {node.address}:{node.port}")
        
        # Refresh the listbox to show the new node
        self.listbox.data = self.model.get_nodes()
        
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
        print("NMOS Registry started - discovering nodes via MDNS...")

    def on_resize(self, widget, width, height, **kwargs):
        # On resize, recalculate margins to center the matrix
        if widget.context:
            # Calculate matrix dimensions


            self.draw_routing_matrix()        

    async def on_press(self, widget, x, y, **kwargs):
        """When clicking on the routing matrix at (x, y), toggle the channel mapping between output and input channels.
        Uses IS-08 channel mapping API to update the routing.
        """
        if not hasattr(self, 'matrix_canvas'):
            return
            



    async def on_exit(self):
        """Cleanup when the app is closing"""
        print("Shutting down NMOS registry...")
        if self.registry:
            await self.registry.stop()
        return True


def main():
    return LocalNMOS("LocalNMOS", "com.sh.localNMOS")