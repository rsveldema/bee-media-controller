"""
Local NMOS
"""

import asyncio
import socket
import sys
from typing import List
from datetime import datetime
from enum import Enum
import toga
from toga.style.pack import Pack, ROW, COLUMN
from toga.app import AppStartupMethod, OnExitHandler, OnRunningHandler
from toga.constants import Baseline
from toga.fonts import SANS_SERIF
from toga.colors import WHITE, rgb
from toga.sources import ListSource

from .registry import NMOSRegistry
from .nmos import NMOS_Node
from .error_log import ErrorLog
from .matrix_canvas import RoutingMatrixCanvas

# Build date - automatically set when the module is imported
BUILD_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class ChannelType(Enum):
    INPUT = "input"
    OUTPUT = "output"

class UI_NMOS_Channel:
    def __init__(self, type: ChannelType, id: str, name: str):
        self.type = type
        self.id = id
        self.name = name

class UI_Matrix_Connection:
    def __init__(self, endpoint: 'UI_NMOS_Sender | UI_NMOS_Receiver', channel: 'UI_NMOS_Channel | None' ):
        self.endpoint = endpoint
        self.channel = channel
        self.device = endpoint.device
        self.node = endpoint.parent

class UI_NMOS_Device:
    def __init__(self, device_id: str, parent: 'UI_NMOS_Node', label: str = "", description: str = "") -> None:
        self.device_id = device_id
        self.parent = parent
        self.label = label
        self.description = description
        self.senders: list['UI_NMOS_Sender'] = []
        self.receivers: list['UI_NMOS_Receiver'] = []
        self.channels: list[UI_NMOS_Channel] = []


class UI_NMOS_Sender:
    def __init__(self, sender_id: str, device: UI_NMOS_Device) -> None:
        self.sender_id = sender_id
        self.device = device
        self.parent = device.parent  # Reference to node for convenience
        self.connected_receivers: list['UI_NMOS_Receiver'] = []
        self.channels: list[UI_NMOS_Channel] = [c for c in device.channels if c.type == ChannelType.OUTPUT]


class UI_NMOS_Receiver:
    def __init__(self, receiver_id: str, device: UI_NMOS_Device) -> None:
        self.receiver_id = receiver_id
        self.device = device
        self.parent = device.parent  # Reference to node for convenience
        self.connected_senders: list[UI_NMOS_Sender] = []
        self.channels: list[UI_NMOS_Channel] = [c for c in device.channels if c.type == ChannelType.INPUT]


class UI_NMOS_Node:
    """represents an NMOS Node"""

    def __init__(self, node_id: str, list_entry: dict) -> None:
        self.node_id = node_id
        self.list_entry = list_entry
        self.devices: list[UI_NMOS_Device] = []
        self.senders: list[UI_NMOS_Sender] = []
        self.receivers: list[UI_NMOS_Receiver] = []

    def add_device(self, dev: UI_NMOS_Device):
        if dev not in self.devices:
            self.devices.append(dev)

    def add_sender(self, sender: UI_NMOS_Sender):
        if sender not in self.senders:
            self.senders.append(sender)

    def add_receiver(self, receiver: UI_NMOS_Receiver):
        if receiver not in self.receivers:
            self.receivers.append(receiver)

    def remove_sender(self, sender: UI_NMOS_Sender):
        if sender in self.senders:
            self.senders.remove(sender)

    def remove_receiver(self, receiver: UI_NMOS_Receiver):
        if receiver in self.receivers:
            self.receivers.remove(receiver)


class UIModel:
    def __init__(self) -> None:
        self.nodes = ListSource(accessors=["title", "subtitle", "icon"], data=[])
        self.node_map: dict[str, UI_NMOS_Node] = {}

    def get_nodes(self) -> List[UI_NMOS_Node]:
        map = self.node_map
        ret: List[UI_NMOS_Node] = []
        for n in map.values():
            ret.append(n)
        return ret

    def add_node(
        self,
        node_id: str,
        node: str,
        subtitle: str,
        icon: toga.Icon = toga.Icon.DEFAULT_ICON,
    ):
        entry = {"title": node, "subtitle": subtitle, "icon": icon, "id" : node_id}
        self.nodes.append(entry)
        if node_id:
            self.node_map[node_id] = UI_NMOS_Node(
                node_id=node_id, list_entry=entry
            )

    def remove_node(self, node_id: str):
        if node_id in self.node_map:
            node = self.node_map.pop(node_id)
        # Find and remove from ListSource
        for item in self.nodes:
            if item.__dict__["id"]  == node_id:
                self.nodes.remove(item)
                break


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

    def about(self):
        """Show about dialog with IP address information"""
        ip_address = self.get_local_ip()
        self.main_window.info_dialog(
            "About LocalNMOS",
            f"{self.formal_name}\n\n"
            f"Version: {self.version}\n"
            f"Build Date: {BUILD_DATE}\n"
            f"Host IP Address: {ip_address}\n\n"
            f"Local NMOS node discovery and routing matrix."
        )

    def get_senders(self) -> List[UI_Matrix_Connection]:
        """Get all sender channels as UI_Matrix_Connection objects for hierarchical display"""
        ret = []
        for n in self.model.get_nodes():
            for s in n.senders:
                if s.channels:
                    for ch in s.channels:
                        ret.append(UI_Matrix_Connection(s, ch))
                else:
                    ret.append(UI_Matrix_Connection(s, None))
        return ret


    def get_receivers(self) -> List[UI_Matrix_Connection]:
        """Get all receivers as UI_Matrix_Connection objects for hierarchical display"""
        ret = []
        for n in self.model.get_nodes():
            for r in n.receivers:
                if r.channels:
                    for ch in r.channels:
                        ret.append(UI_Matrix_Connection(r, ch))
                else:
                    ret.append(UI_Matrix_Connection(r, None))
        return ret

    def draw_routing_matrix(self):
        """draw the routing matrix with hierarchical nodes and devices. On the horizonal axis we have the transmitting devices,
        on the vertical axis we have the receiving devices. A checkmark is drawn on the intersections where routing is active.
        """
        if not hasattr(self, "matrix_canvas") or not self.matrix_canvas:
            return

        # Update zoom label
        if hasattr(self, 'zoom_label'):
            self.zoom_label.text = f"Zoom: {int(self.matrix_canvas.zoom_factor * 100)}%"

        # Use all nodes as both potential senders and receivers (hierarchical tuples)
        senders = self.get_senders()  # List of UI_Matrix_Connection
        receivers = self.get_receivers()  # List of UI_Matrix_Connection

        # Update registry reference in matrix canvas
        self.matrix_canvas.registry = self.registry
        
        # Draw the matrix
        self.matrix_canvas.draw_matrix(senders, receivers)

    def refresh_matrix_command(self, widget):
        """Handler for the Refresh Routing Matrix menu command"""
        print("Refreshing routing matrix...")
        self.draw_routing_matrix()
        self.update_error_status()

    def show_error_log_command(self, widget):
        """Handler for the Show Error Log menu command"""
        error_log = ErrorLog()
        errors = error_log.get_all_errors()
        
        if not errors:
            self.main_window.info_dialog(
                "Error Log",
                "No errors have been logged."
            )
            return
        
        # Format all errors for display
        error_text = f"Total Errors: {len(errors)}\n\n"
        for i, error in enumerate(errors[-50:], 1):  # Show last 50 errors
            error_text += f"{i}. {error}\n"
        
        # Show in a scrollable dialog (using info_dialog as a simple option)
        self.main_window.info_dialog(
            f"Error Log ({len(errors)} errors)",
            error_text
        )

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
        self.model = UIModel()
        self.registry = None  # Will be initialized in on_running

        # Create menu commands
        refresh_command = toga.Command(
            self.refresh_matrix_command,
            text="Refresh Routing Matrix",
            tooltip="Refresh the routing matrix display",
            group=toga.Group.VIEW
        )
        self.commands.add(refresh_command)

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
        
        self.zoom_label = toga.Label(
            "Zoom: 100%",
            style=Pack(padding=5)
        )
        
        toolbar.add(zoom_in_button)
        toolbar.add(zoom_out_button)
        toolbar.add(refresh_button)
        toolbar.add(self.zoom_label)

        main_box = toga.Box(direction=ROW, style=Pack(flex=1))

        nodes_box = toga.Box(direction=COLUMN, style=Pack(width=300))
        self.listbox = toga.DetailedList(
            data=self.model.nodes,
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
        self.matrix_canvas = RoutingMatrixCanvas(self.canvas, self.registry)

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

    def on_node_select(self, widget):
        """Handler for when a device is selected in the list"""
        if widget.selection:
            node_name = widget.selection.title
            node_id = widget.selection.__dict__.get("id")
            
            # Build detailed information string
            info_text = f"Node Name: {node_name}\n"
            info_text += f"Connection: {widget.selection.subtitle}\n\n"
            
            # Get the UI node from the model
            if node_id and node_id in self.model.node_map:
                ui_node = self.model.node_map[node_id]
                
                # List devices with label and description from NMOS registry
                if ui_node.devices:
                    info_text += f"Devices ({len(ui_node.devices)}):\n"
                    for dev in ui_node.devices:
                        info_text += f"  • {dev.device_id}\n"
                        
                        # Get label and description from NMOS_Device if available
                        if self.registry and node_id in self.registry.nodes:
                            nmos_node = self.registry.nodes[node_id]
                            for nmos_device in nmos_node.devices:
                                if nmos_device.device_id == dev.device_id:
                                    if nmos_device.label:
                                        info_text += f"    Label: {nmos_device.label}\n"
                                    if nmos_device.description:
                                        info_text += f"    Description: {nmos_device.description}\n"
                                    break
                    info_text += "\n"
                
                # List senders
                if ui_node.senders:
                    info_text += f"Senders ({len(ui_node.senders)}):\n"
                    for sender in ui_node.senders:
                        info_text += f"  • {sender.sender_id}\n"
                        if sender.channels:
                            for ch in sender.channels:
                                info_text += f"    - {ch.name} (ID: {ch.id})\n"
                    info_text += "\n"
                
                # List receivers
                if ui_node.receivers:
                    info_text += f"Receivers ({len(ui_node.receivers)}):\n"
                    for receiver in ui_node.receivers:
                        info_text += f"  • {receiver.receiver_id}\n"
                        if receiver.channels:
                            for ch in receiver.channels:
                                info_text += f"    - {ch.name} (ID: {ch.id})\n"
                    info_text += "\n"
                
                # Get channel information from the NMOS registry if available
                if self.registry and node_id in self.registry.nodes:
                    nmos_node = self.registry.nodes[node_id]
                    
                    # Show IS-08 channel mapping information
                    for device in nmos_node.devices:
                        if device.is08_input_channels:
                            info_text += f"\nInput Channels for {device.device_id}:\n"
                            for input_dev in device.is08_input_channels:
                                info_text += f"  Input Device: {input_dev.name or input_dev.id}\n"
                                for ch in input_dev.channels:
                                    info_text += f"    • {ch.label} (ID: {ch.id or 'N/A'})\n"
                        
                        if device.is08_output_channels:
                            info_text += f"\nOutput Channels for {device.device_id}:\n"
                            for output_dev in device.is08_output_channels:
                                info_text += f"  Output Device: {output_dev.name or output_dev.id}\n"
                                for ch in output_dev.channels:
                                    mapped_info = ""
                                    if ch.mapped_device:
                                        mapped_info = f" → {ch.mapped_device.label}"
                                    info_text += f"    • {ch.label} (ID: {ch.id or 'N/A'}){mapped_info}\n"
            
            self.main_window.info_dialog(
                "NMOS Node Information",
                info_text
            )

    def on_node_added(self, node: NMOS_Node):
        """Callback when an NMOS node is discovered"""
        # Schedule UI updates on the main thread to avoid hangs
        self.loop.call_soon_threadsafe(self._on_node_added_ui, node)

    def _on_node_added_ui(self, node: NMOS_Node):
        """UI update for node added (runs on main thread)"""
        print(f"Node discovered: {node.name} at {node.address}:{node.port}")
        service_type = "Node" if "node" in node.service_type else "Service"
        self.model.add_node(
            node_id=node.node_id,
            node=node.name,
            subtitle=f"{node.address}:{node.port} ({service_type}, {node.api_ver})",
        )

        # Add devices from the NMOS_Node to the UI_NMOS_Node
        if node.node_id in self.model.node_map:
            ui_node = self.model.node_map[node.node_id]

            # Add devices from the registered node
            for nmos_device in node.devices:
                # Create UI device
                ui_device = UI_NMOS_Device(device_id=nmos_device.device_id, parent=ui_node, label=nmos_device.label, description=nmos_device.description)
                ui_node.add_device(ui_device)
                print(f"  Added device: {nmos_device.device_id} to node {node.name}")

                # Note: Senders and receivers are registered separately via the registration API
                # They will be added when sender/receiver registration messages arrive

        self.listbox.refresh()
        self.draw_routing_matrix()
        self.update_error_status()

    def add_sender_to_device(self, node_id: str, device_id: str, sender_id: str):
        """Add a sender to a device on a node"""
        if node_id in self.model.node_map:
            ui_node = self.model.node_map[node_id]
            # Find the device
            ui_device = None
            for dev in ui_node.devices:
                if dev.device_id == device_id:
                    ui_device = dev
                    break

            if ui_device:
                # Check if sender already exists
                for existing_sender in ui_device.senders:
                    if existing_sender.sender_id == sender_id:
                        if '--debug-registry' in sys.argv:
                            print(f"  Sender {sender_id} already exists in device {device_id}, ignoring duplicate")
                        return

                # Create and add sender
                sender = UI_NMOS_Sender(sender_id=sender_id, device=ui_device)
                ui_device.senders.append(sender)
                ui_node.add_sender(sender)
                print(f"  Added sender {sender_id} to device {device_id} on node {node_id}")
                self.loop.call_soon_threadsafe(self.draw_routing_matrix)

    def add_receiver_to_device(self, node_id: str, device_id: str, receiver_id: str):
        """Add a receiver to a device on a node"""
        if node_id in self.model.node_map:
            ui_node = self.model.node_map[node_id]
            # Find the device
            ui_device = None
            for dev in ui_node.devices:
                if dev.device_id == device_id:
                    ui_device = dev
                    break

            if ui_device:
                # Check if receiver already exists
                for existing_receiver in ui_device.receivers:
                    if existing_receiver.receiver_id == receiver_id:
                        if '--debug-registry' in sys.argv:
                            print(f"  Receiver {receiver_id} already exists in device {device_id}, ignoring duplicate")
                        return

                # Create and add receiver
                receiver = UI_NMOS_Receiver(receiver_id=receiver_id, device=ui_device)
                ui_device.receivers.append(receiver)
                ui_node.add_receiver(receiver)
                print(f"  Added receiver {receiver_id} to device {device_id} on node {node_id}")
                self.loop.call_soon_threadsafe(self.draw_routing_matrix)

    def on_node_removed(self, node: NMOS_Node):
        """Callback when an NMOS node is removed from the network"""
        # Schedule UI updates on the main thread to avoid hangs
        self.loop.call_soon_threadsafe(self._on_node_removed_ui, node)

    def _on_node_removed_ui(self, node: NMOS_Node):
        """UI update for node removed (runs on main thread)"""
        print(f"node removed: {node.name}")
        self.model.remove_node(node.node_id)
        self.listbox.refresh()
        self.draw_routing_matrix()

    def on_device_added(self, node_id: str, device_id: str):
        """Callback when a device is registered to a node"""
        self.loop.call_soon_threadsafe(self._on_device_added_ui, node_id, device_id)

    def _on_device_added_ui(self, node_id: str, device_id: str):
        """UI update for device added (runs on main thread)"""
        print(f"Device added: {device_id} to node {node_id}")
        if node_id in self.model.node_map:
            ui_node = self.model.node_map[node_id]

            if self.registry == None:
                ui_device = UI_NMOS_Device(device_id=device_id, parent=ui_node)
            else:
                # Get the NMOS_Device to access its label, description, and channels
                nmos_node = self.registry.nodes.get(node_id)
                ui_device = UI_NMOS_Device(device_id=device_id, parent=ui_node)
                
                if nmos_node:
                    for dev in nmos_node.devices:
                        if dev.device_id == device_id:
                            # Update label and description from NMOS_Device
                            ui_device.label = dev.label
                            ui_device.description = dev.description
                            ui_device.channels = []

                            print(f" examine device channels: {dev.is08_input_channels}, {dev.is08_output_channels}")

                            for d in dev.is08_input_channels:
                                for c in d.channels:
                                    ui_device.channels.append(UI_NMOS_Channel(type=ChannelType.INPUT, id=c.id, name=c.label))
                            for d in dev.is08_output_channels:
                                for c in d.channels:
                                    ui_device.channels.append(UI_NMOS_Channel(type=ChannelType.OUTPUT, id=c.id, name=c.label))
                            break

            ui_node.add_device(ui_device)
            self.draw_routing_matrix()


    def on_sender_added(self, node_id: str, device_id: str, sender_id: str):
        """Callback when a sender is registered to a device"""
        self.loop.call_soon_threadsafe(self.add_sender_to_device, node_id, device_id, sender_id)

    def on_receiver_added(self, node_id: str, device_id: str, receiver_id: str):
        """Callback when a receiver is registered to a device"""
        self.loop.call_soon_threadsafe(self.add_receiver_to_device, node_id, device_id, receiver_id)

    def on_channel_updated(self, node_id: str, device_id: str):
        """Callback when device channels are updated"""
        self.loop.call_soon_threadsafe(self._on_channel_updated_ui, node_id, device_id)

    def _on_channel_updated_ui(self, node_id: str, device_id: str):
        """UI update for channel changes (runs on main thread)"""
        print(f"Channels updated for device {device_id} on node {node_id}")
        if node_id in self.model.node_map:
            ui_node = self.model.node_map[node_id]
            
            # Find the UI device
            ui_device = None
            for dev in ui_node.devices:
                if dev.device_id == device_id:
                    ui_device = dev
                    break
            
            if ui_device and self.registry:
                # Get the NMOS_Device to access updated channels
                nmos_node = self.registry.nodes.get(node_id)
                if nmos_node:
                    for nmos_device in nmos_node.devices:
                        if nmos_device.device_id == device_id:
                            # Update UI device channels
                            ui_device.channels = []
                            
                            for d in nmos_device.is08_input_channels:
                                for c in d.channels:
                                    ui_device.channels.append(UI_NMOS_Channel(type=ChannelType.INPUT, id=c.id, name=c.label))
                            for d in nmos_device.is08_output_channels:
                                for c in d.channels:
                                    ui_device.channels.append(UI_NMOS_Channel(type=ChannelType.OUTPUT, id=c.id, name=c.label))
                            
                            # Update senders and receivers with new channel information
                            for sender in ui_device.senders:
                                sender.channels = [c for c in ui_device.channels if c.type == ChannelType.OUTPUT]
                            for receiver in ui_device.receivers:
                                receiver.channels = [c for c in ui_device.channels if c.type == ChannelType.INPUT]
                            
                            print(f"  Updated {len(ui_device.channels)} channels for device {device_id}")
                            break
                
                # Redraw the routing matrix with updated channels
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
        print("NMOS Registry started - discovering nodes via MDNS...")

    def on_resize(self, widget, width, height, **kwargs):
        # On resize, recalculate margins to center the matrix
        if widget.context:
            # Calculate matrix dimensions

            senders = self.get_senders()
            receivers = self.get_receivers()

            matrix_width = len(senders) * self.cell_width
            matrix_height = len(receivers) * self.cell_height

            # Center the matrix horizontally and vertically
            label_space_left = 150  # Space for receiver labels on the left
            label_space_top = 100   # Space for sender labels on top

            available_width = width - label_space_left
            available_height = height - label_space_top

            self.margin_left = label_space_left + max(0, (available_width - matrix_width) / 2)
            self.margin_top = label_space_top + max(0, (available_height - matrix_height) / 2)

            self.draw_routing_matrix()

    async def on_press(self, widget, x, y, **kwargs):
        """When clicking on the routing matrix at (x, y), toggle the channel mapping between output and input channels.
        Uses IS-08 channel mapping API to update the routing.
        """
        # Get all nodes (same logic as draw_routing_matrix)
        nodes = list(self.model.node_map.values())

        # Use all nodes as both potential senders and receivers (hierarchical tuples)
        senders = self.get_senders()
        receivers = self.get_receivers()

        if not senders or not receivers:
            # No nodes to route
            return

        matrix_width = len(senders) * self.cell_width
        matrix_height = len(receivers) * self.cell_height

        # Check if click is within the matrix bounds
        if x < self.margin_left or x > self.margin_left + matrix_width:
            return
        if y < self.margin_top or y > self.margin_top + matrix_height:
            return

        # Calculate which cell was clicked
        sender_idx = int((x - self.margin_left) / self.cell_width)
        receiver_idx = int((y - self.margin_top) / self.cell_height)

        # Validate indices
        if sender_idx < 0 or sender_idx >= len(senders):
            return
        if receiver_idx < 0 or receiver_idx >= len(receivers):
            return

        s_conn = senders[sender_idx]
        r_conn = receivers[receiver_idx]

        # Handle channel-level mapping if both have channels
        if s_conn.channel and r_conn.channel:
            await self._toggle_channel_mapping(s_conn, r_conn)
        else:
            # Fallback to device-level connection for devices without channels
            sender = s_conn.endpoint
            receiver = r_conn.endpoint

            # Toggle the routing connection
            if sender in receiver.connected_senders:
                # Disconnect
                receiver.connected_senders.remove(sender)
                sender.connected_receivers.remove(receiver)
                await self.disconnect_nodes(sender, receiver)
            else:
                # Connect
                receiver.connected_senders.append(sender)
                sender.connected_receivers.append(receiver)
                print(f"Connected: {s_conn.node.list_entry.get('title', 'Node')} / {s_conn.device.device_id} / {sender.sender_id} -> {r_conn.node.list_entry.get('title', 'Node')} / {r_conn.device.device_id} / {receiver.receiver_id}")
                await self.connect_nodes(sender, receiver)

        # Redraw the matrix to show the change
        self.draw_routing_matrix()

    async def _toggle_channel_mapping(self, s_conn: UI_Matrix_Connection, r_conn: UI_Matrix_Connection):
        """Toggle the mapping between an output channel and an input channel."""
        if not self.registry:
            return

        # Get the NMOS node and device for the sender (output)
        sender_node = self.registry.nodes.get(s_conn.node.node_id)
        if not sender_node:
            return

        # Find the NMOS device and output channel
        target_output_chan = None
        target_output_dev = None
        target_nmos_device = None
        
        for nmos_device in sender_node.devices:
            if nmos_device.device_id != s_conn.device.device_id:
                continue
                
            # Find the specific output channel - prefer ID match, then label
            for output_dev in nmos_device.is08_output_channels:
                for output_chan in output_dev.channels:
                    # Prefer exact ID match if both have IDs
                    if s_conn.channel.id and output_chan.id:
                        if output_chan.id == s_conn.channel.id:
                            target_output_chan = output_chan
                            target_output_dev = output_dev
                            target_nmos_device = nmos_device
                            break
                    # Fallback to label match only if no ID available
                    elif output_chan.label == s_conn.channel.name:
                        target_output_chan = output_chan
                        target_output_dev = output_dev
                        target_nmos_device = nmos_device
                        break
                if target_output_chan:
                    break
            if target_output_chan:
                break
        
        if not target_output_chan:
            print(f"Warning: Could not find output channel {s_conn.channel.name} (ID: {s_conn.channel.id})")
            return
        
        # Check if currently mapped to this specific input
        is_currently_mapped = False
        if target_output_chan.mapped_device:
            # Prefer ID match if both have IDs
            if r_conn.channel.id and target_output_chan.mapped_device.id:
                is_currently_mapped = (target_output_chan.mapped_device.id == r_conn.channel.id)
            # Fallback to label match
            elif target_output_chan.mapped_device.label == r_conn.channel.name:
                is_currently_mapped = True
        
        if is_currently_mapped:
            # Disconnect: clear the mapping for this specific channel only
            target_output_chan.mapped_device = None
            target_output_chan.mapped_channel = None
            print(f"Disconnected channel: {s_conn.node.list_entry.get('title', 'Node')} / {target_output_chan.label} -> {r_conn.node.list_entry.get('title', 'Node')} / {r_conn.channel.name}")
            
            # Update via IS-08 API
            await self.registry.disconnect_channel_mapping(
                sender_node,
                target_nmos_device,
                target_output_dev,
                target_output_chan
            )
        else:
            # Connect: set the mapping to this input channel
            receiver_node = self.registry.nodes.get(r_conn.node.node_id)
            if not receiver_node:
                print(f"Warning: Could not find receiver node {r_conn.node.node_id}")
                return
            
            target_input_chan = None
            target_input_dev = None
            target_recv_device = None
            
            for recv_nmos_device in receiver_node.devices:
                if recv_nmos_device.device_id != r_conn.device.device_id:
                    continue
                    
                for input_dev in recv_nmos_device.is08_input_channels:
                    for input_chan in input_dev.channels:
                        # Prefer exact ID match if both have IDs
                        if r_conn.channel.id and input_chan.id:
                            if input_chan.id == r_conn.channel.id:
                                target_input_chan = input_chan
                                target_input_dev = input_dev
                                target_recv_device = recv_nmos_device
                                break
                        # Fallback to label match only if no ID available
                        elif input_chan.label == r_conn.channel.name:
                            target_input_chan = input_chan
                            target_input_dev = input_dev
                            target_recv_device = recv_nmos_device
                            break
                    if target_input_chan:
                        break
                if target_input_chan:
                    break
            
            if not target_input_chan:
                print(f"Warning: Could not find input channel {r_conn.channel.name} (ID: {r_conn.channel.id})")
                return
            
            # Update the mapping for this specific channel only
            target_output_chan.mapped_device = target_input_chan
            print(f"Connected channel: {s_conn.node.list_entry.get('title', 'Node')} / {target_output_chan.label} -> {r_conn.node.list_entry.get('title', 'Node')} / {target_input_chan.label}")
            
            # Update via IS-08 API
            await self.registry.connect_channel_mapping(
                sender_node,
                target_nmos_device,
                target_output_dev,
                target_output_chan,
                receiver_node,
                target_recv_device,
                target_input_dev,
                target_input_chan
            )

    async def connect_nodes(self, sender: UI_NMOS_Sender, receiver: UI_NMOS_Receiver):
        """Use IS-05 API to connect sender to receiver by handing over the transport file from sender to receiver."""
        print(f"Connecting nodes via IS-05: {sender.sender_id} -> {receiver.receiver_id}")
        if self.registry:
            await self.registry.connect_sender_to_receiver(sender.sender_id, receiver.receiver_id)

    async def disconnect_nodes(self, sender: UI_NMOS_Sender, receiver: UI_NMOS_Receiver):
        """Use IS-05 API to disconnect sender from receiver."""
        print(f"Disconnecting nodes via IS-05: {sender.sender_id} -> {receiver.receiver_id}")
        if self.registry:
            await self.registry.disconnect_sender_from_receiver(sender.sender_id, receiver.receiver_id)

    async def on_exit(self):
        """Cleanup when the app is closing"""
        print("Shutting down NMOS registry...")
        if self.registry:
            await self.registry.stop()
        return True


def main():
    return LocalNMOS("LocalNMOS", "com.sh.localNMOS")