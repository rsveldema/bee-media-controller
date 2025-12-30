"""
Local NMOS
"""

import asyncio
import socket
from typing import List
from datetime import datetime
import toga
from toga.style.pack import Pack, ROW, COLUMN
from toga.app import AppStartupMethod, OnExitHandler, OnRunningHandler
from toga.constants import Baseline
from toga.fonts import SANS_SERIF
from toga.colors import WHITE, rgb
from toga.sources import ListSource

from .registry import NMOSRegistry
from .nmos import NMOS_Node

# Build date - automatically set when the module is imported
BUILD_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

class UI_NMOS_Device:
    def __init__(self, device_id: str, parent: 'UI_NMOS_Node') -> None:
        self.device_id = device_id
        self.parent = parent
        self.senders: list['UI_NMOS_Sender'] = []
        self.receivers: list['UI_NMOS_Receiver'] = []

class UI_NMOS_Sender:
    def __init__(self, sender_id: str, device: UI_NMOS_Device) -> None:
        self.sender_id = sender_id
        self.device = device
        self.parent = device.parent  # Reference to node for convenience
        self.connected_receivers: list['UI_NMOS_Receiver'] = []

class UI_NMOS_Receiver:
    def __init__(self, receiver_id: str, device: UI_NMOS_Device) -> None:
        self.receiver_id = receiver_id
        self.device = device
        self.parent = device.parent  # Reference to node for convenience
        self.connected_senders: list[UI_NMOS_Sender] = []

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

    def get_senders(self) -> List[tuple[UI_NMOS_Node, UI_NMOS_Sender]]:
        """Get all senders as (node, sender) tuples for hierarchical display"""
        ret = []
        for n in self.model.get_nodes():
            for s in n.senders:
                ret.append((n, s))
        return ret


    def get_receivers(self) -> List[tuple[UI_NMOS_Node, UI_NMOS_Receiver]]:
        """Get all receivers as (node, receiver) tuples for hierarchical display"""
        ret = []
        for n in self.model.get_nodes():
            for r in n.receivers:
                ret.append((n, r))
        return ret

    def draw_routing_matrix(self):
        """draw the routing matrix with hierarchical nodes and devices. On the horizonal axis we have the transmitting devices,
        on the vertical axis we have the receiving devices. A checkmark is drawn on the intersections where routing is active.
        """
        if not hasattr(self, "canvas") or not self.canvas:
            return

        # Clear canvas
        self.canvas.context.clear()

        # Get all discovered nodes, devices, etc- show them all in the matrix
        nodes = list(self.model.node_map.values())

        # Use all nodes as both potential senders and receivers (hierarchical tuples)
        senders = self.get_senders()  # List of (node, sender) tuples
        receivers = self.get_receivers()  # List of (node, receiver) tuples

        matrix_width = len(senders) * self.cell_width
        matrix_height = len(receivers) * self.cell_height

        # Draw matrix grid
        with self.canvas.context.Stroke(
            line_width=1, color=rgb(200, 200, 200)
        ) as stroke:
            # Horizontal lines
            for i in range(len(receivers) + 1):
                y = self.margin_top + i * self.cell_height
                stroke.move_to(self.margin_left, y)
                stroke.line_to(self.margin_left + matrix_width, y)

            # Vertical lines
            for i in range(len(senders) + 1):
                x = self.margin_left + i * self.cell_width
                stroke.move_to(x, self.margin_top)
                stroke.line_to(x, self.margin_top + matrix_height)

        # Draw hierarchical separator lines for senders (vertical, thicker lines between nodes/devices)
        prev_sender_node = None
        prev_sender_device = None
        with self.canvas.context.Stroke(line_width=3, color=rgb(60, 100, 160)) as stroke:
            for i, (sender_node, sender) in enumerate(senders):
                # Draw thick line before new node
                if prev_sender_node is not None and sender_node != prev_sender_node:
                    x = self.margin_left + i * self.cell_width
                    stroke.move_to(x, self.margin_top)
                    stroke.line_to(x, self.margin_top + matrix_height)
                prev_sender_node = sender_node
                
        # Draw medium separator lines for devices
        prev_sender_node = None
        prev_sender_device = None
        with self.canvas.context.Stroke(line_width=2, color=rgb(100, 140, 200)) as stroke:
            for i, (sender_node, sender) in enumerate(senders):
                # Draw medium line before new device (same node)
                if (prev_sender_device is not None and sender.device != prev_sender_device 
                    and sender_node == prev_sender_node):
                    x = self.margin_left + i * self.cell_width
                    stroke.move_to(x, self.margin_top)
                    stroke.line_to(x, self.margin_top + matrix_height)
                prev_sender_node = sender_node
                prev_sender_device = sender.device

        # Draw hierarchical separator lines for receivers (horizontal, thicker lines between nodes/devices)
        prev_receiver_node = None
        prev_receiver_device = None
        with self.canvas.context.Stroke(line_width=3, color=rgb(160, 100, 60)) as stroke:
            for i, (receiver_node, receiver) in enumerate(receivers):
                # Draw thick line before new node
                if prev_receiver_node is not None and receiver_node != prev_receiver_node:
                    y = self.margin_top + i * self.cell_height
                    stroke.move_to(self.margin_left, y)
                    stroke.line_to(self.margin_left + matrix_width, y)
                prev_receiver_node = receiver_node
                
        # Draw medium separator lines for devices
        prev_receiver_node = None
        prev_receiver_device = None
        with self.canvas.context.Stroke(line_width=2, color=rgb(200, 140, 100)) as stroke:
            for i, (receiver_node, receiver) in enumerate(receivers):
                # Draw medium line before new device (same node)
                if (prev_receiver_device is not None and receiver.device != prev_receiver_device 
                    and receiver_node == prev_receiver_node):
                    y = self.margin_top + i * self.cell_height
                    stroke.move_to(self.margin_left, y)
                    stroke.line_to(self.margin_left + matrix_width, y)
                prev_receiver_node = receiver_node
                prev_receiver_device = receiver.device

        # Draw border
        with self.canvas.context.Stroke(
            line_width=2, color=rgb(100, 100, 100)
        ) as stroke:
            stroke.rect(self.margin_left, self.margin_top, matrix_width, matrix_height)

        # Draw axis labels
        font_label = toga.Font(family=SANS_SERIF, size=14, weight="bold")
        
        # Draw "SENDERS" label at the top center of the matrix (well above the rotated channel names)
        senders_label = "SENDERS"
        senders_label_size = self.canvas.measure_text(senders_label, font_label)
        senders_x = self.margin_left + (matrix_width - senders_label_size[0]) / 2
        senders_y = 10  # Position at the very top of the canvas
        with self.canvas.context.Fill(color=rgb(40, 80, 140)) as fill:
            fill.write_text(senders_label, senders_x, senders_y, font_label, Baseline.TOP)
        
        # Draw "RECEIVERS" label on the left side (rotated 90 degrees)
        receivers_label = "RECEIVERS"
        receivers_label_x_offset = 20  # Position close to the left edge
        receivers_x = receivers_label_x_offset
        receivers_y = self.margin_top + matrix_height / 2
        
        # Apply transformations: translate then rotate
        self.canvas.context.translate(receivers_x, receivers_y)
        self.canvas.context.rotate(-1.5708)  # Rotate -90 degrees (in radians)
        
        receivers_label_size = self.canvas.measure_text(receivers_label, font_label)
        with self.canvas.context.Fill(color=rgb(140, 80, 40)) as fill:
            fill.write_text(receivers_label, -receivers_label_size[0] / 2, 0, font_label, Baseline.MIDDLE)
        
        # Undo transformations
        self.canvas.context.rotate(1.5708)  # Rotate back +90 degrees
        self.canvas.context.translate(-receivers_x, -receivers_y)

        # Draw sender labels (horizontal, rotated) - show node/device/sender hierarchy
        font = toga.Font(family=SANS_SERIF, size=10)
        font_small = toga.Font(family=SANS_SERIF, size=8)
        for i, (sender_node, sender) in enumerate(senders):
            x = self.margin_left + i * self.cell_width + self.cell_width / 2
            y = self.margin_top - 10

            # Apply transformations: translate then rotate
            self.canvas.context.translate(x, y)
            self.canvas.context.rotate(-0.785)  # Rotate -45 degrees (in radians)

            # Draw node name in bold color
            node_label = sender_node.list_entry.get("title", f"Node {i}")[:10]
            with self.canvas.context.Fill(color=rgb(40, 80, 140)) as fill:
                fill.write_text(node_label, 0, 0, font, Baseline.BOTTOM)

            # Draw device and sender ID below node name in lighter colors
            device_label = f" >{sender.device.device_id[:6]}"
            sender_label = f":{sender.sender_id[:6]}"
            with self.canvas.context.Fill(color=rgb(100, 140, 200)) as fill:
                fill.write_text(device_label, 0, 12, font_small, Baseline.BOTTOM)
            with self.canvas.context.Fill(color=rgb(120, 160, 220)) as fill:
                fill.write_text(sender_label, 0, 20, font_small, Baseline.BOTTOM)

            # Undo transformations: rotate back then translate back
            self.canvas.context.rotate(0.785)  # Rotate back +45 degrees
            self.canvas.context.translate(-x, -y)

        # Draw receiver labels (vertical) - positioned to the right of "RECEIVERS", show hierarchy
        # Calculate starting position for receiver labels (after "RECEIVERS" text)
        receiver_label_start_x = receivers_label_x_offset + 35  # Leave space after "RECEIVERS"
        
        for i, (receiver_node, receiver) in enumerate(receivers):
            y = self.margin_top + i * self.cell_height + self.cell_height / 2

            # Show node, device, and receiver hierarchically
            node_label = receiver_node.list_entry.get("title", f"Node {i}")[:12]
            device_label = f" >{receiver.device.device_id[:6]}"
            receiver_label = f":{receiver.receiver_id[:6]}"
            full_label = f"{node_label}{device_label}{receiver_label}"

            # Draw node name in bold color (left-aligned from receiver_label_start_x)
            with self.canvas.context.Fill(color=rgb(140, 80, 40)) as fill:
                node_size = self.canvas.measure_text(node_label, font)
                fill.write_text(node_label, receiver_label_start_x, y - 4, font, Baseline.MIDDLE)

            # Draw device label in lighter color
            with self.canvas.context.Fill(color=rgb(200, 140, 100)) as fill:
                device_size = self.canvas.measure_text(device_label, font_small)
                fill.write_text(device_label, receiver_label_start_x + node_size[0], y - 4, font_small, Baseline.MIDDLE)

            # Draw receiver label in lightest color
            with self.canvas.context.Fill(color=rgb(220, 160, 120)) as fill:
                fill.write_text(receiver_label, receiver_label_start_x + node_size[0] + device_size[0], y - 4, font_small, Baseline.MIDDLE)

        # Draw routing connections (checkmarks)
        # Connections are tracked between sender and receiver objects
        for receiver_idx, (receiver_node, receiver) in enumerate(receivers):
            # Check if this receiver has any connections stored
            if hasattr(receiver, 'connected_senders'):
                for connected_sender in receiver.connected_senders:
                    # Find sender index by matching sender object
                    sender_idx = None
                    for idx, (snode, sender) in enumerate(senders):
                        if sender == connected_sender:
                            sender_idx = idx
                            break

                    if sender_idx is None:
                        continue

                    # Calculate cell center
                    x = self.margin_left + sender_idx * self.cell_width + self.cell_width / 2
                    y = self.margin_top + receiver_idx * self.cell_height + self.cell_height / 2

                    # Draw checkmark
                    check_size = 15
                    with self.canvas.context.Stroke(
                        line_width=3, color=rgb(50, 200, 50)
                    ) as stroke:
                        # Checkmark shape
                        stroke.move_to(x - check_size / 2, y)
                        stroke.line_to(x - check_size / 4, y + check_size / 2)
                        stroke.line_to(x + check_size / 2, y - check_size / 2)

        self.canvas.redraw()

    def startup(self):
        """Construct and show the Toga application."""
        self.margin_left = 150
        self.margin_top = 80
        self.cell_width = 40
        self.cell_height = 40

        self.model = UIModel()
        self.registry = None  # Will be initialized in on_running

        main_box = toga.Box(direction=ROW)

        nodes_box = toga.Box(direction=COLUMN, style=Pack(width=300))
        self.listbox = toga.DetailedList(
            data=self.model.nodes,
            on_select=self.on_node_select,
            style=Pack(flex=1)
        )
        nodes_box.add(toga.Label("NMOS Nodes (MDNS Discovery)"))
        nodes_box.add(self.listbox)

        self.canvas = toga.Canvas(
            flex=1, on_resize=self.on_resize, on_press=self.on_press
        )

        self.draw_routing_matrix()

        main_box.add(nodes_box)
        main_box.add(toga.Divider())
        main_box.add(self.canvas)

        self.main_window = toga.MainWindow(title=self.formal_name)
        self.main_window.content = main_box
        self.main_window.show()

        self.loop.call_soon_threadsafe(self.sync_task, "Hi")

    def on_node_select(self, widget):
        """Handler for when a device is selected in the list"""
        if widget.selection:
            node_name = widget.selection.title
            self.main_window.info_dialog(
                "NMOS Node Information",
                f"Device Name: {node_name}\n\n"
                f"Subtitle: {widget.selection.subtitle}"
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
                ui_device = UI_NMOS_Device(device_id=nmos_device.device_id, parent=ui_node)
                ui_node.add_device(ui_device)
                print(f"  Added device: {nmos_device.device_id} to node {node.name}")
                
                # Note: Senders and receivers are registered separately via the registration API
                # They will be added when sender/receiver registration messages arrive
        
        self.listbox.refresh()
        self.draw_routing_matrix()
    
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
            ui_device = UI_NMOS_Device(device_id=device_id, parent=ui_node)
            ui_node.add_device(ui_device)
            self.draw_routing_matrix()
    
    def on_sender_added(self, node_id: str, device_id: str, sender_id: str):
        """Callback when a sender is registered to a device"""
        self.loop.call_soon_threadsafe(self.add_sender_to_device, node_id, device_id, sender_id)
    
    def on_receiver_added(self, node_id: str, device_id: str, receiver_id: str):
        """Callback when a receiver is registered to a device"""
        self.loop.call_soon_threadsafe(self.add_receiver_to_device, node_id, device_id, receiver_id)

    def sync_task(self, arg: str):
        print(f"running sync task: {arg}")
        asyncio.create_task(self.async_task("from sync task"))

    async def async_task(self, arg):
        print(f"running async task: {arg}")

    async def on_running(self):
        print(f"on_running")

        # Initialize NMOS registry with MDNS discovery
        self.registry = NMOSRegistry(
            node_added_callback=self.on_node_added,
            node_removed_callback=self.on_node_removed,
            device_added_callback=self.on_device_added,
            sender_added_callback=self.on_sender_added,
            receiver_added_callback=self.on_receiver_added,
        )
        await self.registry.start()
        print("NMOS Registry started - discovering nodes via MDNS...")

    def on_resize(self, widget, width, height, **kwargs):
        # On resize, recalculate margins to center the matrix
        if widget.context:
            # Calculate matrix dimensions

            senders = self.get_senders()
            receivers = self.get_receivers()

            matrix_width = len(receivers) * self.cell_width
            matrix_height = len(senders) * self.cell_height

            # Center the matrix horizontally and vertically
            label_space_left = 150  # Space for receiver labels on the left
            label_space_top = 100   # Space for sender labels on top

            available_width = width - label_space_left
            available_height = height - label_space_top

            self.margin_left = label_space_left + max(0, (available_width - matrix_width) / 2)
            self.margin_top = label_space_top + max(0, (available_height - matrix_height) / 2)

            self.draw_routing_matrix()

    async def on_press(self, widget, x, y, **kwargs):
        """When clicking on the routing matrix at (x, y), we need to compute the intersection of senders and receivers
        to find out which NMOSNode sender and NMOSNode receiver it was. We then use is-05 to connect the sender and receiver by handing the transport file from the sender to the receiver.
        """
        # Get all nodes (same logic as draw_routing_matrix)
        nodes = list(self.model.node_map.values())

        # Use all nodes as both potential senders and receivers (hierarchical tuples)
        senders = self.get_senders()  # List of (node, sender) tuples
        receivers = self.get_receivers()  # List of (node, receiver) tuples

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

        sender_node, sender = senders[sender_idx]
        receiver_node, receiver = receivers[receiver_idx]

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
            print(f"Connected: {sender_node.list_entry.get('title', 'Node')} / {sender.device.device_id} / {sender.sender_id} -> {receiver_node.list_entry.get('title', 'Node')} / {receiver.device.device_id} / {receiver.receiver_id}")
            await self.connect_nodes(sender, receiver)

        # Redraw the matrix to show the change
        self.draw_routing_matrix()

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
