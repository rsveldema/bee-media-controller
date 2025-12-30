"""
Local NMOS
"""

import asyncio
import socket
from typing import List
import toga
from toga.style.pack import Pack, ROW, COLUMN
from toga.app import AppStartupMethod, OnExitHandler, OnRunningHandler
from toga.constants import Baseline
from toga.fonts import SANS_SERIF
from toga.colors import WHITE, rgb
from toga.sources import ListSource

from .registry import NMOSRegistry, NMOS_Node

class UI_NMOS_Device:
    def __init__(self, device_id: str, parent: 'UI_NMOS_Node') -> None:
        self.device_id = device_id
        self.parent = parent

class UI_NMOS_Node:
    """represents an NMOS Node"""

    def __init__(self, node_id: str, list_entry: dict) -> None:
        self.node_id = node_id
        self.list_entry = list_entry
        self.senders: list[UI_NMOS_Device] = []
        self.receivers: list[UI_NMOS_Device] = []

    def add_sender(self, dev: "UI_NMOS_Device"):
        self.senders.append(dev)

    def add_receiver(self, dev: "UI_NMOS_Device"):
        self.receivers.append(dev)

    def remove_sender(self, dev: "UI_NMOS_Device"):
        if dev in self.senders:
            self.senders.remove(dev)

    def remove_receiver(self, dev: "UI_NMOS_Device"):
        if dev in self.receivers:
            self.receivers.remove(dev)


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
            f"Host IP Address: {ip_address}\n\n"
            f"Local NMOS node discovery and routing matrix."
        )

    def get_senders(self) -> List[UI_NMOS_Device]:
        ret = []
        for n in self.model.get_nodes():
            s = n.senders
            ret.extend(s)
        return ret


    def get_receivers(self) -> List[UI_NMOS_Device]:
        ret = []
        for n in self.model.get_nodes():
            s = n.receivers
            ret.extend(s)
        return ret

    def draw_routing_matrix(self):
        """draw the routing matrix with on the canvas. On the horizonal axis we have the transmitting devices,
        on the vertical axis we have the receiving devices. A checkmark is drawn on the intersections where routing is active.
        """
        if not hasattr(self, "canvas") or not self.canvas:
            return

        # Clear canvas
        self.canvas.context.clear()

        # Get all discovered nodes, devices, etc- show them all in the matrix
        nodes = list(self.model.node_map.values())

        # Use all nodes as both potential senders and receivers
        senders = self.get_senders()
        receivers = self.get_receivers()

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

        # Draw border
        with self.canvas.context.Stroke(
            line_width=2, color=rgb(100, 100, 100)
        ) as stroke:
            stroke.rect(self.margin_left, self.margin_top, matrix_width, matrix_height)

        # Draw sender labels (horizontal, rotated)
        font = toga.Font(family=SANS_SERIF, size=12)
        for i, sender in enumerate(senders):
            x = self.margin_left + i * self.cell_width + self.cell_width / 2
            y = self.margin_top - 10

            # Apply transformations: translate then rotate
            self.canvas.context.translate(x, y)
            self.canvas.context.rotate(-0.785)  # Rotate -45 degrees (in radians)

            with self.canvas.context.Fill(color=rgb(60, 120, 180)) as fill:
                label = sender.parent.list_entry.get("title", f"Sender {i}")
                fill.write_text(label[:15], 0, 0, font, Baseline.BOTTOM)

            # Undo transformations: rotate back then translate back
            self.canvas.context.rotate(0.785)  # Rotate back +45 degrees
            self.canvas.context.translate(-x, -y)

        # Draw receiver labels (vertical) - positioned to the left of the matrix
        for i, receiver in enumerate(receivers):
            y = self.margin_top + i * self.cell_height + self.cell_height / 2

            label = receiver.parent.list_entry.get("title", f"Receiver {i}")[:20]
            # Measure text width for accurate right alignment
            text_size = self.canvas.measure_text(label, font)

            with self.canvas.context.Fill(color=rgb(180, 120, 60)) as fill:
                # Position text so it ends 15 pixels to the left of the matrix
                fill.write_text(label, self.margin_left - text_size[0] - 15, y, font, Baseline.MIDDLE)

        # Draw routing connections (checkmarks)
        for receiver_idx, receiver in enumerate(receivers):
            for sender in receiver.parent.senders:
                # Find sender index
                try:
                    sender_idx = senders.index(sender)
                except ValueError:
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
        self.listbox.refresh()
        self.draw_routing_matrix()

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

        # Use all nodes as both potential senders and receivers
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

        sender = senders[sender_idx]
        receiver = receivers[receiver_idx]

        # Toggle the routing connection
        if sender in receiver.parent.senders:
            # Disconnect
            receiver.parent.remove_sender(sender)
            sender.parent.remove_receiver(receiver)
            await self.disconnect_nodes(sender, receiver)
        else:
            # Connect
            receiver.parent.add_sender(sender)
            sender.parent.add_receiver(receiver)
            print(f"Connected: {sender.device_id} -> {receiver.device_id}")
            await self.connect_nodes(sender, receiver)

        # Redraw the matrix to show the change
        self.draw_routing_matrix()

    async def connect_nodes(self, sender: UI_NMOS_Device, receiver: UI_NMOS_Device):
        """Use IS-05 API to connect sender to receiver by handing over the transport file from sender to receiver."""
        print(f"Connecting nodes via IS-05: {sender.device_id} -> {receiver.device_id}")
        if self.registry:
            await self.registry.connect_sender_to_receiver(sender.device_id, receiver.device_id)

    async def disconnect_nodes(self, sender: UI_NMOS_Device, receiver: UI_NMOS_Device):
        """Use IS-05 API to disconnect sender from receiver."""
        print(f"Disconnecting nodes via IS-05: {sender.device_id} -> {receiver.device_id}")
        if self.registry:
            await self.registry.disconnect_sender_from_receiver(sender.device_id, receiver.device_id)

    async def on_exit(self):
        """Cleanup when the app is closing"""
        print("Shutting down NMOS registry...")
        if self.registry:
            await self.registry.stop()
        return True


def main():
    return LocalNMOS("LocalNMOS", "com.sh.localNMOS")
