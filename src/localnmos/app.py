"""
Local NMOS
"""

import asyncio
import toga
from toga.style.pack import Pack, ROW, COLUMN
from toga.app import AppStartupMethod, OnExitHandler, OnRunningHandler
from toga.constants import Baseline
from toga.fonts import SANS_SERIF
from toga.colors import WHITE, rgb
from toga.sources import ListSource

from .registry import NMOSRegistry, NMOSDevice


class UI_NMOS_Node:
    """represents an NMOS Node"""

    def __init__(self, device_id: str, list_entry: dict) -> None:
        self.device_id = device_id
        self.list_entry = list_entry
        self.senders: list[UI_NMOS_Node] = []
        self.receivers: list[UI_NMOS_Node] = []

    def add_sender(self, node: "UI_NMOS_Node"):
        self.senders.append(node)

    def add_receiver(self, node: "UI_NMOS_Node"):
        self.receivers.append(node)

    def remove_sender(self, node: "UI_NMOS_Node"):
        if node in self.senders:
            self.senders.remove(node)

    def remove_receiver(self, node: "UI_NMOS_Node"):
        if node in self.receivers:
            self.receivers.remove(node)


class UIModel:
    def __init__(self) -> None:
        self.devices = ListSource(accessors=["title", "subtitle", "icon"], data=[])
        self.device_map: dict[str, UI_NMOS_Node] = {}  # Map device_id to NMOSDevice

    def add_device(
        self,
        device_id: str,
        dev: str,
        subtitle: str,
        icon: toga.Icon = toga.Icon.DEFAULT_ICON,
    ):
        entry = {"title": dev, "subtitle": subtitle, "icon": icon}
        self.devices.append(entry)
        if device_id:
            self.device_map[device_id] = UI_NMOS_Node(
                device_id=device_id, list_entry=entry
            )

    def remove_device(self, device_id: str):
        if device_id in self.device_map:
            node = self.device_map.pop(device_id)
            # Find and remove from ListSource
            for i, item in enumerate(self.devices):
                if item == node.list_entry:
                    self.devices.remove(item)
                    break


class LocalNMOS(toga.App):
    def draw_routing_matrix(self):
        """draw the routing matrix with on the canvas. On the horizonal axis we have the transmitting devices,
        on the vertical axis we have the receiving devices. A checkmark is drawn on the intersections where routing is active.
        """
        if not hasattr(self, "canvas") or not self.canvas:
            return

        # Clear canvas
        self.canvas.context.clear()

        # Get all devices with senders/receivers
        nodes = list(self.model.device_map.values())

        # Collect all unique senders and receivers
        all_senders = []
        all_receivers = []
        for node in nodes:
            all_senders.extend(node.senders)
            all_receivers.extend(node.receivers)

        # Remove duplicates while preserving order
        senders = []
        receivers = []
        seen_senders = set()
        seen_receivers = set()

        for sender in all_senders:
            if sender.device_id not in seen_senders:
                senders.append(sender)
                seen_senders.add(sender.device_id)

        for receiver in all_receivers:
            if receiver.device_id not in seen_receivers:
                receivers.append(receiver)
                seen_receivers.add(receiver.device_id)

        # Matrix dimensions and positioning
        margin_left = 150
        margin_top = 80
        cell_width = 80
        cell_height = 40

        matrix_width = len(senders) * cell_width
        matrix_height = len(receivers) * cell_height

        # Draw matrix grid
        with self.canvas.context.Stroke(
            line_width=1, color=rgb(200, 200, 200)
        ) as stroke:
            # Horizontal lines
            for i in range(len(receivers) + 1):
                y = margin_top + i * cell_height
                stroke.move_to(margin_left, y)
                stroke.line_to(margin_left + matrix_width, y)

            # Vertical lines
            for i in range(len(senders) + 1):
                x = margin_left + i * cell_width
                stroke.move_to(x, margin_top)
                stroke.line_to(x, margin_top + matrix_height)

        # Draw border
        with self.canvas.context.Stroke(
            line_width=2, color=rgb(100, 100, 100)
        ) as stroke:
            stroke.rect(margin_left, margin_top, matrix_width, matrix_height)

        # Draw sender labels (horizontal, rotated)
        font = toga.Font(family=SANS_SERIF, size=12)
        for i, sender in enumerate(senders):
            x = margin_left + i * cell_width + cell_width / 2
            y = margin_top - 10

            self.canvas.context.save()
            self.canvas.context.translate(x, y)
            self.canvas.context.rotate(-0.5)  # Rotate ~28 degrees

            with self.canvas.context.Fill(color=rgb(60, 120, 180)) as fill:
                label = sender.list_entry.get("title", f"Sender {i}")
                fill.write_text(label[:15], 0, 0, font, Baseline.BOTTOM)

            self.canvas.context.restore()

        # Draw receiver labels (vertical)
        for i, receiver in enumerate(receivers):
            y = margin_top + i * cell_height + cell_height / 2

            with self.canvas.context.Fill(color=rgb(180, 120, 60)) as fill:
                label = receiver.list_entry.get("title", f"Receiver {i}")
                fill.write_text(label[:20], margin_left - 140, y, font, Baseline.MIDDLE)

        # Draw routing connections (checkmarks)
        for receiver_idx, receiver in enumerate(receivers):
            for sender in receiver.senders:
                # Find sender index
                try:
                    sender_idx = senders.index(sender)
                except ValueError:
                    continue

                # Calculate cell center
                x = margin_left + sender_idx * cell_width + cell_width / 2
                y = margin_top + receiver_idx * cell_height + cell_height / 2

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
        self.model = UIModel()
        self.registry = None  # Will be initialized in on_running

        main_box = toga.Box(direction=ROW)

        devices_box = toga.Box(direction=COLUMN)
        self.listbox = toga.DetailedList(data=self.model.devices, style=Pack(flex=1))
        devices_box.add(toga.Label("NMOS Devices (MDNS Discovery)"))
        devices_box.add(self.listbox)

        self.canvas = toga.Canvas(
            flex=1, on_resize=self.on_resize, on_press=self.on_press
        )

        self.draw_routing_matrix()

        main_box.add(devices_box)
        main_box.add(toga.Divider())
        main_box.add(self.canvas)

        self.main_window = toga.MainWindow(title=self.formal_name)
        self.main_window.content = main_box
        self.main_window.show()

        self.x = 0
        self.n = 100
        self.loop.call_soon_threadsafe(self.sync_task, "Hi")

    def on_device_added(self, device: NMOSDevice):
        """Callback when an NMOS device is discovered"""
        print(f"Device discovered: {device.name} at {device.address}:{device.port}")
        service_type = "Node" if "node" in device.service_type else "Service"
        self.model.add_device(
            device_id=device.device_id,
            dev=device.name,
            subtitle=f"{device.address}:{device.port} ({service_type}, {device.api_ver})",
        )
        self.listbox.refresh()
        self.draw_routing_matrix()

    def on_device_removed(self, device: NMOSDevice):
        """Callback when an NMOS device is removed from the network"""
        print(f"Device removed: {device.name}")
        self.model.remove_device(device.device_id)
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
            device_added_callback=self.on_device_added,
            device_removed_callback=self.on_device_removed,
        )
        await self.registry.start()
        print("NMOS Registry started - discovering devices via MDNS...")

    def on_resize(self, widget, width, height, **kwargs):
        # On resize, redraw the routing matrix
        if widget.context:
            self.draw_routing_matrix()

    async def on_press(self, widget, x, y, **kwargs):
        """When clicking on the routing matrix at (x, y), we need to compute the intersection of senders and receivers
        to find out which NMOSDevice sender and NMOSDevice receiver it was. We then use is-05 to connect the sender and receiver by handing the transport file from the sender to the receiver.
        """
        # Get all devices with senders/receivers
        nodes = list(self.model.device_map.values())
        
        # Collect all unique senders and receivers (same logic as draw_routing_matrix)
        all_senders = []
        all_receivers = []
        for node in nodes:
            all_senders.extend(node.senders)
            all_receivers.extend(node.receivers)
        
        # Remove duplicates while preserving order
        senders = []
        receivers = []
        seen_senders = set()
        seen_receivers = set()
        
        for sender in all_senders:
            if sender.device_id not in seen_senders:
                senders.append(sender)
                seen_senders.add(sender.device_id)
        
        for receiver in all_receivers:
            if receiver.device_id not in seen_receivers:
                receivers.append(receiver)
                seen_receivers.add(receiver.device_id)
        
        if not senders or not receivers:
            # No devices to route
            return
        
        # Matrix dimensions (must match draw_routing_matrix)
        margin_left = 150
        margin_top = 80
        cell_width = 80
        cell_height = 40
        
        matrix_width = len(senders) * cell_width
        matrix_height = len(receivers) * cell_height
        
        # Check if click is within the matrix bounds
        if x < margin_left or x > margin_left + matrix_width:
            return
        if y < margin_top or y > margin_top + matrix_height:
            return
        
        # Calculate which cell was clicked
        sender_idx = int((x - margin_left) / cell_width)
        receiver_idx = int((y - margin_top) / cell_height)
        
        # Validate indices
        if sender_idx < 0 or sender_idx >= len(senders):
            return
        if receiver_idx < 0 or receiver_idx >= len(receivers):
            return
        
        sender = senders[sender_idx]
        receiver = receivers[receiver_idx]
        
        # Toggle the routing connection
        if sender in receiver.senders:
            # Disconnect
            receiver.remove_sender(sender)
            sender.remove_receiver(receiver)
            print(f"Disconnected: {sender.list_entry.get('title', 'Sender')} -> {receiver.list_entry.get('title', 'Receiver')}")
            
            # TODO: Use IS-05 API to disconnect the actual devices
            # await self.disconnect_devices(sender, receiver)
        else:
            # Connect
            receiver.add_sender(sender)
            sender.add_receiver(receiver)
            print(f"Connected: {sender.list_entry.get('title', 'Sender')} -> {receiver.list_entry.get('title', 'Receiver')}")
            
            # TODO: Use IS-05 API to connect the actual devices
            # await self.connect_devices(sender, receiver)
        
        # Redraw the matrix to show the change
        self.draw_routing_matrix()

    async def on_exit(self):
        """Cleanup when the app is closing"""
        print("Shutting down NMOS registry...")
        if self.registry:
            await self.registry.stop()
        return True


def main():
    return LocalNMOS()
