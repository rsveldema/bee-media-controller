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




class Model:
    def __init__(self) -> None:
        self.devices = ListSource(
            accessors=["title", "subtitle", "icon"],
            data=[]
        )
        self.device_map = {}  # Map device_id to NMOSDevice

    def add_device(self, dev: str, subtitle: str, icon: toga.Icon = toga.Icon.DEFAULT_ICON, device_id: str = None):
        entry = {'title': dev, 'subtitle': subtitle, 'icon': icon}
        self.devices.append(entry)
        if device_id:
            self.device_map[device_id] = entry
    
    def remove_device(self, device_id: str):
        if device_id in self.device_map:
            entry = self.device_map.pop(device_id)
            # Find and remove from ListSource
            for i, item in enumerate(self.devices):
                if item == entry:
                    self.devices.remove(item)
                    break


class LocalNMOS(toga.App):
    def startup(self):
        """Construct and show the Toga application.
        """
        self.model = Model()
        self.registry = None  # Will be initialized in on_running
        
        main_box = toga.Box(direction=ROW)

        devices_box = toga.Box(direction=COLUMN)
        self.listbox = toga.DetailedList(data=self.model.devices,
                                        style=Pack(flex=1))
        devices_box.add(toga.Label("NMOS Devices (MDNS Discovery)"))
        devices_box.add(self.listbox)

        canvas = toga.Canvas(flex=1,
            on_resize=self.on_resize,
            on_press=self.on_press)

        matrix_x = 120
        matrix_y = 80

        matrix_width = 400
        matrix_height = 400

        with canvas.context.Stroke(matrix_x, matrix_y, color="orange") as stroke:
            stroke.line_to(matrix_width, matrix_y)
            stroke.move_to(matrix_x, matrix_y)
            stroke.line_to(matrix_x, matrix_height)

        for i in range(5):
            canvas.context.rotate(0.8)
            canvas.context.translate(70 + 30 * i, -70 + -30 * i)
            with canvas.Fill(color=rgb(149, 119, 73)) as text_filler:
                font = toga.Font(family=SANS_SERIF, size=15)
                t = text_filler.write_text(f"Sender {i}", 0, 0, font, Baseline.TOP)
                self.text = t
            canvas.context.reset_transform()

            with canvas.Fill(color=rgb(149, 119, 73)) as text_filler:
                font = toga.Font(family=SANS_SERIF, size=15)
                t = text_filler.write_text(f"Receiver {i}", 10, matrix_y + i * 30, font, Baseline.TOP)
                self.text = t

        for dev in model.devices:
            


        canvas.redraw()


        main_box.add(devices_box)
        main_box.add(toga.Divider())
        main_box.add(canvas)

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
            dev=device.name,
            subtitle=f"{device.address}:{device.port} ({service_type}, {device.api_ver})",
            device_id=device.device_id
        )
        self.listbox.refresh()
    
    def on_device_removed(self, device: NMOSDevice):
        """Callback when an NMOS device is removed from the network"""
        print(f"Device removed: {device.name}")
        self.model.remove_device(device.device_id)
        self.listbox.refresh()

    def sync_task(self, arg:str):
        print(f"running sync task: {arg}")
        asyncio.create_task(self.async_task("from sync task"))

    async def async_task(self, arg):
        print(f"running async task: {arg}")

    async def on_running(self):
        print(f"on_running")
        
        # Initialize NMOS registry with MDNS discovery
        self.registry = NMOSRegistry(
            device_added_callback=self.on_device_added,
            device_removed_callback=self.on_device_removed
        )
        await self.registry.start()
        print("NMOS Registry started - discovering devices via MDNS...")
        
        for i in range(1, self.n + 1):
            print(f"Counter {self.x}: {i}")
            await asyncio.sleep(0.5)
            if i > 10:
                pass #self.model.add_device(f"device-dyn {i}")
        return f"Finished {self.x} in {self.n}"

    def on_resize(self, widget, width, height, **kwargs):
        # On resize, center the text horizontally on the canvas. on_resize will be
        # called when the canvas is initially created, when the drawing objects won't
        # exist yet. Only attempt to reposition the text if there's context objects on
        # the canvas.
        if widget.context:
            widget.redraw()

    async def on_press(self, widget, x, y, **kwargs):
        pass
        #await self.main_window.dialog(
        #    toga.InfoDialog("Hey!", f"You poked the yak at ({x}, {y})")
        #)
    
    async def on_exit(self):
        """Cleanup when the app is closing"""
        print("Shutting down NMOS registry...")
        if self.registry:
            await self.registry.stop()
        return True

def main():
    return LocalNMOS()
