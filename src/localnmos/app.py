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




class Model:
    def __init__(self) -> None:
        self.devices = ListSource(
            accessors=["title, subtitle, icon"],
            data=[]
        )

    def add_device(self, dev: str, subtitle: str, icon: toga.Icon = toga.Icon.DEFAULT_ICON):
        self.devices.append({'title': dev, 'subtitle': subtitle, 'icon': icon})


class LocalNMOS(toga.App):
    def startup(self):
        """Construct and show the Toga application.
        """
        self.model = Model()
        main_box = toga.Box(direction=ROW)

        devices_box = toga.Box(direction=COLUMN)
        self.listbox = toga.DetailedList(data=self.model.devices,
                                        style=Pack(flex=1))
        devices_box.add(toga.Label("Devices"))
        devices_box.add(self.listbox)

        self.model.add_device("device 1", "192.168.1.123")
        self.listbox.refresh()

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

    def sync_task(self, arg:str):
        print(f"running sync task: {arg}")
        asyncio.create_task(self.async_task("from sync task"))

    async def async_task(self, arg):
        print(f"running async task: {arg}")

    async def on_running(self):
        print(f"on_running")
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

def main():
    return LocalNMOS()
