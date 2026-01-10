"""
Routing Matrix Canvas Drawing
"""

from typing import List
import toga
from toga.fonts import SANS_SERIF
from toga.constants import Baseline
from toga.colors import rgb


class RoutingMatrixCanvas:
    """Handles all canvas drawing operations for the routing matrix"""
    
    def __init__(self, canvas, registry=None):
        self.canvas = canvas
        self.registry = registry
        
        # Drawing parameters
        self.margin_left = 150
        self.margin_top = 80
        self.base_cell_width = 40
        self.base_cell_height = 40
        self.zoom_factor = 1.0
        
    @property
    def cell_width(self):
        return self.base_cell_width * self.zoom_factor
    
    @property
    def cell_height(self):
        return self.base_cell_height * self.zoom_factor
    
    def set_zoom_factor(self, factor):
        """Set the zoom factor for the canvas"""
        self.zoom_factor = factor
    
    def draw_matrix_grid(self, senders, receivers, matrix_width, matrix_height):
        """Draw the matrix grid lines"""
        # Draw matrix grid
        with self.canvas.context.Stroke(
            line_width=0.5, color=rgb(220, 220, 220)
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

        # Draw hierarchical separator lines for senders (vertical)
        prev_sender_node = None
        prev_sender_device = None
        prev_sender = None
        with self.canvas.context.Stroke(line_width=3, color=rgb(60, 100, 160)) as stroke_node, \
             self.canvas.context.Stroke(line_width=2, color=rgb(100, 140, 200)) as stroke_dev, \
             self.canvas.context.Stroke(line_width=1, color=rgb(150, 180, 220)) as stroke_sender:

            for i, s_conn in enumerate(senders):
                if i > 0:
                    if s_conn.node != prev_sender_node:
                        x = self.margin_left + i * self.cell_width
                        stroke_node.move_to(x, self.margin_top)
                        stroke_node.line_to(x, self.margin_top + matrix_height)
                    elif s_conn.device != prev_sender_device:
                        x = self.margin_left + i * self.cell_width
                        stroke_dev.move_to(x, self.margin_top)
                        stroke_dev.line_to(x, self.margin_top + matrix_height)
                    elif s_conn.endpoint != prev_sender:
                        x = self.margin_left + i * self.cell_width
                        stroke_sender.move_to(x, self.margin_top)
                        stroke_sender.line_to(x, self.margin_top + matrix_height)

                prev_sender_node = s_conn.node
                prev_sender_device = s_conn.device
                prev_sender = s_conn.endpoint

        # Draw hierarchical separator lines for receivers (horizontal)
        prev_receiver_node = None
        prev_receiver_device = None
        prev_receiver = None
        with self.canvas.context.Stroke(line_width=3, color=rgb(160, 100, 60)) as stroke_node, \
             self.canvas.context.Stroke(line_width=2, color=rgb(200, 140, 100)) as stroke_dev, \
             self.canvas.context.Stroke(line_width=1, color=rgb(220, 180, 150)) as stroke_receiver:

            for i, r_conn in enumerate(receivers):
                if i > 0:
                    if r_conn.node != prev_receiver_node:
                        y = self.margin_top + i * self.cell_height
                        stroke_node.move_to(self.margin_left, y)
                        stroke_node.line_to(self.margin_left + matrix_width, y)
                    elif r_conn.device != prev_receiver_device:
                        y = self.margin_top + i * self.cell_height
                        stroke_dev.move_to(self.margin_left, y)
                        stroke_dev.line_to(self.margin_left + matrix_width, y)
                    elif r_conn.endpoint != prev_receiver:
                        y = self.margin_top + i * self.cell_height
                        stroke_receiver.move_to(self.margin_left, y)
                        stroke_receiver.line_to(self.margin_left + matrix_width, y)

                prev_receiver_node = r_conn.node
                prev_receiver_device = r_conn.device
                prev_receiver = r_conn.endpoint

        # Draw border
        with self.canvas.context.Stroke(
            line_width=2, color=rgb(100, 100, 100)
        ) as stroke:
            stroke.rect(self.margin_left, self.margin_top, matrix_width, matrix_height)

    def draw_checkmarks_for_routing_connections(self, senders, receivers, matrix_width, matrix_height):
        """Draw checkmarks for active routing connections"""
        # Draw routing connections (checkmarks) based on channel-level mappings
        for receiver_idx, r_conn in enumerate(receivers):
            for sender_idx, s_conn in enumerate(senders):
                # Check if there's a channel-level connection
                is_connected = False
                
                if s_conn.channel and r_conn.channel and self.registry:
                    # Get the NMOS node and device for the sender
                    sender_node = self.registry.nodes.get(s_conn.node.node_id)
                    if sender_node:
                        for nmos_device in sender_node.devices:
                            if nmos_device.device_id != s_conn.device.device_id:
                                continue
                            
                            # Debug: print what we're looking for
                            print(f"Looking for output channel: {s_conn.channel.name} (ID: {s_conn.channel.id})")
                                
                            # Find the specific output channel in the NMOS device
                            for output_dev in nmos_device.is08_output_channels:
                                print(f"  Checking output device: {output_dev.id} with {len(output_dev.channels)} channels")
                                for output_chan in output_dev.channels:
                                    # Prefer exact ID match if both have IDs (handle trailing slashes)
                                    output_match = False
                                    if s_conn.channel.id and output_chan.id:
                                        output_match = (output_chan.id.rstrip('/') == s_conn.channel.id.rstrip('/'))
                                        print(f"    Channel {output_chan.label} (ID: {output_chan.id}) - ID match: {output_match}")
                                    elif output_chan.label == s_conn.channel.name:
                                        output_match = True
                                        print(f"    Channel {output_chan.label} - Label match: {output_match}")
                                    
                                    if output_match:
                                        print(f"    ✓ Found matching output channel: {output_chan.label}")
                                        # Check if this specific output is mapped to the specific input channel
                                        if output_chan.mapped_device:
                                            print(f"      Mapped to: {output_chan.mapped_device.label} (ID: {output_chan.mapped_device.id})")
                                            print(f"      Looking for input: {r_conn.channel.name} (ID: {r_conn.channel.id})")
                                            # Prefer exact ID match if both have IDs (handle trailing slashes)
                                            if r_conn.channel.id and output_chan.mapped_device.id:
                                                is_connected = (output_chan.mapped_device.id.rstrip('/') == r_conn.channel.id.rstrip('/'))
                                                print(f"      ID comparison: '{output_chan.mapped_device.id.rstrip('/')}' == '{r_conn.channel.id.rstrip('/')}' -> {is_connected}")
                                            elif output_chan.mapped_device.label == r_conn.channel.name:
                                                is_connected = True
                                                print(f"      Label comparison matched: {is_connected}")
                                            
                                            # Debug logging
                                            if is_connected:
                                                print(f"✓✓✓ CHECKMARK SHOULD APPEAR: {output_chan.label} -> {output_chan.mapped_device.label}")
                                        else:
                                            print(f"      ✗ No mapped_device set")
                                        break
                                if is_connected:
                                    break
                            break
                
                # Fallback: if no channels, check sender-to-receiver connection
                if not is_connected and not s_conn.channel and not r_conn.channel:
                    if s_conn.endpoint in r_conn.endpoint.connected_senders:
                        is_connected = True
                
                if is_connected:
                    # Calculate cell center
                    x = self.margin_left + sender_idx * self.cell_width + self.cell_width / 2
                    y = self.margin_top + receiver_idx * self.cell_height + self.cell_height / 2

                    # Draw checkmark
                    check_size = 10
                    with self.canvas.context.Stroke(
                        line_width=2, color=rgb(50, 200, 50)
                    ) as stroke:
                        stroke.move_to(x - check_size / 2, y)
                        stroke.line_to(x - check_size / 4, y + check_size / 2)
                        stroke.line_to(x + check_size / 2, y - check_size / 2)

    def draw_axis_labels(self, senders, receivers, matrix_width, matrix_height, receivers_label_x_offset):
        """Draw axis labels (SENDERS and RECEIVERS)"""
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

    def draw_connection_labels(self, senders, receivers, matrix_width, matrix_height, receivers_label_x_offset):
        """Draw labels for senders and receivers"""
        # Draw sender labels (horizontal, rotated)
        font = toga.Font(family=SANS_SERIF, size=10)
        font_small = toga.Font(family=SANS_SERIF, size=8)
        font_chan = toga.Font(family=SANS_SERIF, size=7)

        prev_sender = None
        for i, s_conn in enumerate(senders):
            x = self.margin_left + i * self.cell_width + self.cell_width / 2
            y = self.margin_top - 10

            # Apply transformations: translate then rotate
            self.canvas.context.translate(x, y)
            self.canvas.context.rotate(-0.785)  # Rotate -45 degrees

            # Draw labels only for the first channel of a sender to avoid clutter
            if s_conn.endpoint != prev_sender:
                # Draw node name
                node_label = s_conn.node.list_entry.get("title", f"Node {i}")[:10]
                with self.canvas.context.Fill(color=rgb(40, 80, 140)) as fill:
                    fill.write_text(node_label, 0, 0, font, Baseline.BOTTOM)

                # Draw device and sender ID
                device_name = s_conn.device.label if s_conn.device.label else s_conn.device.device_id
                device_label = f" >{device_name[:12]}"
                sender_label = f":{s_conn.endpoint.sender_id[:6]}"
                with self.canvas.context.Fill(color=rgb(100, 140, 200)) as fill:
                    fill.write_text(device_label, 0, 12, font_small, Baseline.BOTTOM)
                with self.canvas.context.Fill(color=rgb(120, 160, 220)) as fill:
                    fill.write_text(sender_label, 0, 20, font_small, Baseline.BOTTOM)

            # Draw channel label
            if s_conn.channel:
                channel_label = s_conn.channel.name[:24]
                with self.canvas.context.Fill(color=rgb(180, 200, 240)) as fill:
                    fill.write_text(channel_label, 0, 30, font_chan, Baseline.BOTTOM)

            # Undo transformations
            self.canvas.context.rotate(0.785)
            self.canvas.context.translate(-x, -y)
            prev_sender = s_conn.endpoint

        # Draw receiver labels (horizontal)
        receiver_label_start_x = receivers_label_x_offset + 35
        prev_receiver = None
        for i, r_conn in enumerate(receivers):
            y = self.margin_top + i * self.cell_height + self.cell_height / 2

            # Draw labels only for the first channel of a receiver
            if r_conn.endpoint != prev_receiver:
                node_label = r_conn.node.list_entry.get("title", f"Node {i}")[:12]
                device_name = r_conn.device.label if r_conn.device.label else r_conn.device.device_id
                device_label = f" >{device_name[:12]}"
                receiver_id_label = f":{r_conn.endpoint.receiver_id[:6]}"

                with self.canvas.context.Fill(color=rgb(140, 80, 40)) as fill:
                    node_size = self.canvas.measure_text(node_label, font)
                    fill.write_text(node_label, receiver_label_start_x, y - 4, font, Baseline.MIDDLE)

                with self.canvas.context.Fill(color=rgb(200, 140, 100)) as fill:
                    device_size = self.canvas.measure_text(device_label, font_small)
                    fill.write_text(device_label, receiver_label_start_x + node_size[0], y - 4, font_small, Baseline.MIDDLE)

                with self.canvas.context.Fill(color=rgb(220, 160, 120)) as fill:
                    fill.write_text(receiver_id_label, receiver_label_start_x + node_size[0] + device_size[0], y - 4, font_small, Baseline.MIDDLE)
            
            # Draw channel label (indented for sub-row appearance)
            if r_conn.channel:
                channel_label = r_conn.channel.name[:24]
                # Indent channel labels to show they're sub-items under the receiver
                channel_indent = 20 if r_conn.endpoint == prev_receiver else 0
                with self.canvas.context.Fill(color=rgb(240, 200, 180)) as fill:
                    fill.write_text(channel_label, receiver_label_start_x + channel_indent, y + 8, font_chan, Baseline.MIDDLE)

            prev_receiver = r_conn.endpoint

    def draw_matrix(self, senders, receivers):
        """Main method to draw the complete routing matrix"""
        if not self.canvas:
            return

        # Clear canvas
        self.canvas.context.clear()

        matrix_width = len(senders) * self.cell_width
        matrix_height = len(receivers) * self.cell_height
        receivers_label_x_offset = 20  # Position close to the left edge

        self.draw_matrix_grid(senders, receivers, matrix_width, matrix_height)
        self.draw_axis_labels(senders, receivers, matrix_width, matrix_height, receivers_label_x_offset)
        self.draw_connection_labels(senders, receivers, matrix_width, matrix_height, receivers_label_x_offset)
        self.draw_checkmarks_for_routing_connections(senders, receivers, matrix_width, matrix_height)

        self.canvas.redraw()
