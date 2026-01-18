"""
Routing Matrix Canvas Drawing
"""

from typing import List
import toga
from toga.constants import Baseline
from toga.colors import rgb
from toga.style.pack import Pack

from localnmos import nmos

from localnmos.ui_model import UI_NMOS_ConnectionMatrix

class RoutingMatrixCanvas:
    """Handles all canvas drawing operations for the routing matrix"""
    
    def __init__(self, canvas: toga.Canvas, registry, model: UI_NMOS_ConnectionMatrix):
        self.canvas = canvas
        self.registry = registry
        
        # Drawing parameters
        self.margin_left = 150
        self.margin_top = 80
        self.base_cell_width = 40
        self.base_cell_height = 40
        self.zoom_factor = 1.0
        self.model = model
        
        
    @property
    def cell_width(self):
        return self.base_cell_width * self.zoom_factor
    
    @property
    def cell_height(self):
        return self.base_cell_height * self.zoom_factor
    
    def set_zoom_factor(self, factor):
        """Set the zoom factor for the canvas"""
        self.zoom_factor = factor
    
    def get_cell_at_position(self, x, y):
        """
        Get the sender and receiver at the given canvas position.
        
        Args:
            x: X coordinate on the canvas
            y: Y coordinate on the canvas
            
        Returns:
            Tuple of (sender_info, receiver_info) where each is a tuple of
            (node, device, sender/receiver, channel) or (None, None) if outside matrix
        """
        # Check if we have sender/receiver data
        if not hasattr(self, 'senders') or not hasattr(self, 'receivers'):
            return (None, None)
        
        # Check if click is within matrix bounds
        if x < self.margin_left or y < self.margin_top:
            return (None, None)
        
        # Calculate column (receiver) index
        col_index = int((x - self.margin_left) / self.cell_width)
        if col_index < 0 or col_index >= len(self.receivers):
            return (None, None)
        
        # Calculate row (sender) index
        row_index = int((y - self.margin_top) / self.cell_height)
        if row_index < 0 or row_index >= len(self.senders):
            return (None, None)
        
        sender_info = self.senders[row_index]
        receiver_info = self.receivers[col_index]
        
        return (sender_info, receiver_info)
    
    def scaled_font_size(self, base_size):
        """Get a font size scaled by the current zoom factor"""
        return int(base_size * self.zoom_factor)
    
    def draw(self):
        """Draws the routing matrix as represented in self.model"""
        # Build flat lists of all senders (rows) and receivers (columns)
        senders = []  # List of (node, device, sender, channel) tuples
        receivers = []  # List of (node, device, receiver, channel) tuples
        
        # Store for later access (e.g., click detection)
        self.senders = senders
        self.receivers = receivers
        
        # Traverse the model to build sender rows
        row_nodes = self.model.get_rows()
        print(f"Debug: get_rows() returned {len(row_nodes)} row nodes")
        for idx, row_node in enumerate(row_nodes):
            row_devices = row_node.get_rows()
            print(f"Debug: Row node {idx} '{row_node.node.name}' has {len(row_devices)} devices")
            for dev_idx, row_device in enumerate(row_devices):
                row_senders = row_device.get_rows()
                print(f"Debug:   Device {dev_idx} has {len(row_senders)} senders")
                for sender_idx, row_sender in enumerate(row_senders):
                    channels = row_sender.get_rows()
                    print(f"Debug:     Sender {sender_idx} '{row_sender.get_name()}' has {len(channels)} channels")
                    if channels:
                        for channel in channels:
                            senders.append((row_node, row_device, row_sender, channel))
                    else:
                        # Sender without channels
                        senders.append((row_node, row_device, row_sender, None))
        
        # Traverse the model to build receiver columns
        col_nodes = self.model.get_columns()
        print(f"Debug: get_columns() returned {len(col_nodes)} column nodes")
        for idx, col_node in enumerate(col_nodes):
            col_devices = col_node.get_columns()
            print(f"Debug: Column node {idx} '{col_node.node.name}' has {len(col_devices)} devices")
            for dev_idx, col_device in enumerate(col_devices):
                col_receivers = col_device.get_columns()
                print(f"Debug:   Device {dev_idx} has {len(col_receivers)} receivers")
                for receiver_idx, col_receiver in enumerate(col_receivers):
                    channels = col_receiver.get_columns()
                    print(f"Debug:     Receiver {receiver_idx} '{col_receiver.get_name()}' has {len(channels)} channels")
                    if channels:
                        for channel in channels:
                            receivers.append((col_node, col_device, col_receiver, channel))
                    else:
                        # Receiver without channels
                        receivers.append((col_node, col_device, col_receiver, None))
        
        # Debug output
        print(f"Matrix draw: {len(senders)} senders (rows), {len(receivers)} receivers (columns)")
        print(f"Model has {len(self.model.get_rows())} row nodes, {len(self.model.get_columns())} column nodes")
        
        # Calculate the maximum row label width to position the matrix
        max_row_label_width = 0
        row_label_font = toga.Font(family="sans-serif", size=self.scaled_font_size(8))
        for node, device, sender, channel in senders:
            channel_name = channel.get_name() if channel else "(no channel)"
            label = f"{node.get_name()} / {device.get_name()} / {sender.get_name()} / {channel_name}"
            label_width, _ = self.canvas.measure_text(label, font=row_label_font)
            max_row_label_width = max(max_row_label_width, label_width)
        
        # Calculate the maximum column label width for proper margin_top
        max_col_label_width = 0
        col_label_font = toga.Font(family="sans-serif", size=self.scaled_font_size(8))
        for node, device, receiver, channel in receivers:
            channel_name = channel.get_name() if channel else "(no channel)"
            label = f"{node.get_name()} / {device.get_name()} / {receiver.get_name()} / {channel_name}"
            label_width, _ = self.canvas.measure_text(label, font=col_label_font)
            max_col_label_width = max(max_col_label_width, label_width)
        
        # When rotated 45 degrees, the text needs height = width * sin(45°) ≈ width * 0.707
        # Add space for "Receivers" label and margins
        rotated_label_height = max_col_label_width * 0.707
        self.margin_top = 40 + rotated_label_height + 20  # "Receivers" label + rotated text + spacing
        
        # The rotated column headers also extend horizontally to the right
        # Horizontal extent = width * cos(45°) ≈ width * 0.707
        rotated_label_right_extent = max_col_label_width * 0.707
        
        # Measure "Senders" axis label width
        axis_label_font = toga.Font(family="sans-serif", size=self.scaled_font_size(14))
        senders_text_width, _ = self.canvas.measure_text("Senders", font=axis_label_font)
        
        # Set margin_left to accommodate axis label + row labels + spacing
        self.margin_left = 10 + senders_text_width + 10 + max_row_label_width + 20
        
        # Calculate required canvas size based on matrix dimensions
        # Include space for rotated column headers extending to the right
        required_width = self.margin_left + len(receivers) * self.cell_width + rotated_label_right_extent + 20
        required_height = self.margin_top + len(senders) * self.cell_height + 20  # +20 for bottom margin
        
        # Store the required dimensions for use by the parent container
        self.required_width = max(int(required_width), 800)
        self.required_height = max(int(required_height), 600)
        
        # Clear and draw on canvas
        self.canvas.context.clear()
        
        # Draw light blue background to fill the entire canvas
        with self.canvas.Fill(color=rgb(240, 248, 255)) as fill:
            fill.rect(0, 0, self.required_width, self.required_height)
        
        # Draw axis labels
        self._draw_axis_labels()
        
        # Draw column headers (receivers at top)
        self._draw_column_headers(senders, receivers)
        
        # Draw row headers (senders on left)
        self._draw_row_headers(senders, receivers)
        
        # Draw the matrix grid and connections
        self._draw_matrix_grid(senders, receivers)
    
    def _draw_axis_labels(self):
        """Draw 'Senders' and 'Receivers' labels"""
        # Draw "Receivers" at the top (horizontal for now until rotation works properly)
        with self.canvas.Fill(color=rgb(0, 0, 0)) as text_filler:
            text_filler.write_text(
                "Receivers",
                self.margin_left + 20,
                20,
                font=toga.Font(family="sans-serif", size=self.scaled_font_size(14))
            )
        
        # Draw "Senders" on the left rotated 90 degrees (vertical, reading upward)
        with self.canvas.context.Context():
            # Move to position where we want the text
            self.canvas.context.translate(30, self.margin_top + 60)
            
            # Rotate -90 degrees (π/2 radians counterclockwise, which is -π/2 in canvas coordinates)
            self.canvas.context.rotate(-1.5708)  # -90 degrees = -π/2 radians
            
            with self.canvas.Fill(color=rgb(0, 0, 0)) as text_filler:
                text_filler.write_text(
                    "Senders",
                    0,
                    0,
                    font=toga.Font(family="sans-serif", size=self.scaled_font_size(14))
                )
            self.canvas.context.rotate(1.5708)  # Rotate back
            self.canvas.context.translate(-30, -(self.margin_top + 60))
    
    def _draw_column_headers(self, senders, receivers):
        """Draw receiver column headers at the top rotated 45 degrees"""
        x = self.margin_left
        
        for idx, (node, device, receiver, channel) in enumerate(receivers):
            # Build label with node, device, receiver, and channel names
            channel_name = channel.get_name() if channel else "(no channel)"
            label = f"{node.get_name()} / {device.get_name()} / {receiver.get_name()} / {channel_name}"
            
            # Use a sub-context for isolated transformations
            with self.canvas.context.Context():
                # Move to the base of where the rotated text should start
                self.canvas.context.translate(x + self.cell_width / 2, self.margin_top - 5)
                
                # Rotate -45 degrees
                self.canvas.context.rotate(-0.785398)
                
                # Draw the text
                with self.canvas.Fill(color=rgb(0, 0, 0)) as text_filler:
                    text_filler.write_text(
                        label,
                        0,
                        0,
                        font=toga.Font(family="sans-serif", size=self.scaled_font_size(8))
                    )
                self.canvas.context.rotate(0.785398)
                self.canvas.context.translate(-(x + self.cell_width / 2), -(self.margin_top - 5))
            
            x += self.cell_width
    
    def _draw_row_headers(self, senders, receivers):
        """Draw sender row headers on the left"""
        y = self.margin_top
        
        # Measure actual width of "Senders" text
        axis_label_font = toga.Font(family="sans-serif", size=self.scaled_font_size(14))
        senders_text_width, senders_text_height = self.canvas.measure_text("Senders", font=axis_label_font)
        row_label_x = 10 + senders_text_width + 10  # 10px left margin + text width + 10px spacing
        
        for idx, (node, device, sender, channel) in enumerate(senders):
            # Build label with node, device, sender, and channel names
            channel_name = channel.get_name() if channel else "(no channel)"
            label = f"{node.get_name()} / {device.get_name()} / {sender.get_name()} / {channel_name}"
            
            with self.canvas.Fill(color=rgb(0, 0, 0)) as text_filler:
                text_filler.write_text(
                    label,
                    row_label_x,
                    y + self.cell_height / 2,
                    font=toga.Font(family="sans-serif", size=self.scaled_font_size(8)),
                    baseline=Baseline.MIDDLE
                )
            
            y += self.cell_height
    
    def _draw_matrix_grid(self, senders, receivers):
        """Draw the matrix grid and connection indicators"""
        # Draw vertical grid lines
        with self.canvas.Stroke(color=rgb(200, 200, 200), line_width=1) as stroke:
            x = self.margin_left
            for i in range(len(receivers) + 1):
                stroke.move_to(x, self.margin_top)
                stroke.line_to(x, self.margin_top + len(senders) * self.cell_height)
                x += self.cell_width
        
        # Draw horizontal grid lines
        with self.canvas.Stroke(color=rgb(200, 200, 200), line_width=1) as stroke:
            y = self.margin_top
            for i in range(len(senders) + 1):
                stroke.move_to(self.margin_left, y)
                stroke.line_to(self.margin_left + len(receivers) * self.cell_width, y)
                y += self.cell_height
        
        # Draw connection indicators
        print(f"About to check registry: registry={self.registry}")
        if self.registry:
            print(f"Registry is set! Calling _draw_connections")
            self._draw_connections(senders, receivers)
        else:
            print(f"Registry is NOT set - skipping _draw_connections")
    
    def _draw_connections(self, senders, receivers):
        """Draw indicators for active connections in the matrix"""
        print(f"_draw_connections called with {len(senders)} senders and {len(receivers)} receivers")
        for sender_idx, (s_node, s_device, s_sender, s_channel) in enumerate(senders):
            for receiver_idx, (r_node, r_device, r_receiver, r_channel) in enumerate(receivers):
                # Check if there's a connection between this sender and receiver
                if self._is_connected(s_node, s_device, s_sender, s_channel,
                                     r_node, r_device, r_receiver, r_channel):
                    # Draw a checkmark
                    x = self.margin_left + receiver_idx * self.cell_width
                    y = self.margin_top + sender_idx * self.cell_height
                    
                    center_x = x + self.cell_width / 2
                    center_y = y + self.cell_height / 2
                    
                    # Calculate checkmark size based on cell size
                    check_size = min(self.cell_width, self.cell_height) * 0.6
                    
                    # Draw checkmark using strokes
                    # Checkmark consists of two lines forming a "✓" shape
                    with self.canvas.Stroke(color=rgb(0, 180, 0), line_width=max(2, int(check_size / 10))) as stroke:
                        # Short vertical line (bottom left part)
                        stroke.move_to(center_x - check_size * 0.3, center_y)
                        stroke.line_to(center_x - check_size * 0.1, center_y + check_size * 0.25)
                        
                        # Long diagonal line (bottom right part going up)
                        stroke.move_to(center_x - check_size * 0.1, center_y + check_size * 0.25)
                        stroke.line_to(center_x + check_size * 0.35, center_y - check_size * 0.3)
    
    def _is_connected(self, s_node, s_device, s_sender, s_channel,
                      r_node, r_device, r_receiver, r_channel):
        """Check if sender and receiver are connected"""
        # Query the registry to determine if there's an active connection
        # This is a simplified check - you may need to implement more sophisticated logic
        
        if not self.registry:
            return False
        
        # For now, check if both are on the same node and have channel mappings
        if s_node.node.node_id == r_node.node.node_id:
            # Same node - check IS-08 channel mappings
            nmos_node = self.registry.nodes.get(s_node.node.node_id)
            if nmos_node:
                for device in nmos_node.devices:
                    if device.device_id == s_device.device.device_id:
                        # Check if sender's output channel is mapped to receiver's input channel
                        if r_channel and s_channel:
                            for output_dev in device.is08_output_channels:
                                for out_ch in output_dev.channels:
                                    # Match output channel by ID (if not empty) or label
                                    # Be very strict about matching to avoid false positives
                                    id_match = out_ch.id and s_channel.channel.id and out_ch.id == s_channel.channel.id
                                    label_match = (not id_match) and out_ch.label == s_channel.channel.label
                                    if id_match or label_match:
                                        # Check if this output channel has a mapped InputChannel
                                        if out_ch.mapped_device:
                                            # Compare the mapped InputChannel with the receiver channel
                                            # Match by ID (if not empty) or label
                                            input_id_match = (out_ch.mapped_device.id and r_channel.channel.id and 
                                                            out_ch.mapped_device.id == r_channel.channel.id)
                                            input_label_match = (not input_id_match) and out_ch.mapped_device.label == r_channel.channel.label
                                            if input_id_match or input_label_match:
                                                return True
        
        # Check IS-05 connections between different nodes
        # This would require tracking sender-receiver connections in the registry
        # For now, return False for cross-node connections
        
        return False
    