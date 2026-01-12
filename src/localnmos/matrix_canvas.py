"""
Routing Matrix Canvas Drawing
"""

from typing import List
import toga
from toga.fonts import SANS_SERIF
from toga.constants import Baseline
from toga.colors import rgb

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
    
    def scaled_font_size(self, base_size):
        """Get a font size scaled by the current zoom factor"""
        return int(base_size * self.zoom_factor)
    