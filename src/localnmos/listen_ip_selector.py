"""
Listen IP Selector
Handles network interface selection for the NMOS registry.
"""

import netifaces
import toga
from toga.style.pack import Pack, ROW, COLUMN

from localnmos import logging_utils

logger = logging_utils.create_logger("localnmos.listen_ip_selector")


class ListenIPSelector:
    """Handles selection of network interface for NMOS registry listening"""
    
    def __init__(self, main_window: toga.MainWindow, registry, restart_callback):
        """
        Initialize the listen IP selector.
        
        Args:
            main_window: The main application window for displaying dialogs
            registry: The NMOS registry instance
            restart_callback: Async callback to restart registry with new IP
        """
        self.main_window = main_window
        self.registry = registry
        self.restart_callback = restart_callback
    
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
            logger.error(f"Error getting network interfaces: {e}")
        return interfaces
    
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
                    logger.info(f"Listen IP changing from {old_ip} to {selected_ip}")
                    
                    dialog.close()
                    
                    # Show progress message
                    try:
                        # Restart registry with new IP
                        await self.restart_callback(selected_ip)
                        
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
