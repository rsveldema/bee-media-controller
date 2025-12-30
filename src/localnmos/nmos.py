"""
NMOS data models

This module defines the core NMOS data structures for devices and nodes.
"""

from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class NMOS_Device:
    """Represents an NMOS device discovered on the network"""
    node_id: str
    device_id: str
    senders: List['NMOS_Device']
    receivers: List['NMOS_Device']
    channels: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.senders is None:
            self.senders = []
        if self.receivers is None:
            self.receivers = []
        if self.channels is None:
            self.channels = []


@dataclass
class NMOS_Node:
    """Represents an NMOS Node discovered on the network"""

    name: str
    node_id: str
    address: str
    port: int
    service_type: str
    properties: Dict[str, Any]
    devices: List[NMOS_Device]
    version: str = "v1.3"
    api_ver: str = "v1.3"

    def __post_init__(self):
        if self.devices is None:
            self.devices = []
        if self.properties is None:
            self.properties = {}

    @property
    def base_url(self) -> str:
        """Get the base URL for the device API"""
        return f"http://{self.address}:{self.port}"

    @property
    def node_url(self) -> str:
        """Get the node API URL"""
        return f"{self.base_url}/x-nmos/node/{self.api_ver}"

    @property
    def connection_url(self) -> str:
        """Get the connection API URL (IS-05)"""
        return f"{self.base_url}/x-nmos/connection/{self.api_ver}"

    @property
    def channelmapping_url(self) -> str:
        """Get the channel mapping API URL (IS-08)"""
        return f"{self.base_url}/x-nmos/channelmapping/{self.api_ver}"
