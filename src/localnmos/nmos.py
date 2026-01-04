"""
NMOS data models

This module defines the core NMOS data structures for devices and nodes.
"""

from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class InputChannel:
    """Represents an IS-08 NMOS input channel"""
    id: str
    label: str


@dataclass
class InputDevice:
    """
    This contains the info retrieved from /x-nmos/channelmapping/v1.1/inputs/{inputId}/
    """
    id: str
    channels: List[InputChannel]
    name: str  # retrieved via inputs/${inputId}/properties
    description: str
    reordering: bool  # via /caps
    block_size: int   # via /caps
    parent_id: str  # id of parent
    parent_type: str  # source/device

    def __post_init__(self):
        if self.channels is None:
            self.channels = []


@dataclass
class OutputChannel:
    """Represents an IS-08 NMOS output channel"""
    id: str
    label: str
    mapped_device: InputChannel | None
    mapped_channel: int | None


@dataclass
class OutputDevice:
    """
    This contains the info retrieved from /x-nmos/channelmapping/v1.1/outputs/{outputId}/
    """
    id: str
    channels: List[OutputChannel]
    name: str  # retrieved via outputs/${outputId}/properties
    description: str  # via /properties
    source_id: str  # the IS-04 Source associated with this output device
    routable_inputs: List[str]

    def __post_init__(self):
        if self.channels is None:
            self.channels = []





@dataclass
class NMOS_Device:
    """Represents an NMOS device discovered on the network"""
    node_id: str
    device_id: str
    senders: List['NMOS_Device']
    receivers: List['NMOS_Device']
    is08_input_channels: List[InputDevice]
    is08_output_channels: List[OutputDevice] # given ["Out1/", "Out2/"] for /x-nmos/channelmapping/v1.1/outputs, then there is one entry in the list for each

    def __post_init__(self):
        if self.is08_input_channels is None:
            self.is08_input_channels = []
        if self.is08_output_channels is None:
            self.is08_output_channels = []
        if self.senders is None:
            self.senders = []
        if self.receivers is None:
            self.receivers = []


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
    channel_mapping_api_ver: str = "v1.0"


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
        return f"{self.base_url}/x-nmos/channelmapping/{self.channel_mapping_api_ver}"

    @property
    def query_url(self) -> str:
        """Get the query API URL (IS-04)"""
        return f"{self.base_url}/x-nmos/query/{self.api_ver}"
