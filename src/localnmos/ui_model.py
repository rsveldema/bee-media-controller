from localnmos import nmos

##----------------- Channels

class UI_NMOS_Row_Channels:
    def __init__(self, channel: nmos.OutputChannel | None = None) -> None:
        self.channel = channel

    def get_name(self) -> str:
        if self.channel:
            return self.channel.label
        return "(no channel)"

class UI_NMOS_Column_Channels:
    def __init__(self, channel: nmos.InputChannel | None = None) -> None:
        self.channel = channel

    def get_name(self) -> str:
        if self.channel:
            return self.channel.label
        return "(no channel)"

##----------------- Senders

class UI_NMOS_Row_Senders:
    def __init__(self, sender: nmos.NMOS_Sender | None = None, device: nmos.NMOS_Device | None = None) -> None:
        self.sender = sender
        self.device = device

    def get_name(self) -> str:
        if self.sender:
            return self.sender.label
        return "(no sender)"

    def get_rows(self) -> list[UI_NMOS_Row_Channels]:
        ret = []
        if self.device:
            # Get output channels from all output devices in the device (senders send OUT)
            sender_name = self.sender.label if self.sender else "(no sender)"
            print(f"      Sender '{sender_name}' has device with {len(self.device.is08_output_channels)} output channel lists")
            for output_dev in self.device.is08_output_channels:
                print(f"        Output device has {len(output_dev.channels)} channels")
                for channel in output_dev.channels:
                    ret.append(UI_NMOS_Row_Channels(channel))
        else:
            sender_name = self.sender.label if self.sender else "(no sender)"
            print(f"      Sender '{sender_name}' has NO device attached!")
        
        # If no channels found, add a dummy channel so the sender still appears in the matrix
        if len(ret) == 0:
            ret.append(UI_NMOS_Row_Channels(None))
        
        return ret


class UI_NMOS_Column_Receivers:
    def __init__(self, receiver: nmos.NMOS_Receiver | None = None, device: nmos.NMOS_Device | None = None) -> None:
        self.receiver = receiver
        self.device = device

    def get_name(self) -> str:
        if self.receiver:
            return self.receiver.label
        return "(no receiver)"

    def get_columns(self) -> list[UI_NMOS_Column_Channels]:
        ret = []
        if self.device:
            # Get input channels from all input devices in the device (receivers receive IN)
            receiver_name = self.receiver.label if self.receiver else "(no receiver)"
            print(f"      Receiver '{receiver_name}' has device with {len(self.device.is08_input_channels)} input channel lists")
            for input_dev in self.device.is08_input_channels:
                print(f"        Input device has {len(input_dev.channels)} channels")
                for channel in input_dev.channels:
                    ret.append(UI_NMOS_Column_Channels(channel))
        else:
            receiver_name = self.receiver.label if self.receiver else "(no receiver)"
            print(f"      Receiver '{receiver_name}' has NO device attached!")
        
        # If no channels found, add a dummy channel so the receiver still appears in the matrix
        if len(ret) == 0:
            ret.append(UI_NMOS_Column_Channels(None))
        
        return ret

##----------------- Devices

class UI_NMOS_Row_Devices:
    def __init__(self, device: nmos.NMOS_Device | None = None) -> None:
        self.device = device

    def get_name(self) -> str:
        if self.device:
            return self.device.label
        return "(no device)"
    
    def get_rows(self) -> list[UI_NMOS_Row_Senders]:
        ret = []
        if self.device:
            for sender in self.device.senders:
                ret.append(UI_NMOS_Row_Senders(sender, self.device))
        
        # If no senders, add a dummy sender so the device still appears
        if len(ret) == 0:
            ret.append(UI_NMOS_Row_Senders(None, self.device))
        
        return ret
    

class UI_NMOS_Column_Devices:
    def __init__(self, device: nmos.NMOS_Device | None = None) -> None:
        self.device = device

    def get_name(self) -> str:
        if self.device:
            return self.device.label
        return "(no device)"
    
    def get_columns(self) -> list[UI_NMOS_Column_Receivers]:
        ret = []
        if self.device:
            for receiver in self.device.receivers:
                ret.append(UI_NMOS_Column_Receivers(receiver, self.device))
        
        # If no receivers, add a dummy receiver so the device still appears
        if len(ret) == 0:
            ret.append(UI_NMOS_Column_Receivers(None, self.device))
        
        return ret
    

##------------------ Nodes


class UI_NMOS_Row_Node:
    def __init__(self, node: nmos.NMOS_Node) -> None:
        self.node = node

    def get_name(self) -> str:
        return self.node.name

    def get_rows(self) -> list[UI_NMOS_Row_Devices]:
        ret = []
        if self.node:
            for device in self.node.devices:
                ret.append(UI_NMOS_Row_Devices(device))
        
        # If no devices, add a dummy device so the node still appears
        if len(ret) == 0:
            ret.append(UI_NMOS_Row_Devices(None))
        
        return ret

class UI_NMOS_Column_Node:
    def __init__(self, node: nmos.NMOS_Node) -> None:
        self.node = node

    def get_name(self) -> str:
        return self.node.name

    def get_columns(self) -> list[UI_NMOS_Column_Devices]:
        ret = []
        if self.node:
            for device in self.node.devices:
                ret.append(UI_NMOS_Column_Devices(device))
        
        # If no devices, add a dummy device so the node still appears
        if len(ret) == 0:
            ret.append(UI_NMOS_Column_Devices(None))
        
        return ret

class UI_NMOS_ConnectionMatrix:
    def __init__(self) -> None:
        # initialized in the app once the registry is available
        self.nodes: dict[str, nmos.NMOS_Node] = None


    def get_rows(self) -> list[UI_NMOS_Row_Node]:
        row_nodes = []
        for p in self.get_nodes():
            row_nodes.append(UI_NMOS_Row_Node(p))
        return row_nodes    

    def get_columns(self) -> list[UI_NMOS_Column_Node]:
        column_nodes = []
        for p in self.get_nodes():
            column_nodes.append(UI_NMOS_Column_Node(p))
        return column_nodes    

    def get_nodes(self) -> list[nmos.NMOS_Node]:
        if self.nodes is None:
            return []
        return list(self.nodes.values())
