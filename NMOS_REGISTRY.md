# NMOS Registry with MDNS Support

This project now includes an NMOS registry with MDNS (mDNS/DNS-SD) support for automatic discovery of NMOS devices on the local network.

## Overview

The NMOS registry implements:
- **IS-04 Discovery and Registration**: Automatic discovery of NMOS nodes using mDNS
- **IS-05 Device Connection Management**: Connection API support for discovered devices
- **Real-time Updates**: Automatic detection when devices join or leave the network
- **Multi-service Discovery**: Discovers node, registration, and query services

## Features

### NMOSRegistry Class

The `NMOSRegistry` class provides comprehensive NMOS device discovery:

- **Automatic MDNS Discovery**: Discovers NMOS services advertised via Zeroconf/Bonjour
- **Device Lifecycle Management**: Tracks devices as they appear and disappear from the network
- **API Version Support**: Supports multiple NMOS API versions (default: v1.3)
- **Service Types Discovered**:
  - `_nmos-node._tcp.local.` - NMOS nodes (devices)
  - `_nmos-registration._tcp.local.` - Registration services
  - `_nmos-query._tcp.local.` - Query services

### NMOSDevice Class

Each discovered device is represented as an `NMOSDevice` with:
- Device name and unique ID
- IP address and port
- Service type and API version
- Base URLs for node and connection APIs
- TXT record properties from mDNS

## Usage

### Basic Setup

The registry is automatically initialized when the app starts:

```python
from localnmos.registry import NMOSRegistry, NMOSDevice

# Create registry with callbacks
registry = NMOSRegistry(
    device_added_callback=on_device_discovered,
    device_removed_callback=on_device_removed
)

# Start discovery
await registry.start()

# Get all discovered devices
devices = registry.get_devices()

# Stop when done
await registry.stop()
```

### Callbacks

Define callbacks to handle device events:

```python
def on_device_discovered(device: NMOSDevice):
    print(f"Found device: {device.name}")
    print(f"Address: {device.address}:{device.port}")
    print(f"Node API: {device.node_url}")
    print(f"Connection API: {device.connection_url}")

def on_device_removed(device: NMOSDevice):
    print(f"Device removed: {device.name}")
```

### Querying Devices

Query device APIs for information:

```python
# Query device self-description
info = await registry.query_device(device, "/self")

# Query device senders
senders = await registry.query_device(device, "/senders")

# Query device receivers  
receivers = await registry.query_device(device, "/receivers")
```

## Dependencies

The NMOS registry requires:
- **zeroconf** (≥0.132.0): For mDNS/DNS-SD service discovery
- **aiohttp** (≥3.9.0): For async HTTP API queries

These are automatically included in `pyproject.toml`.

## Installation

Install dependencies:

```bash
# Using pip
pip install zeroconf aiohttp

# Or using briefcase (recommended for this Toga app)
briefcase dev
```

## Integration with Toga App

The registry is integrated into the Toga UI:

1. **Startup**: Registry starts in `on_running()` method
2. **Device Display**: Discovered devices appear in the DetailedList
3. **Real-time Updates**: UI updates automatically as devices appear/disappear
4. **Cleanup**: Registry stops cleanly on app exit

## NMOS Standards Supported

- **IS-04**: NMOS Discovery and Registration Specification
- **IS-05**: NMOS Device Connection Management Specification

## Network Requirements

- Devices must be on the same local network
- Multicast DNS must be enabled (mDNS)
- Port 5353 must be accessible for mDNS
- NMOS devices must advertise via mDNS/DNS-SD

## Troubleshooting

### No devices discovered

1. Ensure NMOS devices are on the same network
2. Check that devices are advertising via mDNS
3. Verify firewall allows mDNS (port 5353 UDP)
4. Check logs for discovery messages

### Import errors

If you see `ModuleNotFoundError: No module named 'zeroconf'`:

```bash
pip install zeroconf aiohttp
```

## Example NMOS Device Advertising

NMOS devices should advertise with TXT records like:
```
api_ver=v1.3
api_proto=http
node_id=<unique-device-id>
```

## API Reference

### NMOSRegistry

**Methods:**
- `start()`: Start MDNS discovery
- `stop()`: Stop discovery and cleanup
- `get_devices()`: Get all discovered devices
- `query_device(device, endpoint)`: Query device API

### NMOSDevice

**Properties:**
- `name`: Device name
- `device_id`: Unique device ID
- `address`: IP address
- `port`: Port number
- `base_url`: Base HTTP URL
- `node_url`: IS-04 Node API URL
- `connection_url`: IS-05 Connection API URL
- `properties`: TXT record properties

## License

Same as the main project (see LICENSE file).
