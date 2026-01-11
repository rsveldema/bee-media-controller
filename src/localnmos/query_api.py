"""
NMOS Query API Server (IS-04)

This module provides an HTTP server that implements the NMOS IS-04 Query API,
allowing clients to discover and query registered nodes, devices, senders, and receivers.
"""

import logging
import socket
import traceback
from typing import Dict, Optional
from aiohttp import web
from aiohttp.web_response import json_response

try:
    from zeroconf import ServiceInfo, Zeroconf
except ImportError:
    ServiceInfo = None
    Zeroconf = None

from .nmos import NMOS_Node
from .error_log import ErrorLog


logger = logging.getLogger(__name__)


class NMOSQueryAPI:
    """NMOS IS-04 Query API Server"""

    def __init__(
        self,
        nodes: Dict[str, NMOS_Node],
        zeroconf: Optional[Zeroconf] = None,
        listen_ip: Optional[str] = None,
        port: int = 8081,
        service_type: str = "_nmos-query._tcp.local."
    ):
        """
        Initialize the Query API server

        Args:
            nodes: Dictionary of registered nodes (shared with registry)
            zeroconf: Zeroconf instance for mDNS registration
            listen_ip: IP address to bind to
            port: Port to listen on (default: 8081)
            service_type: mDNS service type for Query API
        """
        self.nodes = nodes
        self.zeroconf = zeroconf
        self.listen_ip = listen_ip
        self.port = port
        self.service_type = service_type
        self.runner = None
        self.service_info: Optional[ServiceInfo] = None

    async def start(self):
        """Start the Query API HTTP server"""
        try:
            app = web.Application()
            # IS-04 Query API endpoints
            app.router.add_get('/x-nmos/query/{api_version}/nodes', self._handle_query_nodes)
            app.router.add_get('/x-nmos/query/{api_version}/nodes/{node_id}', self._handle_query_node)
            app.router.add_get('/x-nmos/query/{api_version}/devices', self._handle_query_devices)
            app.router.add_get('/x-nmos/query/{api_version}/devices/{device_id}', self._handle_query_device)
            app.router.add_get('/x-nmos/query/{api_version}/senders', self._handle_query_senders)
            app.router.add_get('/x-nmos/query/{api_version}/senders/{sender_id}', self._handle_query_sender)
            app.router.add_get('/x-nmos/query/{api_version}/receivers', self._handle_query_receivers)
            app.router.add_get('/x-nmos/query/{api_version}/receivers/{receiver_id}', self._handle_query_receiver)
            app.router.add_get('/x-nmos/query/{api_version}/sources', self._handle_query_sources)
            app.router.add_get('/x-nmos/query/{api_version}/flows', self._handle_query_flows)

            self.runner = web.AppRunner(app)
            await self.runner.setup()

            # Bind to specified IP or all interfaces
            bind_address = self.listen_ip or '0.0.0.0'
            site = web.TCPSite(self.runner, bind_address, self.port)
            await site.start()

            logger.info(f"Query API HTTP server started on {bind_address}:{self.port}")
            
            # Note: mDNS service registration is handled by NMOSMDNSService, not here
        except Exception as e:
            error_msg = f"Failed to start Query API server: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def stop(self):
        """Stop the HTTP Query API server"""
        if self.runner:
            try:
                await self.runner.cleanup()
                logger.info("Query API HTTP server stopped")
                self.runner = None
            except Exception as e:
                error_msg = f"Failed to stop Query API server: {e}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _register_service(self):
        """Register and advertise NMOS Query API service via mDNS"""
        if not ServiceInfo or not self.zeroconf:
            return

        try:
            hostname = socket.gethostname()
            if self.listen_ip:
                local_ip = self.listen_ip
            else:
                local_ip = socket.gethostbyname(hostname)

            service_name = f"NMOS Query API on {hostname}.{self.service_type}"
            properties = {
                b"api_ver": b"v1.3",
                b"api_proto": b"http",
                b"pri": b"100",
            }

            self.service_info = ServiceInfo(
                self.service_type,
                service_name,
                addresses=[socket.inet_aton(local_ip)],
                port=self.port,
                properties=properties,
                server=f"{hostname}.local."
            )

            self.zeroconf.register_service(self.service_info)
            logger.info(f"Query API service registered via mDNS: {service_name}")
        except Exception as e:
            error_msg = f"Failed to register Query API service: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _unregister_service(self):
        """Unregister Query API service from mDNS"""
        if self.service_info and self.zeroconf:
            try:
                self.zeroconf.unregister_service(self.service_info)
                logger.info("Query API service unregistered from mDNS")
                self.service_info = None
            except Exception as e:
                error_msg = f"Failed to unregister Query API service: {e}"
                logger.error(error_msg)
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _handle_query_nodes(self, request):
        """Handle GET /x-nmos/query/{api_version}/nodes"""
        try:
            nodes_data = []
            for node in self.nodes.values():
                node_data = {
                    'id': node.node_id,
                    'label': node.name,
                    'description': node.properties.get('description', ''),
                    'version': node.version,
                    'hostname': node.address,
                    'api': {
                        'versions': [node.api_ver],
                        'endpoints': [{
                            'host': node.address,
                            'port': node.port,
                            'protocol': 'http'
                        }]
                    }
                }
                nodes_data.append(node_data)
            return json_response(nodes_data, status=200)
        except Exception as e:
            error_msg = f"Error handling query nodes: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_query_node(self, request):
        """Handle GET /x-nmos/query/{api_version}/nodes/{node_id}"""
        try:
            node_id = request.match_info.get('node_id')
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node_data = {
                    'id': node.node_id,
                    'label': node.name,
                    'description': node.properties.get('description', ''),
                    'version': node.version,
                    'hostname': node.address,
                    'api': {
                        'versions': [node.api_ver],
                        'endpoints': [{
                            'host': node.address,
                            'port': node.port,
                            'protocol': 'http'
                        }]
                    }
                }
                return json_response(node_data, status=200)
            else:
                return json_response({'error': 'Node not found'}, status=404)
        except Exception as e:
            error_msg = f"Error handling query node: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_query_devices(self, request):
        """Handle GET /x-nmos/query/{api_version}/devices"""
        try:
            devices_data = []
            for node in self.nodes.values():
                for device in node.devices:
                    device_data = {
                        'id': device.device_id,
                        'label': device.label,
                        'description': device.description,
                        'node_id': node.node_id,
                        'senders': [s.sender_id if hasattr(s, 'sender_id') else str(s) for s in device.senders],
                        'receivers': [r.receiver_id if hasattr(r, 'receiver_id') else str(r) for r in device.receivers]
                    }
                    devices_data.append(device_data)
            return json_response(devices_data, status=200)
        except Exception as e:
            error_msg = f"Error handling query devices: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_query_device(self, request):
        """Handle GET /x-nmos/query/{api_version}/devices/{device_id}"""
        try:
            device_id = request.match_info.get('device_id')
            for node in self.nodes.values():
                for device in node.devices:
                    if device.device_id == device_id:
                        device_data = {
                            'id': device.device_id,
                            'label': device.label,
                            'description': device.description,
                            'node_id': node.node_id,
                            'senders': [s.sender_id if hasattr(s, 'sender_id') else str(s) for s in device.senders],
                            'receivers': [r.receiver_id if hasattr(r, 'receiver_id') else str(r) for r in device.receivers]
                        }
                        return json_response(device_data, status=200)
            return json_response({'error': 'Device not found'}, status=404)
        except Exception as e:
            error_msg = f"Error handling query device: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_query_senders(self, request):
        """Handle GET /x-nmos/query/{api_version}/senders"""
        try:
            senders_data = []
            for node in self.nodes.values():
                for device in node.devices:
                    for sender in device.senders:
                        sender_id = sender.sender_id if hasattr(sender, 'sender_id') else str(sender)
                        sender_data = {
                            'id': sender_id,
                            'device_id': device.device_id,
                            'label': f"Sender {sender_id}"
                        }
                        senders_data.append(sender_data)
            return json_response(senders_data, status=200)
        except Exception as e:
            error_msg = f"Error handling query senders: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_query_sender(self, request):
        """Handle GET /x-nmos/query/{api_version}/senders/{sender_id}"""
        try:
            sender_id = request.match_info.get('sender_id')
            for node in self.nodes.values():
                for device in node.devices:
                    for sender in device.senders:
                        s_id = sender.sender_id if hasattr(sender, 'sender_id') else str(sender)
                        if s_id == sender_id:
                            sender_data = {
                                'id': s_id,
                                'device_id': device.device_id,
                                'label': f"Sender {s_id}"
                            }
                            return json_response(sender_data, status=200)
            return json_response({'error': 'Sender not found'}, status=404)
        except Exception as e:
            error_msg = f"Error handling query sender: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_query_receivers(self, request):
        """Handle GET /x-nmos/query/{api_version}/receivers"""
        try:
            receivers_data = []
            for node in self.nodes.values():
                for device in node.devices:
                    for receiver in device.receivers:
                        receiver_id = receiver.receiver_id if hasattr(receiver, 'receiver_id') else str(receiver)
                        receiver_data = {
                            'id': receiver_id,
                            'device_id': device.device_id,
                            'label': f"Receiver {receiver_id}"
                        }
                        receivers_data.append(receiver_data)
            return json_response(receivers_data, status=200)
        except Exception as e:
            error_msg = f"Error handling query receivers: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_query_receiver(self, request):
        """Handle GET /x-nmos/query/{api_version}/receivers/{receiver_id}"""
        try:
            receiver_id = request.match_info.get('receiver_id')
            for node in self.nodes.values():
                for device in node.devices:
                    for receiver in device.receivers:
                        r_id = receiver.receiver_id if hasattr(receiver, 'receiver_id') else str(receiver)
                        if r_id == receiver_id:
                            receiver_data = {
                                'id': r_id,
                                'device_id': device.device_id,
                                'label': f"Receiver {r_id}"
                            }
                            return json_response(receiver_data, status=200)
            return json_response({'error': 'Receiver not found'}, status=404)
        except Exception as e:
            error_msg = f"Error handling query receiver: {e}"
            logger.error(error_msg)
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_query_sources(self, request):
        """Handle GET /x-nmos/query/{api_version}/sources"""
        # Sources are not currently tracked, return empty array
        return json_response([], status=200)

    async def _handle_query_flows(self, request):
        """Handle GET /x-nmos/query/{api_version}/flows"""
        # Flows are not currently tracked, return empty array
        return json_response([], status=200)
