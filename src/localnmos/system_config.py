"""
NMOS System Configuration API Server (IS-09)

This module provides an HTTP server that implements the NMOS IS-09 System API,
allowing clients to manage system-wide configuration and parameters.
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

from .error_log import ErrorLog
from .logging_utils import create_logger

logger = create_logger(__name__)


class NMOSSystemConfig:
    """NMOS IS-09 System Configuration API Server"""

    def __init__(
        self,
        zeroconf: Optional[Zeroconf] = None,
        listen_ip: Optional[str] = None,
        port: int = 8082,
        service_type: str = "_nmos-system._tcp.local."
    ):
        """
        Initialize the System Configuration API server

        Args:
            zeroconf: Zeroconf instance for mDNS registration
            listen_ip: IP address to bind to
            port: Port to listen on (default: 8082)
            service_type: mDNS service type for System API
        """
        self.zeroconf = zeroconf
        self.listen_ip = listen_ip
        self.port = port
        self.service_type = service_type
        self.runner = None
        self.service_info: Optional[ServiceInfo] = None
        
        # System parameters storage
        self.global_config: Dict = {}

    async def start(self):
        """Start the System Configuration API HTTP server"""
        try:
            app = web.Application()
            # IS-09 System API endpoints
            app.router.add_get('/x-nmos/system/{api_version}/', self._handle_root)
            app.router.add_get('/x-nmos/system/{api_version}/global', self._handle_get_global)
            app.router.add_post('/x-nmos/system/{api_version}/global', self._handle_post_global)

            self.runner = web.AppRunner(app)
            await self.runner.setup()

            # Bind to specified IP or all interfaces
            bind_address = self.listen_ip or '0.0.0.0'
            site = web.TCPSite(self.runner, bind_address, self.port)
            await site.start()

            logger.info(f"System Configuration API HTTP server started on {bind_address}:{self.port}")
            
            # Note: mDNS service registration is handled by NMOSMDNSService, not here
        except Exception as e:
            error_msg = f"Failed to start System Configuration API server: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def stop(self):
        """Stop the HTTP System Configuration API server"""
        if self.runner:
            try:
                await self.runner.cleanup()
                logger.info("System Configuration API HTTP server stopped")
                self.runner = None
            except Exception as e:
                error_msg = f"Failed to stop System Configuration API server: {e}"
                ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())

    async def _handle_root(self, request):
        """Handle GET /x-nmos/system/{api_version}/"""
        try:
            api_version = request.match_info.get('api_version', 'v1.0')
            return json_response([
                f"global/"
            ], status=200)
        except Exception as e:
            error_msg = f"Error handling system root request: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_get_global(self, request):
        """Handle GET /x-nmos/system/{api_version}/global"""
        try:
            api_version = request.match_info.get('api_version', 'v1.0')
            
            # Return current global configuration
            # Default empty configuration if none set
            if not self.global_config:
                self.global_config = {
                    "id": "00000000-0000-0000-0000-000000000000",
                    "version": "0:0",
                    "label": "Default System Configuration",
                    "description": "NMOS IS-09 System Configuration",
                    "tags": {},
                    "ptp_domain_number": 0,
                    "is04_token": None,
                    "is05_merge_strategy": "preserve_current"
                }
            
            return json_response(self.global_config, status=200)
        except Exception as e:
            error_msg = f"Error handling global config GET request: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=500)

    async def _handle_post_global(self, request):
        """Handle POST /x-nmos/system/{api_version}/global"""
        try:
            api_version = request.match_info.get('api_version', 'v1.0')
            data = await request.json()
            
            # Update global configuration
            self.global_config = data
            
            logger.info(f"Global system configuration updated: {data}")
            return json_response(self.global_config, status=200)
        except Exception as e:
            error_msg = f"Error handling global config POST request: {e}"
            ErrorLog().add_error(error_msg, exception=e, traceback_str=traceback.format_exc())
            return json_response({'error': str(e)}, status=400)
