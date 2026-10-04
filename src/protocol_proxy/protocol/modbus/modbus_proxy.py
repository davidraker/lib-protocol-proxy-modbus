import asyncio
import json
from uuid import UUID
import logging
import sys


from typing import Iterable

from protocol_proxy.ipc import callback, ProtocolProxyMessage
from protocol_proxy.proxy.asyncio import AsyncioProtocolProxy

from pymodbus import pymodbus_apply_logging_config

from .json import serialize
from .client import ModbusClient

_log = logging.getLogger(__name__)


class ModbusProxy(AsyncioProtocolProxy):
    """One proxy process serves any number of Modbus devices over any mix of transports.

    Clients (one per gateway, keyed by transport, address and port) are created by REGISTER_DEVICE; each holds the
    register data types of every unit behind it, declared via CONFIGURE_REGISTERS.
    """
    # Requests to units sharing a gateway queue behind one another (see ModbusClient), so a request may legitimately
    # wait for several other units' timeout-and-retry cycles before it runs. This bounds how long the IPC layer
    # waits before abandoning it; on expiry the running request is cancelled and the gateway lock released.
    DEFAULT_CALLBACK_TIMEOUT = 120.0

    def __init__(self, callback_timeout: float = DEFAULT_CALLBACK_TIMEOUT, **kwargs):
        pymodbus_apply_logging_config('INFO')
        super(ModbusProxy, self).__init__(**kwargs)
        self.clients: dict[str, ModbusClient] = {}
        # The caller's remote id (header version 2) -> (client key, unit id) it registered. Several remotes (unit ids)
        # share one client when they sit behind the same gateway connection.
        self.remotes: dict[UUID, tuple[str, int]] = {}
        self.loop = asyncio.get_event_loop()

        self.register_callback(self.register_device_endpoint, 'REGISTER_DEVICE', provides_response=True,
                               timeout=callback_timeout)
        self.register_callback(self.configure_registers_endpoint, 'CONFIGURE_REGISTERS', provides_response=True,
                               timeout=callback_timeout)
        self.register_callback(self.read_registers_endpoint, 'READ_REGISTERS', provides_response=True,
                               timeout=callback_timeout)
        self.register_callback(self.write_registers_endpoint, 'WRITE_REGISTERS', provides_response=True,
                               timeout=callback_timeout)

    @callback
    async def register_device_endpoint(self, headers, raw_message: bytes):
        """Endpoint for registering a Modbus device.

        Payload: device_address, device_type, and any pymodbus client options (port, timeout, retries, ...).
        May also carry the CONFIGURE_REGISTERS fields (unit_id, tables, max_gap, clear_others) to declare the
        device's register data types in the same round trip. The caller's remote id (header version 2) is remembered
        with the client and unit id, so later requests from that caller need neither in their payload.
        """
        message = json.loads(raw_message.decode('utf8'))
        remote_id = getattr(headers, 'remote_id', None)
        device_address = message.pop('device_address')
        transport_protocol = message.pop('device_type')
        client_key = self._get_client_key(device_address, transport_protocol, message)
        if not client_key:
            return serialize({'results': {}, 'errors': {'device': f"Unsupported device type: {transport_protocol}"}})
        tables = message.pop('tables', None)
        configure_args = {k: message.pop(k) for k in ('unit_id', 'max_gap', 'clear_others') if k in message}
        try:
            if client_key not in self.clients:
                self.clients[client_key] = await ModbusClient.create(device_address, transport_protocol, **message)
            if remote_id is not None:
                self.remotes[remote_id] = (client_key, int(configure_args.get('unit_id', 1)))
            results = {'client': client_key}
            if tables is not None:
                results.update(self.clients[client_key].configure_registers(tables, **configure_args))
            return serialize({'results': results, 'errors': {}})
        except Exception as e:
            _log.warning(f"Failed to register Modbus device {device_address}: {e}")
            return serialize({'results': {}, 'errors': {'device': str(e)}})

    @callback
    async def configure_registers_endpoint(self, _, raw_message: bytes):
        """Endpoint for declaring the data types of registers on a Modbus device.

        Payload: device_address, device_type, [port], unit_id (default 1), tables, [max_gap], [clear_others].
        tables maps a table name ('coil', 'discrete_input', 'holding', 'input') to a list of specs, each
        {address, data_type, [count], [word_order], [string_encoding]}. Pads are specs with data_type 'pad'.
        Each listed table is replaced whole. Other tables on the unit are kept unless clear_others is true.
        """
        message = json.loads(raw_message.decode('utf8'))
        device_address = message.pop('device_address')
        transport_protocol = message.pop('device_type')
        client = self._get_client(device_address, transport_protocol, message, 'configure')
        if client is None:
            return serialize({'results': {}, 'errors': {'device': 'Client not found'}})
        try:
            results = client.configure_registers(message.get('tables', {}),
                                                 unit_id=message.get('unit_id', 1),
                                                 max_gap=message.get('max_gap'),
                                                 clear_others=message.get('clear_others', False))
            return serialize({'results': results, 'errors': {}})
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            _log.warning(f"Failed to configure Modbus registers for {device_address}: {e}")
            return serialize({'results': {}, 'errors': {'configure': str(e)}})

    @callback
    async def read_registers_endpoint(self, headers, raw_message: bytes):
        """Endpoint for reading registers from a Modbus device.

        Payload: [queries], [register_map], [decode], and for a caller without a remote id in its header
        (version 1) device_address, device_type, [port], [unit_id]. With decode true, values are decoded using the
        map declared via CONFIGURE_REGISTERS, and queries may be omitted to read the whole map in the fewest requests.
        """
        message = json.loads(raw_message.decode('utf8'))
        client, unit_id = self._resolve(headers, message, 'read')
        if client is None:
            return None
        queries: Iterable[tuple[int, int]] | None = message.pop('queries', None)
        register_map: str = message.pop('register_map', 'holding')
        decode: bool = message.pop('decode', False)
        try:
            result = await client.read(queries, register_map, unit_id, decode=decode, **message)
        except ValueError as e:
            result = {'results': {} if decode else [], 'errors': {'read': str(e)} if decode else [str(e)]}
        return serialize(result)

    @callback
    async def write_registers_endpoint(self, headers, raw_message: bytes):
        """Endpoint for writing registers to a Modbus device.

        Payload: queries, [register_map], [encode], and for a caller without a remote id in its header (version 1)
        device_address, device_type, [port], [unit_id]. With encode true, each query's value is encoded using the map
        declared via CONFIGURE_REGISTERS.
        """
        message = json.loads(raw_message.decode('utf8'))
        client, unit_id = self._resolve(headers, message, 'write')
        if client is None:
            return None
        queries: Iterable[tuple[int, int, list]] = message.pop('queries', [])
        register_map: str = message.pop('register_map', 'holding')
        encode: bool = message.pop('encode', False)
        try:
            result = await client.write(queries, register_map, unit_id, encode=encode, **message)
        except ValueError as e:
            result = {'results': [], 'errors': [str(e)]}
        return serialize(result)

    def _resolve(self, headers, message: dict, action: str) -> tuple[ModbusClient | None, int]:
        """The client and unit id a request concerns: the header's remote id first, the payload's identity otherwise.

        Removes the identity fields from ``message`` so the remainder can be passed on as request options. A payload
        ``unit_id`` overrides the registered one.
        """
        remote_id = getattr(headers, 'remote_id', None)
        device_address = message.pop('device_address', None)
        transport_protocol = message.pop('device_type', None)
        registered = self.remotes.get(remote_id) if remote_id else None
        if registered is not None:
            client_key, unit_id = registered
            client = self.clients.get(client_key)
            if client is None:
                _log.warning(f'Failed to {action} modbus registers: the client for remote {remote_id} is gone.')
            return client, int(message.pop('unit_id', unit_id))
        if device_address is None or transport_protocol is None:
            _log.warning(f'Failed to {action} modbus registers: unknown remote {remote_id} and no device named in the payload.')
            return None, 1
        return self._get_client(device_address, transport_protocol, message, action), int(message.pop('unit_id', 1))

    def _get_client(self, device_address: str, transport_protocol: str, message: dict, action: str
                    ) -> ModbusClient | None:
        client_key = self._get_client_key(device_address, transport_protocol, message)
        client = self.clients.get(client_key) if client_key else None
        if client is None:
            _log.warning(f"Failed to {action} modbus registers."
                         f" A {transport_protocol} client for device {device_address} was not found.")
        return client

    @staticmethod
    def _get_client_key(device_address: str, transport_protocol: str, message: dict) -> str:
        """Identify the client (i.e., the gateway connection) a message refers to. Empty for unknown transports."""
        transport = transport_protocol.lower().strip()
        match transport:
            case 'serial':
                return f'serial:{device_address}'
            case 'tcp' | 'udp':
                return f"{transport}:{device_address}:{message.get('port', 502)}"
            case 'tls':
                return f"tls:{device_address}:{message.get('port', 802)}"
            case _:
                _log.error(f"Unsupported transport protocol: {transport_protocol}")
                return ''

    @classmethod
    def get_unique_remote_id(cls, unique_remote_id: tuple) -> tuple:
        """Identify the proxy process a caller wants.

        Modbus clients bind no local resources, so by default every device shares one process and callers pass a
        constant. A site wanting several processes may append a group name; the tuple is used as-is.
        """
        return tuple(unique_remote_id)
