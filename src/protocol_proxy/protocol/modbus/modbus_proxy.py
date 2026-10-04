import asyncio
import json
from uuid import UUID
import logging
import sys


from typing import Iterable

from functools import partial

from protocol_proxy.ipc import callback, ProtocolProxyMessage
from protocol_proxy.proxy.asyncio import AsyncioProtocolProxy

from pymodbus import pymodbus_apply_logging_config

from .json import serialize
from .client import ModbusClient
from .server import ServedModbusDevice

_log = logging.getLogger(__name__)


ROLES = {'master': 'master', 'client': 'master', 'slave': 'slave', 'server': 'slave', 'outstation': 'slave'}


class ModbusProxy(AsyncioProtocolProxy):
    """One proxy process serves any number of Modbus devices over any mix of transports, and any number of Modbus TCP
    slaves served to remote masters, in either role as the registration says.

    Clients (one per gateway, keyed by transport, address and port) are created by REGISTER_REMOTE (or its older name
    REGISTER_DEVICE) with role ``master`` (the default); each holds the register data types of every unit behind it,
    declared via CONFIGURE_REGISTERS. Role ``slave`` (``server``) makes the proxy serve a unit instead: a pymodbus TCP
    server on ``bind_host:port`` whose registers hold the registered points. Reads and writes to a served unit are
    keyed by topic (READ_POINTS ``topics``, WRITE_POINTS ``values``) and the master's accepted writes are pushed as
    RECEIVE_WRITES, tagged with the caller's remote id. CLOSE_REMOTE stops a served unit.
    """
    # Requests to units sharing a gateway queue behind one another (see ModbusClient), so a request may legitimately
    # wait for several other units' timeout-and-retry cycles before it runs. This bounds how long the IPC layer
    # waits before abandoning it; on expiry the running request is cancelled and the gateway lock released.
    DEFAULT_CALLBACK_TIMEOUT = 120.0

    def __init__(self, callback_timeout: float = DEFAULT_CALLBACK_TIMEOUT, **kwargs):
        pymodbus_apply_logging_config('INFO')
        super(ModbusProxy, self).__init__(**kwargs)
        self.clients: dict[str, ModbusClient] = {}
        self.servers: dict[str, ServedModbusDevice] = {}        # served units, by listener and unit id
        # The caller's remote id (header version 2) -> (client key, unit id) it registered. Several remotes (unit ids)
        # share one client when they sit behind the same gateway connection. A served unit's key starts with 'served:'.
        self.remotes: dict[UUID, tuple[str, int]] = {}
        self.loop = asyncio.get_event_loop()

        for name in ('REGISTER_REMOTE', 'REGISTER_DEVICE'):
            self.register_callback(self.register_device_endpoint, name, provides_response=True, timeout=callback_timeout)
        self.register_callback(self.read_points_endpoint, 'READ_POINTS', provides_response=True, timeout=callback_timeout)
        self.register_callback(self.write_points_endpoint, 'WRITE_POINTS', provides_response=True, timeout=callback_timeout)
        self.register_callback(self.close_remote_endpoint, 'CLOSE_REMOTE', provides_response=True, timeout=callback_timeout)
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
        with the client and unit id; it is how every later request from that caller names them.
        """
        message = json.loads(raw_message.decode('utf8'))
        remote_id = getattr(headers, 'remote_id', None)
        if remote_id is None:
            return serialize({'results': {}, 'errors': {'device': 'Registration needs the remote id in its header (version 2).'}})
        role = ROLES.get(str(message.pop('role', None) or 'master').lower())
        if role is None:
            return serialize({'results': {}, 'errors': {'device': 'Unknown role; expected master or slave.'}})
        if role == 'slave':
            return await self._register_served(remote_id, message)
        if 'device_address' not in message:
            return serialize({'results': {}, 'errors': {'device': 'Registering a device to reach needs a device_address.'}})
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
            self.remotes[remote_id] = (client_key, int(configure_args.get('unit_id', 1)))
            results = {'client': client_key}
            if tables is not None:
                results.update(self.clients[client_key].configure_registers(tables, **configure_args))
            return serialize({'results': results, 'errors': {}})
        except Exception as e:
            _log.warning(f"Failed to register Modbus device {device_address}: {e}")
            return serialize({'results': {}, 'errors': {'device': str(e)}})

    @staticmethod
    def server_key(message: dict) -> str:
        return f"served:{message.get('bind_host', '0.0.0.0')}:{message.get('port', 502)}:{message.get('unit_id', 1)}"

    async def _register_served(self, remote_id: UUID, message: dict):
        """Serve a Modbus TCP unit (role ``slave``). Payload: ``bind_host``, ``port``, ``unit_id``, ``tables`` (per
        table, spec rows with ``topic``, ``remote_writable`` and ``initial_value``) and optionally ``values`` (the
        caller's current values for points nothing has written). Reply: ``{'server': key, 'configured': ..., 'seeded': n,
        'port': bound port}``; everything held is pushed as well."""
        key = self.server_key(message)
        served = self.servers.get(key)
        try:
            if served is None:
                served = ServedModbusDevice(message.get('bind_host', '0.0.0.0'), int(message.get('port', 502)),
                                            unit_id=int(message.get('unit_id', 1)))
                served.push = partial(self._push, served)
            served.remote_id = remote_id
            configured = served.configure_points(message.get('tables') or {})
            await served.start(message.get('values') or {})
        except Exception as e:      # a bad row, or whatever the listener's bind raised
            _log.warning(f'Failed to serve Modbus unit {key}: {e}')
            if served is not None and key not in self.servers:
                await served.close()
            return serialize({'results': {}, 'errors': {'device': str(e)}})
        self.servers[key] = served
        self.remotes[remote_id] = (key, served.unit_id)
        self.loop.create_task(served.push_all())
        return serialize({'results': {'server': key, 'configured': configured, 'seeded': served.seeded, 'port': served.bound_port},
                          'errors': {}})

    async def _push(self, served: ServedModbusDevice, values: dict):
        """Send a master's accepted writes to the manager, keyed by topic and tagged with the caller's remote id."""
        peer = self.peers.get(self.manager)
        if peer is None:    # pragma: no cover
            return
        await self.send(peer, ProtocolProxyMessage(method_name='RECEIVE_WRITES', payload=serialize(values),
                                                   remote_id=served.remote_id))

    def _served(self, headers, action: str) -> ServedModbusDevice | None:
        remote_id = getattr(headers, 'remote_id', None)
        registered = self.remotes.get(remote_id) if remote_id else None
        served = self.servers.get(registered[0]) if registered else None
        if served is None:
            _log.warning(f'Failed to {action}: remote {remote_id} has no served unit.')
        return served

    @callback
    async def read_points_endpoint(self, headers, raw_message: bytes):
        """The current served values of ``topics`` (all registered points when omitted)."""
        message = json.loads(raw_message.decode('utf8'))
        served = self._served(headers, 'read points')
        if served is None:
            return serialize({'results': {}, 'errors': {'device': 'No served unit for this remote.'}})
        results, errors = await served.read(message.get('topics'))
        return serialize({'results': results, 'errors': errors})

    @callback
    async def write_points_endpoint(self, headers, raw_message: bytes):
        """Store ``values`` (``{topic: value}``) in the served registers."""
        message = json.loads(raw_message.decode('utf8'))
        served = self._served(headers, 'write points')
        if served is None:
            return serialize({'results': {}, 'errors': {'device': 'No served unit for this remote.'}})
        results, errors = await served.write(message.get('values') or {})
        return serialize({'results': results, 'errors': errors})

    @callback
    async def close_remote_endpoint(self, headers, raw_message: bytes):
        remote_id = getattr(headers, 'remote_id', None)
        registered = self.remotes.pop(remote_id, None) if remote_id else None
        served = self.servers.pop(registered[0], None) if registered else None
        if served is not None:
            await served.close()
        return serialize({'results': {'closed': served is not None}, 'errors': {}})

    @callback
    async def configure_registers_endpoint(self, headers, raw_message: bytes):
        """Endpoint for declaring the data types of registers on a Modbus device the caller registered.

        Payload: tables, [unit_id] (default: the registered unit), [max_gap], [clear_others].
        tables maps a table name ('coil', 'discrete_input', 'holding', 'input') to a list of specs, each
        {address, data_type, [count], [word_order], [string_encoding]}. Pads are specs with data_type 'pad'.
        Each listed table is replaced whole. Other tables on the unit are kept unless clear_others is true.
        """
        message = json.loads(raw_message.decode('utf8'))
        client, unit_id = self._resolve(headers, message, 'configure')
        if client is None:
            return serialize({'results': {}, 'errors': {'device': 'Client not found'}})
        try:
            results = client.configure_registers(message.get('tables', {}),
                                                 unit_id=unit_id,
                                                 max_gap=message.get('max_gap'),
                                                 clear_others=message.get('clear_others', False))
            return serialize({'results': results, 'errors': {}})
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            _log.warning(f"Failed to configure Modbus registers for remote {getattr(headers, 'remote_id', None)}: {e}")
            return serialize({'results': {}, 'errors': {'configure': str(e)}})

    @callback
    async def read_registers_endpoint(self, headers, raw_message: bytes):
        """Endpoint for reading registers from a Modbus device.

        Payload: [queries], [register_map], [decode], [unit_id] (default: the registered unit). With decode true,
        values are decoded using the map declared via CONFIGURE_REGISTERS, and queries may be omitted to read the
        whole map in the fewest requests.
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

        Payload: queries, [register_map], [encode], [unit_id] (default: the registered unit). With encode true, each
        query's value is encoded using the map declared via CONFIGURE_REGISTERS.
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
        """The client and unit id a request concerns, by the remote id in its header.

        A payload ``unit_id`` overrides the registered one; stray identity fields in the payload are discarded so the
        remainder can be passed on as request options.
        """
        remote_id = getattr(headers, 'remote_id', None)
        for stale in ('device_address', 'device_type', 'port'):
            message.pop(stale, None)
        registered = self.remotes.get(remote_id) if remote_id else None
        if registered is None:
            _log.warning(f'Failed to {action} modbus registers: remote {remote_id} has no registered device.')
            return None, 1
        client_key, unit_id = registered
        client = self.clients.get(client_key)
        if client is None:
            _log.warning(f'Failed to {action} modbus registers: the client for remote {remote_id} is gone.')
        return client, int(message.pop('unit_id', unit_id))

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
