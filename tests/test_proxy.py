"""Tests for the ModbusProxy IPC endpoints and the JSON response serializer."""
import asyncio
import json
from types import SimpleNamespace
from enum import Enum
from uuid import uuid4

import pytest

from protocol_proxy.protocol.modbus import json as modbus_json
from protocol_proxy.protocol.modbus.modbus_proxy import ModbusProxy

from conftest import FakeDevice, FakeResponse, SAMPLE_REGISTERS

DEVICE = dict(device_address='10.0.0.4', device_type='tcp', port=1502)
CLIENT_KEY = 'tcp:10.0.0.4:1502'


class TestSerialize:
    def test_results_and_errors_split(self):
        out = json.loads(modbus_json.serialize({'results': {1001: 1.5, 3: [True, False]},
                                                'errors': {1100: FakeResponse(error=True)}}))
        assert out == {'result': {'1001': 1.5, '3': [True, False]}, 'error': {'1100': 'FakeResponse(error=True)'}}

    def test_lists_preserved(self):
        out = json.loads(modbus_json.serialize({'results': [None, [1, 2]], 'errors': ['bad', None]}))
        assert out == {'result': [None, [1, 2]], 'error': ['bad', None]}

    def test_plain_value_is_a_result(self):
        class Color(Enum):
            RED = 1
        out = json.loads(modbus_json.serialize({'configured': {'holding': {'blocks': [[1, 2]]}}, 'color': Color.RED,
                                                'raw': b'\x01\x02', 'nested': (1, {2, 3})}))
        assert out['result']['configured']['holding']['blocks'] == [[1, 2]]
        assert out['result']['color'] == 'RED' and out['result']['raw'] == '0102'
        assert out['result']['nested'][0] == 1 and sorted(out['result']['nested'][1]) == [2, 3]
        assert out['error'] == {}

    def test_unserializable_becomes_error(self):
        class Bad:
            def __str__(self):
                raise RuntimeError('nope')
        out = json.loads(modbus_json.serialize(Bad()))
        assert out['error']['error'] == 'SerializationError'


def make_proxy():
    """Construct a real ModbusProxy. Must be called inside a running event loop (the IPC layer requires one)."""
    return ModbusProxy(proxy_id=uuid4(), token=uuid4(), manager_address='127.0.0.1', manager_port=1,
                       manager_id=uuid4(), manager_token=uuid4(), registration_retry_delay=0,
                       registration_timeout=0.05)


async def call(proxy, name, remote=None, **message):
    """Invoke an endpoint the way the IPC layer would after authenticating the caller, and decode its reply.
    ``remote`` is the header's remote id (version 2); None is a version 1 caller naming the device in the payload."""
    endpoint = {'REGISTER_DEVICE': proxy.register_device_endpoint,
                'CONFIGURE_REGISTERS': proxy.configure_registers_endpoint,
                'READ_REGISTERS': proxy.read_registers_endpoint,
                'WRITE_REGISTERS': proxy.write_registers_endpoint}[name]
    inner = endpoint.__wrapped__                     # skip the @callback peer-authentication wrapper
    reply = await inner(proxy, SimpleNamespace(remote_id=remote), json.dumps(message).encode('utf8'))
    return None if reply is None else json.loads(reply)


def test_endpoints_are_registered_with_generous_timeout():
    async def main():
        proxy = make_proxy()
        names = {'REGISTER_DEVICE', 'CONFIGURE_REGISTERS', 'READ_REGISTERS', 'WRITE_REGISTERS'}
        assert names <= set(proxy.callbacks)
        # Requests queue behind other units on a shared gateway, so the IPC limit must outlast several device
        # timeout-and-retry cycles rather than the 30 s default.
        assert all(proxy.callbacks[n].timeout == ModbusProxy.DEFAULT_CALLBACK_TIMEOUT == 120.0 for n in names)
        custom = ModbusProxy(callback_timeout=7.5, proxy_id=uuid4(), token=uuid4(), manager_address='127.0.0.1',
                             manager_port=1, manager_id=uuid4(), manager_token=uuid4(), registration_retry_delay=0)
        assert custom.callbacks['READ_REGISTERS'].timeout == 7.5
    asyncio.run(main())


def test_register_device_creates_client_and_configures_map():
    async def main():
        proxy = make_proxy()
        reply = await call(proxy, 'REGISTER_DEVICE', **DEVICE, timeout=1, unit_id=1,
                           tables={'holding': [{'address': 1001, 'data_type': '>f'}, {'address': 1003, 'data_type': '>f'}]})
        assert reply['error'] == {}
        assert reply['result']['client'] == CLIENT_KEY
        assert reply['result']['configured']['holding']['blocks'] == [[1001, 4]]
        client = proxy.clients[CLIENT_KEY]
        assert type(client.client).__name__ == 'AsyncModbusTcpClient'
        assert client.client.comm_params.port == 1502
        # Registering again is idempotent and does not replace the client.
        again = await call(proxy, 'REGISTER_DEVICE', **DEVICE)
        assert again['error'] == {} and proxy.clients[CLIENT_KEY] is client
    asyncio.run(main())


def test_register_device_rejects_unknown_transport():
    async def main():
        proxy = make_proxy()
        reply = await call(proxy, 'REGISTER_DEVICE', device_address='x', device_type='carrier-pigeon')
        assert 'Unsupported device type' in reply['error']['device'] and proxy.clients == {}
    asyncio.run(main())


def test_configure_registers_endpoint():
    async def main():
        proxy = make_proxy()
        reply = await call(proxy, 'CONFIGURE_REGISTERS', **DEVICE, tables={'holding': []})
        assert reply['error'] == {'device': 'Client not found'}

        await call(proxy, 'REGISTER_DEVICE', **DEVICE, tables={'holding': [{'address': 1001, 'data_type': '>f'}]})
        client = proxy.clients[CLIENT_KEY]

        reply = await call(proxy, 'CONFIGURE_REGISTERS', **DEVICE,
                           tables={'coil': [{'address': 7, 'data_type': 'bool', 'count': 2}]})
        assert reply['error'] == {} and reply['result']['configured']['coil']['specs'] == 1
        assert (1, 'holding') in client.register_maps            # unlisted table kept

        reply = await call(proxy, 'CONFIGURE_REGISTERS', **DEVICE,
                           tables={'holding': [{'address': 1001, 'data_type': 'uint32'}, {'address': 1002, 'data_type': 'uint16'}]})
        assert 'Invalid spec 1 for holding' in reply['error']['configure']
        assert client.get_register_map('holding').get(1001).count == 2   # untouched by the failed call

        reply = await call(proxy, 'CONFIGURE_REGISTERS', **DEVICE, clear_others=True, max_gap=2,
                           tables={'input': [{'address': 1, 'data_type': 'uint16', 'word_order': 'little'}]})
        assert sorted(reply['result']['cleared']) == ['coil', 'holding']
        assert set(client.register_maps) == {(1, 'input')}
        assert client.get_register_map('input').max_gap == 2
    asyncio.run(main())


def test_read_and_write_endpoints():
    async def main():
        proxy = make_proxy()
        await call(proxy, 'REGISTER_DEVICE', **DEVICE, tables={
            'holding': [{'address': 1001, 'data_type': '>f'}, {'address': 1003, 'data_type': '>f'},
                        {'address': 1005, 'data_type': 'pad', 'count': 5}, {'address': 1010, 'data_type': 'uint16'}]})
        client = proxy.clients[CLIENT_KEY]
        client.client = device = FakeDevice(registers=SAMPLE_REGISTERS)    # swap the real transport for a fake

        reply = await call(proxy, 'READ_REGISTERS', **DEVICE, decode=True)
        assert reply == {'result': {'1001': 1.0, '1003': 2.0, '1010': 42}, 'error': {}}
        assert device.requests() == [(1001, 10)]

        reply = await call(proxy, 'READ_REGISTERS', **DEVICE, queries=[[1010, 1]])
        assert reply == {'result': [[42]], 'error': [None]}

        reply = await call(proxy, 'READ_REGISTERS', **DEVICE)
        assert reply == {'result': [], 'error': ['Queries are required unless decode is True.']}

        reply = await call(proxy, 'READ_REGISTERS', **DEVICE, register_map='input', decode=True)
        assert 'No register data types' in reply['error']['read']

        reply = await call(proxy, 'WRITE_REGISTERS', **DEVICE, queries=[[1001, None, 3.0]], encode=True)
        assert reply == {'result': [{'address': 1001, 'count': 2}], 'error': [None]}
        assert (device.registers[1001], device.registers[1002]) == (0x4040, 0)

        reply = await call(proxy, 'WRITE_REGISTERS', **DEVICE, queries=[[1010, 1, [5]]])
        assert reply == {'result': [{'address': 1010, 'count': 1}], 'error': [None]}

        assert await call(proxy, 'READ_REGISTERS', device_address='9.9.9.9', device_type='tcp') is None
        assert await call(proxy, 'WRITE_REGISTERS', device_address='9.9.9.9', device_type='tcp', queries=[]) is None
    asyncio.run(main())


def test_get_client_key_includes_transport():
    assert ModbusProxy._get_client_key('h', 'tcp', {}) == 'tcp:h:502'
    assert ModbusProxy._get_client_key('h', 'udp', {}) == 'udp:h:502'          # distinct from the tcp client
    assert ModbusProxy._get_client_key('h', 'tls', {}) == 'tls:h:802'
    assert ModbusProxy._get_client_key('h', 'UDP ', {'port': 9}) == 'udp:h:9'
    assert ModbusProxy._get_client_key('/dev/ttyUSB0', 'serial', {}) == 'serial:/dev/ttyUSB0'
    assert ModbusProxy._get_client_key('h', 'nope', {}) == ''


def test_unique_remote_id_is_passed_through():
    """The manager uses this to decide whether to launch a new process; callers pass a constant, or add a group."""
    assert ModbusProxy.get_unique_remote_id(('modbus',)) == ('modbus',)
    assert ModbusProxy.get_unique_remote_id(['modbus', 'plant-b']) == ('modbus', 'plant-b')


def test_proxy_launches_through_generic_entry_point_without_options():
    from protocol_proxy.proxy.launch import resolve_launcher, proxy_command_parser
    import protocol_proxy.protocol.modbus as package
    assert ModbusProxy.LAUNCHER is None and ModbusProxy.PATCH_GEVENT is False
    assert package.PROXY_CLASS is ModbusProxy
    parser, runner = resolve_launcher('protocol_proxy.protocol.modbus.modbus_proxy:ModbusProxy')(proxy_command_parser())
    assert {a.dest for a in parser._actions} == {a.dest for a in proxy_command_parser()._actions}   # nothing added


def test_remote_id_in_the_header_identifies_the_client_and_unit(monkeypatch):
    """A version 2 caller registers once with its connection details; later requests carry only the request."""
    async def main():
        proxy = make_proxy()
        created = {}

        class FakeClient:
            def __init__(self):
                self.calls = []

            def configure_registers(self, tables, **kwargs):
                return {'configured': {t: {'specs': len(v)} for t, v in tables.items()}}

            async def read(self, queries, register_map, unit_id, decode=False, **kwargs):
                self.calls.append(('read', queries, register_map, unit_id, decode, kwargs))
                return {'results': {'1': 7}, 'errors': {}}

            async def write(self, queries, register_map, unit_id, encode=False, **kwargs):
                self.calls.append(('write', queries, register_map, unit_id, encode, kwargs))
                return {'results': [None], 'errors': [None]}

        async def create(device_address, transport_protocol, **kwargs):
            created['args'] = (device_address, transport_protocol, kwargs)
            return FakeClient()
        from protocol_proxy.protocol.modbus import modbus_proxy
        monkeypatch.setattr(modbus_proxy.ModbusClient, 'create', staticmethod(create))
        remote_a, remote_b = uuid4(), uuid4()
        reply = await call(proxy, 'REGISTER_DEVICE', remote=remote_a, device_address='10.0.0.4', device_type='tcp', port=502,
                           unit_id=3, tables={'holding': [{'address': 1, 'data_type': 'UINT16', 'count': 1}]})
        assert reply['error'] == {} and proxy.remotes[remote_a] == ('tcp:10.0.0.4:502', 3)
        reply = await call(proxy, 'REGISTER_DEVICE', remote=remote_b, device_address='10.0.0.4', device_type='tcp', port=502, unit_id=4)
        assert proxy.remotes[remote_b] == ('tcp:10.0.0.4:502', 4) and len(proxy.clients) == 1        # one gateway connection
        client = proxy.clients['tcp:10.0.0.4:502']
        reply = await call(proxy, 'READ_REGISTERS', remote=remote_a, queries=[[1, 1]], register_map='holding', decode=True)
        assert reply['result'] == {'1': 7} and client.calls[-1][:4] == ('read', [[1, 1]], 'holding', 3)
        await call(proxy, 'WRITE_REGISTERS', remote=remote_b, queries=[[1, None, 5]], register_map='holding', encode=True)
        assert client.calls[-1][:4] == ('write', [[1, None, 5]], 'holding', 4)
        await call(proxy, 'READ_REGISTERS', remote=remote_b, queries=[[1, 1]], unit_id=9)       # a payload unit id overrides
        assert client.calls[-1][3] == 9
        assert await call(proxy, 'READ_REGISTERS', remote=uuid4(), queries=[[1, 1]]) is None   # unknown remote, no device named
        # A version 1 caller still names the device in the payload.
        await call(proxy, 'READ_REGISTERS', device_address='10.0.0.4', device_type='tcp', port=502, unit_id=2, queries=[[1, 1]])
        assert client.calls[-1][3] == 2
    asyncio.run(main())
