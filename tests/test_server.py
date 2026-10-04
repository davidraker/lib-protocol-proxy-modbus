"""The slave role: ModbusListeners on the loopback interface (TCP, UDP, TLS) and over a pseudo-terminal pair (serial),
exercised by pymodbus's own clients."""
import asyncio
import datetime
import ipaddress
import json
import os
import pty
import ssl
import tty
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient, AsyncModbusTlsClient, AsyncModbusUdpClient

from protocol_proxy.protocol.modbus.modbus_proxy import ModbusProxy
from protocol_proxy.protocol.modbus.server import ModbusListener, ServedPoint, ServedUnit


def make_proxy():
    return ModbusProxy(proxy_id=uuid4(), token=uuid4(), manager_address='127.0.0.1', manager_port=1, manager_id=uuid4(),
                       manager_token=uuid4(), registration_retry_delay=0, registration_timeout=0.05)


TABLES = {
    'holding': [{'topic': 's/CO2Stpt', 'address': 1003, 'data_type': '>f', 'initial_value': 1000.0},
                {'topic': 's/Name', 'address': 1005, 'data_type': 'string[6]', 'remote_writable': False},
                {'address': 1008, 'data_type': 'pad', 'count': 2},
                {'topic': 's/Mode', 'address': 1010, 'data_type': 'uint16', 'initial_value': 3}],
    'input': [{'topic': 's/ReturnAirCO2', 'address': 1001, 'data_type': '>f', 'initial_value': 412.5},
              {'topic': 's/Temp', 'address': 1010, 'data_type': 'int16'}],
    'coil': [{'topic': 's/FanCmd', 'address': 5, 'data_type': 'bool', 'initial_value': False},
             {'topic': 's/Locked', 'address': 6, 'data_type': 'bool', 'remote_writable': False, 'initial_value': True}],
    'discrete_input': [{'topic': 's/FanStatus', 'address': 3, 'data_type': 'bool', 'initial_value': True}],
}
SECOND_UNIT = {'holding': [{'topic': 'u2/Mode', 'address': 1010, 'data_type': 'uint16', 'initial_value': 9}]}


class Pushes:
    def __init__(self):
        self.values = []

    async def __call__(self, values):
        self.values.append(values)

    def latest(self, topic):
        for values in reversed(self.values):
            if topic in values:
                return values[topic]
        return None


async def wait_for(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


def served_unit(unit_id=7, tables=TABLES) -> ServedUnit:
    unit = ServedUnit(unit_id, push=Pushes())
    unit.configure_points(tables)
    unit.pushes = unit.push
    return unit


@pytest_asyncio.fixture
async def listener():
    lst = ModbusListener('tcp', '127.0.0.1', 0)
    lst.add_unit(served_unit())
    await lst.start()
    try:
        yield lst
    finally:
        await lst.stop()


@pytest_asyncio.fixture
async def served(listener):
    return listener.units[7]


@pytest_asyncio.fixture
async def master(listener):
    client = AsyncModbusTcpClient('127.0.0.1', port=listener.bound_port, timeout=3)
    assert await client.connect()
    try:
        yield client
    finally:
        client.close()


def test_served_point_defaults_and_rejections():
    p = ServedPoint.from_dict('holding', {'topic': 't', 'address': 10, 'data_type': 'float'})
    assert p.remote_writable and p.spec.count == 2 and p.initial_value is None
    assert ServedPoint.from_dict('input', {'topic': 't', 'address': 10, 'data_type': 'uint16'}).remote_writable is False
    assert ServedPoint.from_dict('coil', {'topic': 't', 'address': 1, 'data_type': 'bool', 'remote_writable': False}).remote_writable is False
    pad = ServedPoint.from_dict('holding', {'address': 10, 'data_type': 'pad', 'count': 3})
    assert pad.spec.is_pad and not pad.remote_writable
    with pytest.raises(ValueError, match='unknown Modbus table'):
        ServedPoint.from_dict('registers', {'topic': 't', 'address': 1, 'data_type': 'uint16'})
    unit = ServedUnit(1)
    with pytest.raises(ValueError, match='Invalid spec 1 for holding'):
        unit.configure_points({'holding': [{'topic': 'a', 'address': 1, 'data_type': 'uint16'}, {'topic': 'b', 'address': 1, 'data_type': 'uint16'}]})
    with pytest.raises(ValueError, match='registered twice'):
        unit.configure_points({'holding': [{'topic': 'a', 'address': 1, 'data_type': 'uint16'}], 'input': [{'topic': 'a', 'address': 1, 'data_type': 'uint16'}]})
    with pytest.raises(ValueError, match='unsupported transport'):
        ModbusListener('rtu-over-carrier-pigeon', 'x')
    assert ModbusListener.key('serial', '/dev/ttyUSB0', None) == 'served:serial:/dev/ttyUSB0'
    assert ModbusListener.key('tls', '0.0.0.0', None) == 'served:tls:0.0.0.0:802' and ModbusListener.key('udp', 'h', 7) == 'served:udp:h:7'


async def test_starting_values_reads_and_platform_writes(served):
    results, errors = await served.read(['s/CO2Stpt', 's/Mode', 's/ReturnAirCO2', 's/Temp', 's/FanCmd', 's/Locked', 's/FanStatus', 's/Name', 's/zz'])
    assert errors == {'s/zz': 'unregistered topic'}
    assert results['s/CO2Stpt'] == 1000.0 and results['s/Mode'] == 3 and results['s/ReturnAirCO2'] == 412.5
    assert results['s/Temp'] == 0 and results['s/FanCmd'] is False and results['s/Locked'] is True and results['s/FanStatus'] is True
    assert results['s/Name'] == ''
    results, errors = await served.write({'s/Temp': -12, 's/Name': 'Rtu-1', 's/FanStatus': False, 's/ReturnAirCO2': 450.25,
                                          's/Mode': 'seventy', 's/zz': 1})
    assert set(results) == {'s/Temp', 's/Name', 's/FanStatus', 's/ReturnAirCO2'}
    assert 'bad value' in errors['s/Mode'] and errors['s/zz'] == 'unregistered topic'
    results, _ = await served.read(['s/Temp', 's/Name', 's/FanStatus', 's/ReturnAirCO2'])
    assert results == {'s/Temp': -12, 's/Name': 'Rtu-1', 's/FanStatus': False, 's/ReturnAirCO2': 450.25}
    assert served.pushes.values == []                                   # the platform's own writes are not pushed


async def test_registration_values_beat_starting_values_but_not_held_ones():
    lst = ModbusListener('tcp', '127.0.0.1', 0)
    unit = served_unit()
    lst.add_unit(unit)
    await lst.start({7: {'s/Temp': 21, 's/Mode': 9, 's/zz': 1, 's/Name': None}})
    try:
        assert unit.seeded == 2
        results, _ = await unit.read(['s/Temp', 's/Mode', 's/CO2Stpt'])
        assert results == {'s/Temp': 21, 's/Mode': 9, 's/CO2Stpt': 1000.0}         # seed beats the starting value
        await unit.write({'s/Temp': 5})
        await lst.start({7: {'s/Temp': 99, 's/Mode': 1}})                          # a restart of the caller: held values win
        results, _ = await unit.read(['s/Temp', 's/Mode'])
        assert results == {'s/Temp': 5, 's/Mode': 9} and unit.seeded == 0
        table = await unit.push_all()
        assert table['s/Temp'] == 5 and 's/Name' not in table                      # never written: not in the table
    finally:
        await lst.stop()


async def test_master_reads_and_writes(served, master):
    r = await master.read_holding_registers(1003, count=2, device_id=7)
    assert master.convert_from_registers(r.registers, master.DATATYPE.FLOAT32) == 1000.0
    r = await master.read_input_registers(1001, count=2, device_id=7)
    assert master.convert_from_registers(r.registers, master.DATATYPE.FLOAT32) == 412.5
    r = await master.read_coils(5, count=2, device_id=7)
    assert r.bits[:2] == [False, True]
    r = await master.read_discrete_inputs(3, count=1, device_id=7)
    assert r.bits[0] is True
    r = await master.read_holding_registers(1003, count=8, device_id=7)   # across the pad: served, not refused
    assert len(r.registers) == 8
    r = await master.read_holding_registers(1020, count=1, device_id=7)
    assert r.isError()                                                   # outside the served block
    r = await master.read_holding_registers(1003, count=1, device_id=9)
    assert r.isError() and r.exception_code == 2                          # a unit nobody serves: the sentinel answers

    r = await master.write_registers(1003, master.convert_to_registers(950.5, master.DATATYPE.FLOAT32), device_id=7)
    assert not r.isError()
    r = await master.write_register(1010, 4, device_id=7)
    assert not r.isError()
    r = await master.write_coil(5, True, device_id=7)
    assert not r.isError()
    assert await wait_for(lambda: served.pushes.latest('s/FanCmd') is True)
    assert served.pushes.latest('s/CO2Stpt') == 950.5 and served.pushes.latest('s/Mode') == 4
    results, _ = await served.read(['s/CO2Stpt', 's/Mode', 's/FanCmd'])
    assert results == {'s/CO2Stpt': 950.5, 's/Mode': 4, 's/FanCmd': True}

    r = await master.write_registers(1005, [0x4142], device_id=7)        # Name is not remote-writable
    assert r.isError() and r.exception_code == 2
    r = await master.write_coil(6, False, device_id=7)                    # Locked is not remote-writable
    assert r.isError()
    r = await master.write_register(1008, 1, device_id=7)                 # a pad
    assert r.isError()
    r = await master.write_register(1015, 1, device_id=7)                 # unregistered but inside the block
    assert r.isError()
    results, _ = await served.read(['s/Name', 's/Locked'])
    assert results == {'s/Name': '', 's/Locked': True}


async def test_several_units_share_one_listener(listener, master):
    second = served_unit(2, SECOND_UNIT)
    listener.add_unit(second)
    await listener.start()                                                # rebuilt with both units; unit 7 keeps its values
    assert master.connected or await master.connect()
    r = await master.read_holding_registers(1010, count=1, device_id=2)
    assert r.registers == [9]
    r = await master.read_holding_registers(1010, count=1, device_id=7)
    assert r.registers == [3]
    r = await master.write_register(1010, 11, device_id=2)
    assert not r.isError() and await wait_for(lambda: second.pushes.latest('u2/Mode') == 11)
    assert listener.units[7].pushes.latest('u2/Mode') is None             # pushed to the unit that owns the point
    assert listener.remove_unit(2) is second
    await listener.start()
    r = await master.read_holding_registers(1010, count=1, device_id=2)
    assert r.isError()                                                    # gone: the sentinel answers


async def test_reconfiguration_keeps_values_of_points_that_stay(listener, served):
    await served.write({'s/Temp': 5})
    served.configure_points({'input': TABLES['input'] + [{'topic': 's/New', 'address': 1020, 'data_type': 'uint16', 'initial_value': 8}]})
    await listener.start()
    results, errors = await served.read(['s/Temp', 's/New', 's/ReturnAirCO2', 's/CO2Stpt'])
    assert results == {'s/Temp': 5, 's/New': 8, 's/ReturnAirCO2': 412.5} and errors == {'s/CO2Stpt': 'unregistered topic'}


async def test_udp_listener():
    lst = ModbusListener('udp', '127.0.0.1', 0)
    lst.add_unit(served_unit())
    await lst.start()
    try:
        assert lst.bound_port
        client = AsyncModbusUdpClient('127.0.0.1', port=lst.bound_port, timeout=3)
        await client.connect()
        r = await client.read_holding_registers(1010, count=1, device_id=7)
        assert r.registers == [3]
        r = await client.write_register(1010, 6, device_id=7)
        assert not r.isError() and await wait_for(lambda: lst.units[7].pushes.latest('s/Mode') == 6)
        client.close()
    finally:
        await lst.stop()


def self_signed(tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'modbus-test')])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
            .sign(key, hashes.SHA256()))
    cert_path, key_path = tmp_path / 'server.pem', tmp_path / 'server.key'
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    return cert_path, key_path


async def test_tls_listener(tmp_path):
    cert_path, key_path = self_signed(tmp_path)
    lst = ModbusListener('tls', '127.0.0.1', 0, cert_path=str(cert_path), key_path=str(key_path))
    lst.add_unit(served_unit())
    await lst.start()
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(cert_path)
        client = AsyncModbusTlsClient('127.0.0.1', port=lst.bound_port, sslctx=ctx, timeout=3)
        assert await client.connect()
        r = await client.read_holding_registers(1010, count=1, device_id=7)
        assert r.registers == [3]
        r = await client.write_register(1010, 12, device_id=7)
        assert not r.isError() and await wait_for(lambda: lst.units[7].pushes.latest('s/Mode') == 12)
        client.close()
    finally:
        await lst.stop()
    with pytest.raises(ValueError, match='cert_path'):
        bare = ModbusListener('tls', '127.0.0.1', 0)
        bare.add_unit(served_unit())
        await bare.start()


class PtyBridge:
    """Two pseudo-terminal pairs with their master sides pumped into each other: a serial cable with a device at each end."""

    def __init__(self):
        self.a_master, a_slave = pty.openpty()
        self.b_master, b_slave = pty.openpty()
        for fd in (self.a_master, a_slave, self.b_master, b_slave):
            tty.setraw(fd)
        self.a_name, self.b_name = os.ttyname(a_slave), os.ttyname(b_slave)
        self.loop = asyncio.get_running_loop()
        self.loop.add_reader(self.a_master, self._pump, self.a_master, self.b_master)
        self.loop.add_reader(self.b_master, self._pump, self.b_master, self.a_master)

    def _pump(self, source, sink):
        try:
            data = os.read(source, 4096)
        except OSError:
            return
        if data:
            os.write(sink, data)

    def close(self):
        for fd in (self.a_master, self.b_master):
            self.loop.remove_reader(fd)
            os.close(fd)


async def test_serial_listener():
    bridge = PtyBridge()
    lst = ModbusListener('serial', bridge.a_name, baudrate=19200)
    lst.add_unit(served_unit())
    try:
        await lst.start()
        assert lst.bound_port is None and lst.describe()['transport'] == 'serial'
        client = AsyncModbusSerialClient(bridge.b_name, baudrate=19200, timeout=3)
        assert await client.connect()
        r = await client.read_holding_registers(1010, count=1, device_id=7)
        assert r.registers == [3]
        r = await client.write_register(1010, 21, device_id=7)
        assert not r.isError() and await wait_for(lambda: lst.units[7].pushes.latest('s/Mode') == 21)
        r = await client.read_input_registers(1001, count=2, device_id=7)
        assert client.convert_from_registers(r.registers, client.DATATYPE.FLOAT32) == 412.5
        client.close()
    finally:
        await lst.stop()
        bridge.close()


async def test_proxy_serves_and_closes_units():
    proxy = make_proxy()
    pushes = []

    async def fake_push(unit, values):
        pushes.append((unit.remote_id, values))
    proxy._push = fake_push
    remote, remote2 = uuid4(), uuid4()

    async def call(endpoint, message, remote=remote):
        reply = await endpoint.__wrapped__(proxy, SimpleNamespace(remote_id=remote), json.dumps(message).encode('utf8'))
        return json.loads(reply)
    reply = await call(proxy.register_device_endpoint, {'role': 'slave', 'bind_host': '127.0.0.1', 'port': 0, 'unit_id': 7,
                                                        'tables': TABLES, 'values': {'s/Temp': 11, 's/Mode': 1}})
    assert reply['error'] == {}, reply
    key = 'served:tcp:127.0.0.1:0'
    assert reply['result']['server'] == key and reply['result']['unit_id'] == 7 and reply['result']['seeded'] == 2
    assert reply['result']['port'] > 0 and reply['result']['configured']['holding'] == {'points': 3, 'specs': 4}
    listener = proxy.listeners[key]
    unit = listener.units[7]
    assert proxy.remotes[remote] == (key, 7)
    await asyncio.sleep(0.05)
    assert pushes and pushes[0][0] == remote and pushes[0][1]['s/Temp'] == 11 and pushes[0][1]['s/Mode'] == 1
    reply = await call(proxy.read_points_endpoint, {'topics': ['s/Temp']})
    assert reply == {'result': {'s/Temp': 11}, 'error': {}}
    reply = await call(proxy.write_points_endpoint, {'values': {'s/Temp': 12}})
    assert reply['result']['s/Temp']['status'] == 'stored'
    # A second unit on the same endpoint shares the listener (port 0 resolves to the bound port in the message).
    reply = await call(proxy.register_device_endpoint, {'role': 'server', 'bind_host': '127.0.0.1', 'port': 0, 'unit_id': 2,
                                                        'tables': SECOND_UNIT}, remote=remote2)
    assert reply['error'] == {} and proxy.listeners[key] is listener and set(listener.units) == {7, 2}
    assert (await call(proxy.read_points_endpoint, {'topics': ['s/Temp']}))['result'] == {'s/Temp': 12}      # unit 7 kept its value
    assert (await call(proxy.read_points_endpoint, {'topics': ['u2/Mode']}, remote=remote2))['result'] == {'u2/Mode': 9}
    # Re-registering unit 7 with a changed table replaces its points and keeps the listener.
    reply = await call(proxy.register_device_endpoint, {'role': 'slave', 'bind_host': '127.0.0.1', 'port': 0, 'unit_id': 7,
                                                        'tables': {'input': TABLES['input']}})
    assert reply['error'] == {} and proxy.listeners[key] is listener and set(listener.units[7].points) == {'s/ReturnAirCO2', 's/Temp'}
    # Bad registrations leave the listener and its other units alone.
    reply = await call(proxy.register_device_endpoint, {'role': 'sideways'})
    assert 'Unknown role' in reply['error']['device']
    reply = await call(proxy.register_device_endpoint, {'role': 'slave', 'bind_host': '127.0.0.1', 'port': 0, 'unit_id': 3,
                                                        'tables': {'holding': [{'topic': 'x', 'address': 1, 'data_type': 'nope'}]}}, remote=uuid4())
    assert 'Invalid spec' in reply['error']['device'] and set(listener.units) == {7, 2} and listener.server is not None
    reply = await call(proxy.register_device_endpoint, {'role': 'slave', 'bind_host': '127.0.0.1', 'port': 1, 'unit_id': 1, 'tables': {}},
                       remote=uuid4())
    assert reply['error']['device'] and len(proxy.listeners) == 1                   # a privileged port: bind fails
    reply = await call(proxy.register_device_endpoint, {'role': 'slave', 'transport_protocol': 'serial', 'unit_id': 1, 'tables': {}},
                       remote=uuid4())
    assert 'device_address' in reply['error']['device']
    reply = await call(proxy.read_points_endpoint, {'topics': ['s/Temp']}, remote=uuid4())
    assert 'No served unit' in reply['error']['device']
    # Closing one unit keeps the listener; closing the last stops it.
    reply = await call(proxy.close_remote_endpoint, {}, remote=remote2)
    assert reply['result'] == {'closed': True} and set(listener.units) == {7} and listener.server is not None
    reply = await call(proxy.close_remote_endpoint, {})
    assert reply['result'] == {'closed': True} and proxy.listeners == {} and listener.server is None and remote not in proxy.remotes
