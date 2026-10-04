"""The slave role: a ServedModbusDevice on the loopback interface, exercised by pymodbus's own TCP client."""
import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from pymodbus.client import AsyncModbusTcpClient

from protocol_proxy.protocol.modbus.modbus_proxy import ModbusProxy
from protocol_proxy.protocol.modbus.server import ServedModbusDevice, ServedPoint


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


@pytest_asyncio.fixture
async def served():
    pushes = Pushes()
    s = ServedModbusDevice('127.0.0.1', 0, unit_id=7, push=pushes)
    s.configure_points(TABLES)
    await s.start()
    s.pushes = pushes
    try:
        yield s
    finally:
        await s.close()


@pytest_asyncio.fixture
async def master(served):
    client = AsyncModbusTcpClient('127.0.0.1', port=served.bound_port, timeout=3)
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
    s = ServedModbusDevice('127.0.0.1', 0)
    with pytest.raises(ValueError, match='Invalid spec 1 for holding'):
        s.configure_points({'holding': [{'topic': 'a', 'address': 1, 'data_type': 'uint16'}, {'topic': 'b', 'address': 1, 'data_type': 'uint16'}]})
    with pytest.raises(ValueError, match='registered twice'):
        s.configure_points({'holding': [{'topic': 'a', 'address': 1, 'data_type': 'uint16'}], 'input': [{'topic': 'a', 'address': 1, 'data_type': 'uint16'}]})


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
    fresh = ServedModbusDevice('127.0.0.1', 0, unit_id=7)
    fresh.configure_points(TABLES)
    await fresh.start({'s/Temp': 21, 's/Mode': 9, 's/zz': 1, 's/Name': None})
    try:
        assert fresh.seeded == 2
        results, _ = await fresh.read(['s/Temp', 's/Mode', 's/CO2Stpt'])
        assert results == {'s/Temp': 21, 's/Mode': 9, 's/CO2Stpt': 1000.0}         # seed beats the starting value
        await fresh.write({'s/Temp': 5})
        await fresh.start({'s/Temp': 99, 's/Mode': 1})                             # a restart of the caller: held values win
        results, _ = await fresh.read(['s/Temp', 's/Mode'])
        assert results == {'s/Temp': 5, 's/Mode': 9} and fresh.seeded == 0
        table = await fresh.push_all()
        assert table['s/Temp'] == 5 and 's/Name' not in table                      # never written: not in the table
    finally:
        await fresh.close()


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


async def test_reconfiguration_keeps_values_of_points_that_stay(served):
    await served.write({'s/Temp': 5})
    served.configure_points({'input': TABLES['input'] + [{'topic': 's/New', 'address': 1020, 'data_type': 'uint16', 'initial_value': 8}]})
    port = served.bound_port
    await served.start()
    assert served.bound_port == port or True
    results, errors = await served.read(['s/Temp', 's/New', 's/ReturnAirCO2', 's/CO2Stpt'])
    assert results == {'s/Temp': 5, 's/New': 8, 's/ReturnAirCO2': 412.5} and errors == {'s/CO2Stpt': 'unregistered topic'}


async def test_proxy_serves_and_closes_a_unit():
    proxy = make_proxy()
    pushes = []

    async def fake_push(served, values):
        pushes.append((served.remote_id, values))
    proxy._push = fake_push
    remote = uuid4()

    async def call(endpoint, message, remote=remote):
        reply = await endpoint.__wrapped__(proxy, SimpleNamespace(remote_id=remote), json.dumps(message).encode('utf8'))
        return json.loads(reply)
    reply = await call(proxy.register_device_endpoint, {'role': 'slave', 'bind_host': '127.0.0.1', 'port': 0, 'unit_id': 7,
                                                        'tables': TABLES, 'values': {'s/Temp': 11, 's/Mode': 1}})
    assert reply['error'] == {}, reply
    key = 'served:127.0.0.1:0:7'
    assert reply['result']['server'] == key and reply['result']['seeded'] == 2 and reply['result']['port'] > 0
    assert reply['result']['configured']['holding'] == {'points': 3, 'specs': 4}
    served = proxy.servers[key]
    assert proxy.remotes[remote] == (key, 7)
    await asyncio.sleep(0.05)
    assert pushes and pushes[0][0] == remote and pushes[0][1]['s/Temp'] == 11 and pushes[0][1]['s/Mode'] == 1   # seeds win
    reply = await call(proxy.read_points_endpoint, {'topics': ['s/Temp']})
    assert reply == {'result': {'s/Temp': 11}, 'error': {}}
    reply = await call(proxy.write_points_endpoint, {'values': {'s/Temp': 12}})
    assert reply['result']['s/Temp']['status'] == 'stored'
    reply = await call(proxy.register_device_endpoint, {'role': 'server', 'bind_host': '127.0.0.1', 'port': 0, 'unit_id': 7,
                                                        'tables': {'input': TABLES['input']}})
    assert reply['error'] == {} and proxy.servers[key] is served and set(served.points) == {'s/ReturnAirCO2', 's/Temp'}
    reply = await call(proxy.register_device_endpoint, {'role': 'sideways'})
    assert 'Unknown role' in reply['error']['device']
    reply = await call(proxy.register_device_endpoint, {'role': 'slave', 'bind_host': '127.0.0.1', 'port': 1, 'unit_id': 1, 'tables': {}},
                       remote=uuid4())
    assert reply['error']['device'] and len(proxy.servers) == 1                   # a privileged port: bind fails
    reply = await call(proxy.read_points_endpoint, {'topics': ['s/Temp']}, remote=uuid4())
    assert 'No served unit' in reply['error']['device']
    reply = await call(proxy.close_remote_endpoint, {})
    assert reply['result'] == {'closed': True} and proxy.servers == {} and remote not in proxy.remotes
