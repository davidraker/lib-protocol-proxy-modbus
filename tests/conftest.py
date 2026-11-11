"""Shared fixtures for the Modbus proxy test suite."""
import asyncio
import inspect

import pytest

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusIOException

from protocol_proxy.protocol.modbus.client import ModbusClient


class FakeResponse:
    """Minimal stand-in for a pymodbus PDU."""

    def __init__(self, registers=None, bits=None, address=None, count=None, error=False, exception_code=None, **fields):
        self.registers = registers if registers is not None else []
        self.bits = bits if bits is not None else []
        self.address = address
        self.count = count
        self.error = error
        if exception_code is not None:
            self.exception_code = exception_code      # a Modbus exception response; 1 is ILLEGAL FUNCTION
        for name, value in fields.items():            # the fields of other replies (status, information, message ...)
            setattr(self, name, value)

    def isError(self):
        return self.error

    def __repr__(self):
        return f'FakeResponse(error={self.error})' if self.error else f'FakeResponse({self.registers or self.bits})'


class FakeDevice:
    """Stands in for a pymodbus async client.

    Every request is bound against the real pymodbus method signature, so passing an argument pymodbus would reject
    raises TypeError here too. Reads are served from (and writes applied to) simple address->value dicts. Any read
    touching an address in `failing` returns an error response.
    """

    def __init__(self, registers: dict[int, int] | None = None, coils: dict[int, bool] | None = None,
                 failing: set[int] = frozenset(), unreachable: set[int] = frozenset(), delay: float = 0.0,
                 events: list | None = None):
        self.registers = dict(registers or {})
        self.coils = dict(coils or {})
        self.failing = set(failing)          # addresses the device rejects with an exception response
        self.unreachable = set(unreachable)  # addresses whose requests time out (no answer at all)
        self.delay = delay                   # simulated request duration
        self.events = events if events is not None else []
        self.unsupported: set[str] = set()   # pymodbus method names the device answers with ILLEGAL FUNCTION
        self.calls: list[tuple[str, dict]] = []
        self.connections = 0            # connect() calls
        self.connected = False
        self.connectable = True
        self.closed = 0
        self.stale = False              # half-open: requests get no answer until the client reconnects

    async def connect(self) -> bool:
        self.connections += 1
        self.connected = self.connectable
        if self.connected:
            self.stale = False
        return self.connected

    def close(self):
        self.connected = False
        self.closed += 1

    def __getattr__(self, name):
        real = getattr(AsyncModbusTcpClient, name)   # AttributeError for anything pymodbus lacks

        async def method(*args, **kwargs):
            bound = inspect.signature(real).bind(self, *args, **kwargs).arguments
            bound.pop('self')
            self.calls.append((name, bound))
            if self.stale:
                raise ModbusIOException('No response received after 3 retries')
            if self.delay:
                self.events.append(('start', id(self), bound.get('address')))
                await asyncio.sleep(self.delay)
                self.events.append(('end', id(self), bound.get('address')))
            return self._respond(name, bound)
        return method

    def _respond(self, name, bound):
        address = bound.get('address', 0)
        if name in self.unsupported:
            return FakeResponse(error=True, exception_code=1)
        if name in ('read_holding_registers', 'read_input_registers'):
            span = range(address, address + bound['count'])
            if self.unreachable & set(span):
                raise ModbusIOException('No response received after 3 retries')
            if self.failing & set(span):
                return FakeResponse(error=True)
            return FakeResponse(registers=[self.registers.get(a, 0) for a in span])
        if name in ('read_coils', 'read_discrete_inputs'):
            return FakeResponse(bits=[self.coils.get(a, False) for a in range(address, address + bound['count'])])
        if name == 'write_registers':
            for offset, value in enumerate(bound['values']):
                self.registers[address + offset] = value
            return FakeResponse(address=address, count=len(bound['values']))
        if name == 'write_coils':
            for offset, value in enumerate(bound['values']):
                self.coils[address + offset] = value
            return FakeResponse(address=address, count=len(bound['values']))
        if name == 'write_register':
            self.registers[address] = bound['value']
            return FakeResponse(address=address, count=1)
        if name == 'write_coil':
            self.coils[address] = bound['value']
            return FakeResponse(address=address, count=1)
        if name == 'mask_write_register':
            current = self.registers.get(address, 0)
            self.registers[address] = (current & bound['and_mask']) | (bound['or_mask'] & ~bound['and_mask'])
            return FakeResponse(address=address, and_mask=bound['and_mask'], or_mask=bound['or_mask'])
        if name == 'read_device_information':
            objects = {0: b'ACME', 1: b'X-1', 2: b'1.2'} if bound['read_code'] == 1 else {4: b'Meter', 5: b'M-9'}
            return FakeResponse(information=objects, conformity=0x82, more_follows=0, next_object_id=0)
        if name == 'read_exception_status':
            return FakeResponse(status=0b00000101)
        if name == 'report_device_id':
            return FakeResponse(identifier=b'\x11', status=True, byte_count=1)
        if name.startswith('diag_'):
            return FakeResponse(message=[bound.get('msg', 7)] if 'msg' in bound else [42])
        if name == 'read_file_record':
            from pymodbus.pdu.file_message import FileRecord
            return FakeResponse(records=[FileRecord(file_number=getattr(r, 'file_number', 0), record_number=getattr(r, 'record_number', 0),
                                                    record_data=b'\x00\x0a\x00\x0b') for r in bound['records']])
        if name == 'write_file_record':
            return FakeResponse(records=list(bound['records']))
        return FakeResponse(registers=[], bits=[])

    def requests(self):
        """(address, count) of every read issued so far."""
        return [(c[1]['address'], c[1]['count']) for c in self.calls if c[0].startswith('read_')]


# Holding registers for a device with two floats (1001-1004), a gap, and a uint16 at 1010.
FLOAT_1_0 = [0x3f80, 0x0000]
FLOAT_2_0 = [0x4000, 0x0000]
SAMPLE_REGISTERS = {1001: FLOAT_1_0[0], 1002: FLOAT_1_0[1], 1003: FLOAT_2_0[0], 1004: FLOAT_2_0[1], 1010: 42, 1201: 7}
SAMPLE_COILS = {3: True, 5: False, 6: True}


@pytest.fixture
def device():
    return FakeDevice(registers=SAMPLE_REGISTERS, coils=SAMPLE_COILS, failing={1100, 1101})


@pytest.fixture
def client(device):
    return ModbusClient(device, '10.0.0.4')


@pytest.fixture
def configured_client(client):
    """Client with holding, coil maps matching SAMPLE_REGISTERS / SAMPLE_COILS, plus a failing int32 at 1100."""
    client.add_registers([(1001, '>f'), (1003, '>f'), (1010, 'uint16'), (1100, 'int32'), (1201, 'uint16')])
    client.add_pad(1005, 5)                      # bridges 1004 -> 1010 into one request
    client.add_registers([(3, 'bool'), (5, 'bool', 2)], register_map='coil')
    return client
