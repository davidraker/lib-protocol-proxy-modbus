import asyncio
import ssl
import logging
import time

from collections import Counter
from contextlib import asynccontextmanager
from typing import Any, Coroutine, Iterable, Mapping

from pymodbus import ModbusException
from pymodbus.exceptions import ConnectionException, ModbusIOException
from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient, AsyncModbusTlsClient, AsyncModbusUdpClient
from pymodbus.framer import FramerType
from pymodbus.pdu import ModbusPDU
from pymodbus.pdu.file_message import FileRecord

from .registers import BIT_TABLES, MAX_READ, QueryBlock, ReadBlock, RegisterMap, RegisterSpec, plan_ranges

_log = logging.getLogger(__name__)


# Keyword arguments accepted by each pymodbus request coroutine. The starting address (and, for writes, the values)
# are passed positionally by the coroutine getters below, so they are not listed here.
_COMMON_KWARGS = frozenset({'device_id', 'no_response_expected'})
_READ_KWARGS: dict[str, frozenset[str]] = {
    'coil': _COMMON_KWARGS | {'count'},
    'device_information': _COMMON_KWARGS | {'read_code', 'object_id'},
    'discrete_input': _COMMON_KWARGS | {'count'},
    'exception_status': _COMMON_KWARGS,
    'fifo_queue': _COMMON_KWARGS,
    'file_record': _COMMON_KWARGS | {'records'},
    'holding': _COMMON_KWARGS | {'count'},
    'input': _COMMON_KWARGS | {'count'},
}
_WRITE_KWARGS: dict[str, frozenset[str]] = {
    'coil': _COMMON_KWARGS,
    'holding': _COMMON_KWARGS,
}
WRITE_MODES = ('auto', 'single', 'multiple')
ILLEGAL_FUNCTION = 1        # Modbus exception code: the device does not implement the function
EXCEPTION_NAMES = {1: 'ILLEGAL FUNCTION', 2: 'ILLEGAL DATA ADDRESS', 3: 'ILLEGAL DATA VALUE', 4: 'SERVER DEVICE FAILURE',
                   5: 'ACKNOWLEDGE', 6: 'SERVER DEVICE BUSY', 8: 'MEMORY PARITY ERROR',
                   10: 'GATEWAY PATH UNAVAILABLE', 11: 'GATEWAY TARGET DEVICE FAILED TO RESPOND'}
# Device identification objects (Modbus Application Protocol V1.1b3, 6.21): 0-2 basic (mandatory), 3-6 regular.
DEVICE_INFORMATION_OBJECTS = {0: 'VendorName', 1: 'ProductCode', 2: 'MajorMinorRevision', 3: 'VendorUrl',
                              4: 'ProductName', 5: 'ModelName', 6: 'UserApplicationName'}
# pymodbus diagnostic and identification requests reachable through diagnostics(): name -> client method.
DIAGNOSTIC_FUNCTIONS = {
    'report_device_id': 'report_device_id', 'query_data': 'diag_query_data', 'restart_communication': 'diag_restart_communication',
    'read_diagnostic_register': 'diag_read_diagnostic_register', 'force_listen_only': 'diag_force_listen_only',
    'clear_counters': 'diag_clear_counters', 'bus_message_count': 'diag_read_bus_message_count',
    'bus_comm_error_count': 'diag_read_bus_comm_error_count', 'bus_exception_error_count': 'diag_read_bus_exception_error_count',
    'device_message_count': 'diag_read_device_message_count', 'device_no_response_count': 'diag_read_device_no_response_count',
    'device_nak_count': 'diag_read_device_nak_count', 'device_busy_count': 'diag_read_device_busy_count',
    'bus_char_overrun_count': 'diag_read_bus_char_overrun_count', 'iop_overrun_count': 'diag_read_iop_overrun_count',
    'clear_overrun_counter': 'diag_clear_overrun_counter', 'comm_event_counter': 'diag_get_comm_event_counter',
    'comm_event_log': 'diag_get_comm_event_log',
}
_PDU_HOUSEKEEPING = frozenset({'dev_id', 'transaction_id', 'retries', 'exception_code', 'bits', 'registers', 'count',
                               'address', 'space_left'})


class ModbusClient:
    def __init__(self, client, device_address, transport_protocol: str = 'tcp', modbus_port=502, **kwargs):
        self.client = client
        self.device_address = device_address
        self.device_type = transport_protocol
        self.modbus_port = modbus_port
        # Register data types, keyed by (unit_id, register_map). Populated with add_registers() and add_pad().
        self.register_maps: dict[tuple[int, str], RegisterMap] = {}
        # Largest unconfigured gap to read through rather than split into separate requests (see RegisterMap).
        self.max_gap: int = kwargs.get('max_gap', 0)
        # Requests to one device share one connection and run one at a time. IPC callbacks are dispatched
        # concurrently, so without this two overlapping polls or a poll and a write would interleave on the wire.
        self._lock = asyncio.Lock()
        # Write functions a unit has refused with ILLEGAL FUNCTION, by (unit_id, table): learned under write_mode
        # 'auto' so the other form is used from then on without another failed exchange.
        self._no_single_writes: set[tuple[int, str]] = set()
        self._no_multiple_writes: set[tuple[int, str]] = set()
        self._no_mask_writes: set[int] = set()            # units that refused FC22: read-modify-write instead
        # Health: counters per unit (and under None for the connection itself), see status().
        self.stats: dict[int | None, dict[str, Any]] = {}
        self._connected_once = False

    @asynccontextmanager
    async def _connection(self):
        """Serialize access to the device for one operation. Each request connects as needed (see _execute).

        The connection is kept open between requests. (Using the pymodbus client as a context manager instead would
        close the connection at the end of every request, which cancels any request another task has in flight.)
        """
        async with self._lock:
            yield self.client

    async def _ensure_connected(self):
        if not self.client.connected:
            if self._connected_once:
                self._unit_stats(None)['reconnects'] += 1
            if not await self.client.connect():
                raise ConnectionException(f"Unable to connect to Modbus device {self.device_address}")
            self._connected_once = True

    # ---- health ------------------------------------------------------------------------------------------------------
    def _unit_stats(self, unit_id: int | None) -> dict[str, Any]:
        stats = self.stats.get(unit_id)
        if stats is None:
            stats = self.stats[unit_id] = {'requests': 0, 'successes': 0, 'failures': 0, 'timeouts': 0,
                                           'exception_responses': Counter(), 'reconnects': 0, 'consecutive_failures': 0,
                                           'last_success': None, 'last_failure': None, 'last_error': None}
        return stats

    def _record(self, unit_id: int | None, error) -> None:
        stats = self._unit_stats(unit_id)
        stats['requests'] += 1
        if error is None:
            stats['successes'] += 1
            stats['consecutive_failures'] = 0
            stats['last_success'] = time.time()
            return
        stats['failures'] += 1
        stats['consecutive_failures'] += 1
        stats['last_failure'] = time.time()
        code = getattr(error, 'exception_code', None)
        if code is not None:
            stats['exception_responses'][code] += 1
            stats['last_error'] = f'{EXCEPTION_NAMES.get(code, "exception " + str(code))} (code {code})'
        else:
            if 'No response' in str(error) or 'Timeout' in str(error) or 'timed out' in str(error).lower():
                stats['timeouts'] += 1
            stats['last_error'] = str(error)

    def status(self, unit_id: int | None = None) -> dict[str, Any]:
        """Health of this connection and of one unit (or every unit when None): request counters, exception responses
        by code, timeouts, reconnects, the last error, and the write functions the units have refused."""
        def view(key):
            stats = dict(self._unit_stats(key))
            stats['exception_responses'] = {str(code): n for code, n in stats['exception_responses'].items()}
            return stats
        units = [unit_id] if unit_id is not None else sorted(k for k in self.stats if k is not None)
        return {'device_address': self.device_address, 'transport': self.device_type, 'connected': bool(self.client.connected),
                'connection': view(None), 'units': {str(u): view(u) for u in units},
                'refused_single_writes': sorted(f'{u}:{t}' for u, t in self._no_single_writes),
                'refused_multiple_writes': sorted(f'{u}:{t}' for u, t in self._no_multiple_writes),
                'refused_mask_writes': sorted(self._no_mask_writes)}


    def close(self):
        """Close the connection to the device."""
        self.client.close()

    @staticmethod
    def tls_context(ca_path: str | None = None, cert_path: str | None = None, key_path: str | None = None, *,
                    tls_verify: bool = True) -> ssl.SSLContext:
        """The client's TLS context: the server is checked against ``ca_path`` (or the system's CAs) unless
        ``tls_verify`` is off, and the client presents ``cert_path``/``key_path`` when given (servers that require
        client authentication)."""
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_path or None)
        if cert_path:
            ctx.load_cert_chain(cert_path, key_path or None)
        if not tls_verify:
            _log.warning('Modbus TLS server certificate verification is disabled; the server identity is not checked.')
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    @classmethod
    async def create(cls, device_address, transport_protocol: str = 'tcp', **kwargs):
        match transport_protocol.lower().strip():
            case 'serial':
                client = AsyncModbusSerialClient(
                    device_address,
                    framer=kwargs.get('framer', FramerType.RTU),
                    baudrate=kwargs.get('baudrate', 19200),
                    bytesize=kwargs.get('bytesize', 8),
                    parity=kwargs.get('parity', 'N'),
                    stopbits=kwargs.get('stopbits', 1),
                    handle_local_echo=kwargs.get('handle_local_echo', False),
                    name=kwargs.get('name', 'comm'),
                    reconnect_delay=kwargs.get('reconnect_delay', 0.1),
                    reconnect_delay_max=kwargs.get('reconnect_delay_max', 300),
                    timeout=kwargs.get('timeout', 3),
                    retries=kwargs.get('retries', 3),
                    trace_packet=kwargs.get('trace_packet', None),
                    trace_pdu=kwargs.get('trace_pdu', None),
                    trace_connect=kwargs.get('trace_connect', None),
                )
            case 'tcp':
                client = AsyncModbusTcpClient(
                    device_address,
                    framer=kwargs.get('framer', FramerType.SOCKET),
                    port=kwargs.get('port', 502),
                    name=kwargs.get('name', 'comm'),
                    source_address=kwargs.get('source_address', None),
                    reconnect_delay=kwargs.get('reconnect_delay', 0.1),
                    reconnect_delay_max=kwargs.get('reconnect_delay_max', 300),
                    timeout=kwargs.get('timeout', 3),
                    retries=kwargs.get('retries', 3),
                    trace_packet=kwargs.get('trace_packet', None),
                    trace_pdu=kwargs.get('trace_pdu', None),
                    trace_connect=kwargs.get('trace_connect', None),
                )
            case 'tls':
                client = AsyncModbusTlsClient(
                    device_address,
                    sslctx=cls.tls_context(kwargs.get('ca_path'), kwargs.get('cert_path'), kwargs.get('key_path'),
                                           tls_verify=kwargs.get('tls_verify', True)),
                    framer=kwargs.get('framer', FramerType.TLS),
                    port=kwargs.get('port', 802),
                    name=kwargs.get('name', 'comm'),
                    source_address=kwargs.get('source_address', None),
                    reconnect_delay=kwargs.get('reconnect_delay', 0.1),
                    reconnect_delay_max=kwargs.get('reconnect_delay_max', 300),
                    timeout=kwargs.get('timeout', 3),
                    retries=kwargs.get('retries', 3),
                    trace_packet=kwargs.get('trace_packet', None),
                    trace_pdu=kwargs.get('trace_pdu', None),
                    trace_connect=kwargs.get('trace_connect', None),
                )
            case 'udp':
                client = AsyncModbusUdpClient(
                    device_address,
                    framer=kwargs.get('framer', FramerType.SOCKET),
                    port=kwargs.get('port', 502),
                    name=kwargs.get('name', 'comm'),
                    source_address=kwargs.get('source_address', None),
                    reconnect_delay=kwargs.get('reconnect_delay', 0.1),
                    reconnect_delay_max=kwargs.get('reconnect_delay_max', 300),
                    timeout=kwargs.get('timeout', 3),
                    retries=kwargs.get('retries', 3),
                    trace_packet=kwargs.get('trace_packet', None),
                    trace_pdu=kwargs.get('trace_pdu', None),
                    trace_connect=kwargs.get('trace_connect', None),
                )
            case _:
                raise ValueError(f"Unknown Modbus device type: {transport_protocol}. Must be one of 'serial', 'tcp', 'tls', or 'udp'.")
        return cls(client, device_address, transport_protocol, **kwargs)

    def get_register_map(self, register_map: str = 'holding', unit_id: int = 1, create: bool = False
                         ) -> RegisterMap | None:
        """Return the RegisterMap for a table on a unit, optionally creating an empty one."""
        key = (unit_id, register_map)
        reg_map = self.register_maps.get(key)
        if reg_map is None and create:
            reg_map = self.register_maps[key] = RegisterMap(register_map, self.max_gap)
        return reg_map

    def add_registers(self, specs: Iterable[RegisterSpec | dict | tuple], register_map: str = 'holding',
                      unit_id: int = 1) -> list[RegisterSpec]:
        """Register the data types of points in a table.

        Each spec may be a RegisterSpec, a dict of RegisterSpec keyword arguments (address, data_type, count,
        word_order, string_encoding), or a tuple of RegisterSpec positional arguments.
        """
        reg_map = self.get_register_map(register_map, unit_id, create=True)
        return [reg_map.add(self._as_spec(spec)) for spec in specs]

    def add_pad(self, address: int, count: int = 1, register_map: str = 'holding', unit_id: int = 1) -> RegisterSpec:
        """Mark registers which should be read (to keep requests contiguous) but never decoded or returned."""
        return self.get_register_map(register_map, unit_id, create=True).add_pad(address, count)

    def configure_registers(self, tables: Mapping[str, Iterable[RegisterSpec | dict | tuple]], unit_id: int = 1,
                            max_gap: int | None = None, clear_others: bool = False) -> dict[str, Any]:
        """Replace the register maps of the listed tables on a unit.

        Each listed table is rebuilt from scratch from its specs. Every table is built and validated before any is
        installed, so an invalid spec leaves the existing maps untouched. Tables not listed are kept, unless
        clear_others is True, in which case they are removed for this unit. max_gap defaults to the client's.

        Returns {'configured': {table: summary}, 'cleared': [table, ...]}, where each summary gives the number of
        specs and pads loaded and the planned read blocks as [start, count] pairs.
        """
        gap = self.max_gap if max_gap is None else max_gap
        new_maps: dict[tuple[int, str], RegisterMap] = {}
        for table, specs in tables.items():
            reg_map = RegisterMap(table, gap)
            for index, spec in enumerate(specs):
                try:
                    reg_map.add(self._as_spec(spec))
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid spec {index} for {table} table on unit {unit_id}: {e}") from e
            new_maps[(unit_id, table)] = reg_map
        cleared = []
        if clear_others:
            for key in [k for k in self.register_maps if k[0] == unit_id and k not in new_maps]:
                del self.register_maps[key]
                cleared.append(key[1])
        self.register_maps.update(new_maps)
        return {'configured': {table: self.describe_registers(table, unit_id) for (_, table) in new_maps},
                'cleared': cleared}

    def describe_registers(self, register_map: str = 'holding', unit_id: int = 1) -> dict[str, Any] | None:
        """Summarize a configured map: spec and pad counts and the planned read blocks."""
        reg_map = self.get_register_map(register_map, unit_id)
        if reg_map is None:
            return None
        pads = sum(1 for spec in reg_map if spec.is_pad)
        return {'specs': len(reg_map) - pads, 'pads': pads, 'max_gap': reg_map.max_gap,
                'blocks': [[block.start, block.count] for block in reg_map.blocks]}

    @staticmethod
    def _as_spec(spec: RegisterSpec | dict | tuple) -> RegisterSpec:
        if isinstance(spec, RegisterSpec):
            return spec
        if isinstance(spec, dict):
            return RegisterSpec(**spec)
        if isinstance(spec, (tuple, list)):
            return RegisterSpec(*spec)
        raise TypeError(f"Register spec must be a RegisterSpec, dict, or tuple, not {type(spec).__name__}")

    async def read(self, queries: Iterable[tuple[int, int]] | None = None, register_map: str = 'holding',
                   unit_id: int = 1, decode: bool = False, **kwargs):
        """Read registers from one table on one device.

        Queries are (start, count) pairs. Overlapping or adjacent queries (and those separated by no more than the
        map's max_gap) are merged into the fewest requests permitted by the protocol. If a merged request fails, its
        constituent queries are retried individually so that one bad address does not fail its neighbours.

        With decode False, 'results' is a list aligned with the queries holding the raw registers (or bits, for coil
        tables) of each, and 'errors' a parallel list.

        With decode True, values are decoded using the data types in this client's RegisterMap. Points wholly inside
        the merged requests are returned; configured pads and unconfigured addresses are read but not returned. If
        queries is None, the map's planned read blocks are used instead. 'results' is then {address: value} and
        'errors' is {request_start_address: error}.
        """
        # TODO: This function reads from one register map on one device. Should it or another func handle many at once?
        bit_table = register_map in BIT_TABLES
        reg_map = None
        if decode:
            reg_map = self.get_register_map(register_map, unit_id)
            if reg_map is None:
                raise ValueError(f"No register data types have been configured for {register_map} on unit {unit_id}.")
        elif queries is None:
            raise ValueError("Queries are required unless decode is True.")

        async def request(start: int, count: int) -> tuple[list | None, Any]:
            response, error = await self._execute(
                lambda: self._get_read_coroutine(register_map, start, count=count, device_id=unit_id, **kwargs), unit_id)
            if error is not None:
                return None, error
            return self._read_values(register_map, response), None

        async with self._connection():
            if queries is None:
                return await self._read_planned(reg_map, request)
            queries = [(int(start), int(count)) for start, count in queries]
            max_gap = reg_map.max_gap if reg_map is not None else self.max_gap
            blocks = plan_ranges(queries, MAX_READ.get(register_map, 1), max_gap) if register_map in MAX_READ \
                else [QueryBlock(start, count, (i,)) for i, (start, count) in enumerate(queries)]
            if decode:
                return await self._read_decoded_queries(reg_map, queries, blocks, request)
            return await self._read_raw_queries(queries, blocks, request)

    @staticmethod
    def _read_values(register_map: str, response) -> list:
        """What a read reply carries for a table: bits, registers, or the one value of an addressless function."""
        if register_map in BIT_TABLES:
            return response.bits
        if register_map == 'exception_status':
            return [response.status]
        if register_map == 'fifo_queue':
            return list(response.values)
        return response.registers

    @staticmethod
    async def _read_raw_queries(queries, blocks, request) -> dict:
        results: list = [None] * len(queries)
        errors: list = [None] * len(queries)

        async def read_one(index: int):
            start, count = queries[index]
            results[index], errors[index] = await request(start, count)

        for block in blocks:
            raw, error = await request(block.start, block.count)
            if error is None:
                for index in block.queries:
                    start, count = queries[index]
                    offset = start - block.start
                    results[index] = raw[offset:offset + count]
            elif len(block.queries) > 1 and ModbusClient._device_rejected(error):
                _log.debug(f"Merged read at {block.start} (count {block.count}) was rejected; retrying its"
                           f" {len(block.queries)} queries individually.")
                for index in block.queries:
                    await read_one(index)
            else:
                for index in block.queries:
                    errors[index] = error
        return {'results': results, 'errors': errors}

    async def _read_decoded_queries(self, reg_map: RegisterMap, queries, blocks, request) -> dict:
        results: dict = {}
        errors: dict = {}
        for block in blocks:
            specs = self._specs_within(reg_map, block.start, block.count)
            raw, error = await request(block.start, block.count)
            if error is None:
                self._decode_into(reg_map, ReadBlock(block.start, block.count, specs), raw, results, errors)
            elif len(block.queries) > 1 and self._device_rejected(error):
                _log.debug(f"Merged read at {block.start} (count {block.count}) was rejected; retrying its"
                           f" {len(block.queries)} queries individually.")
                for index in block.queries:
                    start, count = queries[index]
                    raw, error = await request(start, count)
                    if error is None:
                        sub_specs = tuple(s for s in specs if start <= s.address and s.end <= start + count - 1)
                        self._decode_into(reg_map, ReadBlock(start, count, sub_specs), raw, results, errors)
                    else:
                        errors[start] = error
            else:
                errors[block.start] = error
        return {'results': results, 'errors': errors}

    async def _read_planned(self, reg_map: RegisterMap, request) -> dict:
        """Read every block the map plans. A failed multi-point block is retried one point at a time."""
        results: dict = {}
        errors: dict = {}
        for block in reg_map.blocks:
            raw, error = await request(block.start, block.count)
            if error is None:
                self._decode_into(reg_map, block, raw, results, errors)
                continue
            points = [s for s in block.specs if not s.is_pad]
            if len(points) > 1 and self._device_rejected(error):
                _log.debug(f"Planned read at {block.start} (count {block.count}) was rejected; retrying its"
                           f" {len(points)} points individually.")
                for spec in points:
                    raw, error = await request(spec.address, spec.count)
                    if error is None:
                        self._decode_into(reg_map, ReadBlock(spec.address, spec.count, (spec,)), raw, results, errors)
                    else:
                        errors[spec.address] = error
            else:
                errors[block.start] = error
        return {'results': results, 'errors': errors}

    @staticmethod
    def _device_rejected(error) -> bool:
        """True when the device answered with a Modbus exception response (e.g., illegal data address).

        Only then is it worth retrying a merged request as its individual parts, since one bad address may have
        spoiled the rest. Timeouts and connection failures (reported as strings by _execute) mean the device is not
        answering at all: retrying smaller pieces would only repeat the full timeout cycle for each of them.
        """
        return not isinstance(error, str)

    @staticmethod
    def _decode_into(reg_map: RegisterMap, block: ReadBlock, raw, results: dict, errors: dict):
        try:
            results.update(reg_map.decode(block, raw))
        except Exception as e:
            errors[block.start] = f"Error decoding Modbus registers starting at {block.start}: {e}"

    @staticmethod
    def _specs_within(reg_map: RegisterMap, start: int, count: int) -> tuple[RegisterSpec, ...]:
        """The configured points lying wholly within [start, start + count - 1]. Partly-covered points are skipped."""
        end = start + count - 1
        specs = []
        for spec in reg_map.in_range(start, end):
            if spec.address < start or spec.end > end:
                _log.warning(f"{spec!r} extends outside the request [{start}, {end}] and will not be decoded.")
            elif not spec.is_pad:
                specs.append(spec)
        return tuple(specs)

    async def write(self, queries: Iterable[tuple[int, int, Any]], register_map: str = 'holding', unit_id: int = 1,
                    encode: bool = False, write_mode: str = 'auto', **kwargs):
        """Write registers to one table on one device.

        Each query is (start, count, values). With encode False, values must already be a list of register ints
        (or bools, for coil tables) and count is ignored, as the write length is taken from values. With encode
        True, start must be the address of a point in this client's RegisterMap, values is the Python value for
        that point, and count (if not None) must match the point's configured count.

        write_mode chooses the function code. 'auto' (the default) writes one register or coil with the single-write
        function (FC06, FC05) and more with the multiple-write function (FC16, FC15); a unit that answers ILLEGAL
        FUNCTION is retried once with the other form and remembered, so later writes use the form it accepts.
        'multiple' always uses FC16/FC15; 'single' always uses FC06/FC05 and refuses a value that spans several
        registers.

        A query may carry a fourth element, a bit index: ``(start, None, value, bit)`` writes that one bit of the
        holding register at start with FC22 mask write, or by read-modify-write when the unit refuses FC22.

        'results' holds {'address', 'count'} from each acknowledged write, aligned with 'errors', one per query.
        """
        # TODO: This may need different parameters for different query types. Needs more fleshed out.
        if write_mode not in WRITE_MODES:
            raise ValueError(f"write_mode must be one of {', '.join(WRITE_MODES)}, not {write_mode!r}.")
        bit_table = register_map in BIT_TABLES
        reg_map = None
        if encode:
            reg_map = self.get_register_map(register_map, unit_id)
            if reg_map is None:
                raise ValueError(f"No register data types have been configured for {register_map} on unit {unit_id}.")
        responses = {'results': [], 'errors': []}
        async with self._connection():
            for query in queries:
                start_register, count, values = query[0], query[1], query[2]
                bit = query[3] if len(query) > 3 else None
                if bit is not None:
                    response, error = await self._write_bit(register_map, start_register, int(bit), values, unit_id, kwargs)
                    responses['results'].append({'address': start_register, 'count': 1, 'bit': int(bit)} if error is None else None)
                    responses['errors'].append(error)
                    continue
                if encode:
                    try:
                        values = self._encode_value(reg_map, start_register, count, values, bit_table)
                    except (KeyError, ValueError, TypeError) as e:
                        responses['results'].append(None)
                        responses['errors'].append(f"Error encoding Modbus value: {e}")
                        continue
                values = list(values)
                single = self._choose_single_write(write_mode, register_map, unit_id, len(values))
                if single is None:
                    responses['results'].append(None)
                    responses['errors'].append(f"Writing {len(values)} registers at {start_register} needs the"
                                               f" multiple-write function, but write_mode is 'single'.")
                    continue
                response, error = await self._write_once(register_map, start_register, values, unit_id, single, kwargs)
                if error is not None and write_mode == 'auto' and self._illegal_function(error):
                    key = (unit_id, register_map)
                    if single:
                        self._no_single_writes.add(key)
                        _log.info(f"Unit {unit_id} on {self.device_address} refused the single-write function for"
                                  f" {register_map}; using the multiple-write function from now on.")
                        response, error = await self._write_once(register_map, start_register, values, unit_id, False, kwargs)
                    elif len(values) == 1:
                        self._no_multiple_writes.add(key)
                        _log.info(f"Unit {unit_id} on {self.device_address} refused the multiple-write function for"
                                  f" {register_map}; using the single-write function for single values from now on.")
                        response, error = await self._write_once(register_map, start_register, values, unit_id, True, kwargs)
                if error is None:
                    responses['results'].append({'address': response.address,
                                                 'count': getattr(response, 'count', None) or len(values)})
                    responses['errors'].append(None)
                else:
                    responses['results'].append(None)
                    responses['errors'].append(error)
        return responses

    async def _write_once(self, register_map: str, start: int, values: list, unit_id: int, single: bool, kwargs: dict):
        return await self._execute(
            lambda: self._get_write_coroutine(register_map, start, values, single=single, device_id=unit_id, **kwargs),
            unit_id)

    async def _write_bit(self, register_map: str, address: int, bit: int, value, unit_id: int, kwargs: dict):
        """Write one bit of a holding register: FC22 mask write, or read-modify-write for a unit that refuses it."""
        if register_map != 'holding':
            return None, f"Bit writes apply to holding registers, not the {register_map} table."
        if not 0 <= bit <= 15:
            return None, f"Bit index {bit} is out of range for a 16-bit register."
        if isinstance(value, str):
            value = value.strip().lower() in ('true', 't', 'on', 'yes', '1')
        and_mask = 0xFFFF & ~(1 << bit)
        or_mask = (1 << bit) if value else 0
        selected = self._select_kwargs(register_map, _WRITE_KWARGS['holding'], kwargs)
        if unit_id not in self._no_mask_writes:
            response, error = await self._execute(
                lambda: self.client.mask_write_register(address=address, and_mask=and_mask, or_mask=or_mask,
                                                        device_id=unit_id, **selected), unit_id)
            if error is None or not self._illegal_function(error):
                return response, error
            self._no_mask_writes.add(unit_id)
            _log.info(f"Unit {unit_id} on {self.device_address} refused FC22 mask write; writing bits by"
                      f" read-modify-write from now on.")
        current, error = await self._execute(
            lambda: self.client.read_holding_registers(address, count=1, device_id=unit_id, **selected), unit_id)
        if error is not None:
            return None, error
        word = (current.registers[0] & and_mask) | (or_mask & ~and_mask)
        single = self._choose_single_write('auto', register_map, unit_id, 1)
        response, error = await self._write_once(register_map, address, [word], unit_id, bool(single), kwargs)
        if error is not None and self._illegal_function(error):
            response, error = await self._write_once(register_map, address, [word], unit_id, not single, kwargs)
        return response, error

    def _choose_single_write(self, write_mode: str, register_map: str, unit_id: int, count: int) -> bool | None:
        """Whether to use the single-write function; None when the write cannot be made under the mode."""
        if write_mode == 'multiple':
            return False
        if write_mode == 'single':
            return True if count == 1 else None
        if count != 1:
            return False
        key = (unit_id, register_map)
        return key not in self._no_single_writes or key in self._no_multiple_writes

    @staticmethod
    def _illegal_function(error) -> bool:
        return getattr(error, 'exception_code', None) == ILLEGAL_FUNCTION

    @staticmethod
    def _encode_value(reg_map: RegisterMap, start: int, count: int | None, value: Any, bit_table: bool
                      ) -> list[int] | list[bool]:
        spec = reg_map.get(start)
        if spec is None:
            raise KeyError(f"No {reg_map.table} register configured at address {start}")
        if count is not None and count != spec.count:
            raise ValueError(f"Count {count} does not match configured count {spec.count} for {spec!r}")
        return spec.encode(value, bit_table)

    async def _execute(self, make_request, unit_id: int | None = None) -> tuple[ModbusPDU | None, Any]:
        """Build and await one request, returning (response, None) on success or (None, error) otherwise.

        A transport failure (no response, connection lost) may mean the connection is half-open: a device or gateway
        that restarted without resetting the TCP connection leaves pymodbus believing it is still connected, and every
        later request would fail the same way. So on such a failure the connection is dropped; the next request opens
        a new one. The failed request is not retried here: a unit that is merely silent behind a healthy gateway would
        otherwise cost two full timeout-and-retry cycles on the shared bus instead of one.
        """
        response, error = await self._execute_unrecorded(make_request)
        self._record(unit_id, error)
        return response, error

    async def _execute_unrecorded(self, make_request) -> tuple[ModbusPDU | None, Any]:
        try:
            await self._ensure_connected()
            return await self._attempt(make_request)
        except (ModbusIOException, ConnectionException) as e:
            # pymodbus 3.15 raised a connection error ("Not connected") for a refused handshake; 3.16 reports it as a
            # read timeout. One message covers both: the device did not answer and the connection is dropped.
            _log.debug(f"Request to {self.device_address} failed ({e}); dropping the connection so the next request"
                       " reconnects.")
            self.client.close()
            return None, f"Error in Modbus Client: Not connected or no response: {e}"
        except ModbusException as e:
            return None, f"Error in Modbus Client: {e}"
        except ValueError as e:
            return None, f"Error formulating Modbus query: {e}"
        except Exception as e:
            return None, f"Unexpected error while communicating with Modbus device: {e}"

    # ---- other function codes ------------------------------------------------------------------------------------------
    @staticmethod
    def _text(value) -> Any:
        if isinstance(value, (bytes, bytearray)):
            try:
                return bytes(value).decode('utf8').rstrip('\x00')
            except UnicodeDecodeError:
                return bytes(value).hex()
        return value

    @classmethod
    def _pdu_fields(cls, response) -> dict[str, Any]:
        """A reply's protocol fields as JSON-friendly values, without the transport housekeeping."""
        fields = {}
        for name, value in vars(response).items():
            if name.startswith('_') or name in _PDU_HOUSEKEEPING:
                continue
            if isinstance(value, dict):
                value = {str(k): cls._text(v) for k, v in value.items()}
            elif isinstance(value, (list, tuple)):
                value = [cls._text(v) for v in value]
            elif hasattr(value, 'value') and not isinstance(value, (int, float, str, bool)):
                value = value.value              # an enum
            else:
                value = cls._text(value)
            fields[name] = value
        return fields

    async def device_information(self, read_code: int = 1, object_id: int = 0, unit_id: int = 1) -> dict[str, Any]:
        """FC43/14 read device identification. read_code 1 basic (VendorName, ProductCode, MajorMinorRevision),
        2 regular (3-6), 3 extended (device-specific objects), 4 one object (object_id). Objects are returned by
        name where the standard names them, else by number."""
        async with self._connection():
            response, error = await self._execute(
                lambda: self.client.read_device_information(read_code=read_code, object_id=object_id, device_id=unit_id), unit_id)
        if error is not None:
            return {'results': {}, 'errors': {'device_information': str(error)}}
        objects = {DEVICE_INFORMATION_OBJECTS.get(k, str(k)): self._text(v)
                   for k, v in (getattr(response, 'information', None) or {}).items()}
        return {'results': {'objects': objects, 'conformity': getattr(response, 'conformity', None),
                            'more_follows': bool(getattr(response, 'more_follows', 0)),
                            'next_object_id': getattr(response, 'next_object_id', None)}, 'errors': {}}

    async def exception_status(self, unit_id: int = 1) -> dict[str, Any]:
        """FC07 read exception status: the device's eight status bits as an integer and as a list of booleans."""
        async with self._connection():
            response, error = await self._execute(lambda: self.client.read_exception_status(device_id=unit_id), unit_id)
        if error is not None:
            return {'results': {}, 'errors': {'exception_status': str(error)}}
        status = int(response.status)
        return {'results': {'status': status, 'bits': [bool(status >> i & 1) for i in range(8)]}, 'errors': {}}

    async def diagnostics(self, function: str, unit_id: int = 1, **arguments) -> dict[str, Any]:
        """FC08 diagnostics, FC17 report server id, FC11/FC12 event counter and log, by the names in
        DIAGNOSTIC_FUNCTIONS. ``arguments`` are the request's own (``msg`` for query_data)."""
        method_name = DIAGNOSTIC_FUNCTIONS.get(function)
        if method_name is None:
            return {'results': {}, 'errors': {'diagnostics': f"Unknown diagnostic function {function!r}; known:"
                                                             f" {', '.join(sorted(DIAGNOSTIC_FUNCTIONS))}"}}
        if function == 'query_data' and isinstance(arguments.get('msg'), str):
            arguments['msg'] = arguments['msg'].encode('utf8')
        method = getattr(self.client, method_name)
        async with self._connection():
            response, error = await self._execute(lambda: method(device_id=unit_id, **arguments), unit_id)
        if error is not None:
            return {'results': {}, 'errors': {'diagnostics': str(error)}}
        return {'results': {'function': function, **self._pdu_fields(response)}, 'errors': {}}

    async def file_record(self, action: str, records: Iterable[Mapping[str, Any]], unit_id: int = 1) -> dict[str, Any]:
        """FC20/FC21 read or write file records. Each record names file_number, record_number and record_length;
        a write also carries record_data as a list of 16-bit values."""
        if action not in ('read', 'write'):
            return {'results': {}, 'errors': {'file_record': f"action must be 'read' or 'write', not {action!r}."}}
        try:
            built = []
            for record in records:
                file_number, record_number = int(record['file_number']), int(record['record_number'])
                if action == 'write':      # pymodbus takes either the data (a write) or the length (a read), not both
                    data = b''.join(int(v).to_bytes(2, 'big') for v in record['record_data'])
                    built.append(FileRecord(file_number=file_number, record_number=record_number, record_data=data))
                else:
                    built.append(FileRecord(file_number=file_number, record_number=record_number,
                                            record_length=int(record['record_length'])))
        except (KeyError, TypeError, ValueError) as e:
            return {'results': {}, 'errors': {'file_record': f'Bad record specification: {e}'}}
        method = self.client.read_file_record if action == 'read' else self.client.write_file_record
        async with self._connection():
            response, error = await self._execute(lambda: method(built, device_id=unit_id), unit_id)
        if error is not None:
            return {'results': {}, 'errors': {'file_record': str(error)}}
        out = []
        for record in getattr(response, 'records', []) or []:
            data = bytes(getattr(record, 'record_data', b''))
            out.append({'file_number': record.file_number, 'record_number': record.record_number,
                        'record_length': record.record_length,
                        'record_data': [int.from_bytes(data[i:i + 2], 'big') for i in range(0, len(data) - 1, 2)]})
        return {'results': {'records': out}, 'errors': {}}

    @staticmethod
    async def _attempt(make_request) -> tuple[ModbusPDU | None, Any]:
        response = await make_request()
        if response.isError():
            return None, response
        return response, None

    @staticmethod
    def _select_kwargs(register_map: str, allowed: frozenset[str], kwargs: dict[str, Any]) -> dict[str, Any]:
        """Return only the keyword arguments accepted by the pymodbus coroutine for this register map."""
        ignored = set(kwargs) - allowed
        if ignored:
            _log.debug(f"Ignoring arguments not applicable to Modbus '{register_map}' requests: {sorted(ignored)}")
        return {k: v for k, v in kwargs.items() if k in allowed}

    def _get_read_coroutine(self, register_map: str, start, **kwargs) -> Coroutine[Any, Any, ModbusPDU] | None:
        allowed = _READ_KWARGS.get(register_map)
        if allowed is None:
            raise ValueError(f"Attempt to query unknown Modbus register map: {register_map}")
        selected = self._select_kwargs(register_map, allowed, kwargs)
        match register_map:
            # TODO: Should report_device_id be one of these?
            case 'coil':
                return self.client.read_coils(start, **selected)
            case 'device_information':
                # Takes no address or count; optionally read_code and object_id.
                return self.client.read_device_information(**selected)
            case 'discrete_input':
                return self.client.read_discrete_inputs(start, **selected)
            case 'exception_status':
                # Takes no address or count.
                return self.client.read_exception_status(**selected)
            case 'fifo_queue':
                # Takes an address but no count.
                return self.client.read_fifo_queue(address=start, **selected)
            case 'file_record':
                # Takes neither address nor count; requires a list of FileRecord objects.
                if 'records' not in selected:
                    raise ValueError("Reading a Modbus file_record requires a 'records' list of FileRecord objects.")
                return self.client.read_file_record(**selected)
            case 'holding':
                return self.client.read_holding_registers(start, **selected)
            case 'input':
                return self.client.read_input_registers(start, **selected)
            case _:
                raise ValueError(f"Attempt to query unknown Modbus register map: {register_map}")

    def _get_write_coroutine(self, register_map: str, start, values, single: bool = False, **kwargs
                             ) -> Coroutine[Any, Any, ModbusPDU] | None:
        """The pymodbus write for a table: FC16/FC15 for ``values``, or FC06/FC05 for its one value when ``single``."""
        allowed = _WRITE_KWARGS.get(register_map)
        if allowed is None:
            raise ValueError(f"Attempt to query unknown Modbus register map: {register_map}")
        selected = self._select_kwargs(register_map, allowed, kwargs)
        if single and len(values) != 1:
            raise ValueError(f"A single-register write takes one value, not {len(values)}.")
        match register_map:
            case 'holding':
                return (self.client.write_register(start, values[0], **selected) if single
                        else self.client.write_registers(start, values, **selected))
            case 'coil':
                return (self.client.write_coil(start, values[0], **selected) if single
                        else self.client.write_coils(start, values, **selected))
            case _:
                raise ValueError(f"Attempt to write to unknown Modbus register map: {register_map}")
