"""One Modbus TCP server (slave) served by the proxy (the ``slave`` role): the inverse of :mod:`client`.

The served values are the register tables of a pymodbus ``SimDevice``: one block per Modbus table (coils, discrete
inputs, holding registers, input registers) covering the configured addresses, served by a ``ModbusTcpServer`` on
``host:port`` as unit ``unit_id``. The register data types come from the same :class:`RegisterSpec` rows the client
role uses, plus a topic, whether the master may write the point (``remote_writable``) and an ``initial_value``.

Platform writes (``WRITE_POINTS``) encode the value into the registers through the datastore API; the master's writes
reach the ``SimDevice.action`` hook, which refuses them for points that are not remote-writable (ILLEGAL_ADDRESS, as
Modbus has no better code) and otherwise lets them through, then decodes the affected points and pushes them to the
caller. Reads from the master are answered by pymodbus from the registers.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pymodbus.constants import ExcCodes
from pymodbus.server import ModbusTcpServer
from pymodbus.simulator import DataType, SimData, SimDevice

from .registers import BIT_TABLES, PAD, RegisterMap, RegisterSpec, parse_type_spec

_log = logging.getLogger(__name__)

PushCallback = Callable[[dict[str, Any]], Awaitable[None]]
TABLES = ('coil', 'discrete_input', 'holding', 'input')
#: Function code the proxy uses to read, and to set, each table through the datastore.
READ_CODE = {'coil': 1, 'discrete_input': 2, 'holding': 3, 'input': 4}
#: Which table a master's write function code touches.
WRITE_TABLE = {5: 'coil', 15: 'coil', 6: 'holding', 16: 'holding', 22: 'holding', 23: 'holding'}
BIND_TIMEOUT = 5.0


@dataclass(frozen=True)
class ServedPoint:
    topic: str
    table: str
    spec: RegisterSpec
    remote_writable: bool
    initial_value: Any = None

    @classmethod
    def from_dict(cls, table: str, row: dict) -> 'ServedPoint':
        if table not in TABLES:
            raise ValueError(f'unknown Modbus table {table!r}')
        fields = {k: row[k] for k in ('address', 'data_type', 'count', 'word_order', 'string_encoding', 'byte_swap') if k in row}
        if isinstance(fields.get('data_type'), str):
            data_type, parsed_count, little = parse_type_spec(fields['data_type'])
            fields['data_type'] = data_type
            fields.setdefault('count', parsed_count)
            if little:
                fields['byte_swap'] = True
                fields.setdefault('word_order', 'little')
        if fields.get('count') is None:
            fields.pop('count', None)
        spec = RegisterSpec(**fields)
        remote_writable = row.get('remote_writable')
        if remote_writable in (None, ''):
            remote_writable = table in ('coil', 'holding') and not spec.is_pad
        return cls(topic=str(row.get('topic') or f'{table}:{spec.address}'), table=table, spec=spec,
                   remote_writable=bool(remote_writable) and not spec.is_pad, initial_value=row.get('initial_value'))


class ServedModbusDevice:
    """A pymodbus TCP server on ``host:port`` serving the registered points as unit ``unit_id``."""

    def __init__(self, host: str, port: int, *, unit_id: int = 1, push: PushCallback | None = None, remote_id=None):
        self.host, self.port, self.unit_id = host, int(port), int(unit_id)
        self.push, self.remote_id = push, remote_id
        self.points: dict[str, ServedPoint] = {}
        self.maps: dict[str, RegisterMap] = {}
        self.by_address: dict[tuple[str, int], ServedPoint] = {}
        self.values: dict[str, Any] = {}             # the last value stored or accepted, by topic
        self.server: ModbusTcpServer | None = None
        self._own_write = False
        self.seeded = 0

    # ---- configuration ------------------------------------------------------------------------------------------
    def configure_points(self, tables: dict[str, list[dict]]) -> dict:
        """Replace the point table; a bad row rejects the whole table. Returns a per-table summary."""
        points: dict[str, ServedPoint] = {}
        maps: dict[str, RegisterMap] = {}
        by_address: dict[tuple[str, int], ServedPoint] = {}
        for table, rows in tables.items():
            reg_map = RegisterMap(table)
            for index, row in enumerate(rows):
                try:
                    point = ServedPoint.from_dict(table, row)
                    reg_map.add(point.spec)
                except (ValueError, TypeError, KeyError) as e:
                    raise ValueError(f'Invalid spec {index} for {table} table: {e}') from e
                if not point.spec.is_pad:
                    if point.topic in points:
                        raise ValueError(f'{point.topic} is registered twice')
                    points[point.topic] = point
                    by_address[(table, point.spec.address)] = point
            maps[table] = reg_map
        self.points, self.maps, self.by_address = points, maps, by_address
        return {table: {'points': sum(1 for p in points.values() if p.table == table), 'specs': len(reg_map)}
                for table, reg_map in maps.items()}

    def describe(self) -> dict:
        return {'role': 'slave', 'host': self.host, 'port': self.bound_port, 'unit_id': self.unit_id, 'points': len(self.points),
                'tables': {t: {'addresses': [m.address for m in reg_map]} for t, reg_map in self.maps.items()},
                'listening': self.server is not None}

    @property
    def bound_port(self) -> int:
        transport = getattr(self.server, 'transport', None)
        sockets = getattr(transport, 'sockets', None)
        return sockets[0].getsockname()[1] if sockets else self.port

    # ---- lifecycle ----------------------------------------------------------------------------------------------
    def _blocks(self) -> tuple[list[SimData], list[SimData], list[SimData], list[SimData]]:
        blocks = []
        for table in TABLES:
            reg_map = self.maps.get(table)
            specs = list(reg_map) if reg_map else []
            if not specs:
                blocks.append([SimData(address=0, count=1, values=False if table in BIT_TABLES else 0,
                                       datatype=DataType.BITS if table in BIT_TABLES else DataType.REGISTERS)])
                continue
            start, end = min(s.address for s in specs), max(s.end for s in specs)
            count = end - start + 1
            if table in BIT_TABLES:
                count = -(-count // 16) * 16                   # bit blocks come in whole registers
                blocks.append([SimData(address=start, count=count, values=False, datatype=DataType.BITS)])
            else:
                blocks.append([SimData(address=start, count=count, values=0, datatype=DataType.REGISTERS)])
        return blocks[0], blocks[1], blocks[2], blocks[3]

    async def start(self, values: dict[str, Any] | None = None, timeout: float = BIND_TIMEOUT):
        """(Re)build the datastore from the point table and serve it.

        A point's value comes from, in order: what this process already holds (a platform restart), the caller's
        current value in ``values`` (the platform's memory, after a proxy restart), the row's ``initial_value``. The
        number of points seeded from ``values`` is left in ``seeded``.
        """
        values = values or {}
        kept = {topic: value for topic, value in self.values.items() if topic in self.points}
        if self.server is not None:
            await self.server.shutdown()
            self.server = None
        device = SimDevice(id=self.unit_id, simdata=self._blocks(), action=self._action)
        self.server = ModbusTcpServer(device, address=(self.host, self.port))
        await self.server.serve_forever(background=True)
        self.values = {}
        self.seeded = 0
        for topic, point in self.points.items():
            if topic in kept:
                value, source = kept[topic], 'held'
            elif values.get(topic) is not None:
                value, source = values[topic], 'seed'
            else:
                value, source = point.initial_value, 'starting'
            if value in (None, ''):
                continue
            try:
                await self._store(point, value)
                self.seeded += source == 'seed'
            except (ValueError, TypeError) as e:
                _log.warning(f'{self.host}:{self.port}: {source} value {value!r} for {topic} ignored: {e}')
        _log.info(f'Serving Modbus unit {self.unit_id} on {self.host}:{self.bound_port} with {len(self.points)} points.')

    async def close(self):
        if self.server is not None:
            await self.server.shutdown()
            self.server = None

    # ---- values -------------------------------------------------------------------------------------------------
    async def _store(self, point: ServedPoint, value: Any):
        self._own_write = True
        try:
            encoded = point.spec.encode(value, point.table in BIT_TABLES)
            result = await self.server.context.async_setValues(self.unit_id, READ_CODE[point.table], point.spec.address, encoded)
        finally:
            self._own_write = False
        if result is not None:
            raise ValueError(f'datastore refused the write: {result.name}')
        self.values[point.topic] = point.spec.decode(encoded, point.table in BIT_TABLES)

    async def _current(self, point: ServedPoint):
        raw = await self.server.context.async_getValues(self.unit_id, READ_CODE[point.table], point.spec.address, point.spec.count)
        if isinstance(raw, ExcCodes):
            raise ValueError(f'datastore refused the read: {raw.name}')
        return point.spec.decode(raw, point.table in BIT_TABLES)

    async def read(self, topics: list[str] | None, **_) -> tuple[dict, dict]:
        topics = list(topics) if topics is not None else list(self.points)
        results, errors = {}, {}
        for topic in topics:
            point = self.points.get(topic)
            if point is None:
                errors[topic] = 'unregistered topic'
            elif self.server is None:
                errors[topic] = 'not serving'
            else:
                try:
                    results[topic] = await self._current(point)
                except ValueError as e:
                    errors[topic] = str(e)
        return results, errors

    async def write(self, values: dict[str, Any], **_) -> tuple[dict, dict]:
        """Store the platform's values in the served registers."""
        results, errors = {}, {}
        for topic, value in values.items():
            point = self.points.get(topic)
            if point is None:
                errors[topic] = 'unregistered topic'
                continue
            if self.server is None:
                errors[topic] = 'not serving'
                continue
            try:
                await self._store(point, value)
            except (ValueError, TypeError) as e:
                errors[topic] = f'bad value {value!r}: {e}'
                continue
            results[topic] = {'status': 'stored', 'value': value}
        return results, errors

    async def push_all(self) -> dict[str, Any]:
        values = {}
        if self.server is not None:
            for topic, point in self.points.items():
                if topic in self.values:
                    try:
                        values[topic] = await self._current(point)
                    except ValueError:
                        continue
        if values and self.push is not None:
            await self.push(values)
        return values

    # ---- the master's writes ------------------------------------------------------------------------------------
    async def _action(self, func_code: int, start_address: int, address: int, count: int, registers, values):
        """pymodbus calls this on every access, before applying it. Writes from the master are checked here."""
        if values is None or self._own_write:
            return None
        table = WRITE_TABLE.get(func_code)
        if table is None:
            return ExcCodes.ILLEGAL_FUNCTION
        reg_map = self.maps.get(table)
        affected = list(reg_map.in_range(address, address + len(values) - 1)) if reg_map else []
        covered = set()
        for spec in affected:
            point = self.by_address.get((table, spec.address))
            if spec.is_pad or point is None or not point.remote_writable:
                _log.info(f'{self.host}:{self.port}: refusing a write to {table} {address}+{len(values)}: '
                          f'{"pad" if spec.is_pad else "not remotely writable"} at {spec.address}.')
                return ExcCodes.ILLEGAL_ADDRESS
            covered.update(range(spec.address, spec.end + 1))
        if not affected or any(a not in covered for a in range(address, address + len(values))):
            _log.info(f'{self.host}:{self.port}: refusing a write to unregistered {table} address {address}+{len(values)}.')
            return ExcCodes.ILLEGAL_ADDRESS
        asyncio.get_running_loop().call_soon(lambda: asyncio.ensure_future(self._after_write(table, affected)))
        return None

    async def _after_write(self, table: str, specs: list[RegisterSpec]):
        values = {}
        for spec in specs:
            point = self.by_address.get((table, spec.address))
            if point is None:
                continue
            try:
                value = await self._current(point)
            except ValueError as e:
                _log.warning(f'{self.host}:{self.port}: unable to read back {point.topic} after a write: {e}')
                continue
            self.values[point.topic] = value
            values[point.topic] = value
        if values and self.push is not None:
            try:
                await self.push(values)
            except Exception as e:    # pragma: no cover
                _log.warning(f'{self.host}:{self.port}: unable to push the master\'s write: {e}')
