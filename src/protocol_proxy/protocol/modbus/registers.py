"""Register data-type tracking for the Modbus proxy client.

A :class:`RegisterMap` describes one Modbus table (coils, discrete inputs, holding or input registers) on one unit.
It holds a :class:`RegisterSpec` for every configured point, keyed by starting address, and derives from them the
minimal set of contiguous :class:`ReadBlock` requests needed to poll the table.

Pad registers are addresses which are read to keep a request contiguous but whose contents are never decoded or
returned. They may be configured explicitly (``PAD`` type) or inferred automatically when the gap between two
configured points is no larger than the map's ``max_gap``.
"""
from __future__ import annotations

import logging
import re

from bisect import bisect_left, bisect_right, insort
from enum import Enum
from typing import Any, Iterable, Iterator, Literal, NamedTuple, Sequence

from pymodbus.client.mixin import ModbusClientMixin

_log = logging.getLogger(__name__)

# pymodbus keeps its DATATYPE enum on the client mixin (it is not in pymodbus.constants).
DATATYPE = ModbusClientMixin.DATATYPE


class PadType(Enum):
    """Registers read only to keep a request contiguous. Values are never decoded or returned."""
    PAD = ('pad', 0)


PAD = PadType.PAD
RegisterDataType = DATATYPE | PadType
WordOrder = Literal['big', 'little']

BIT_TABLES = frozenset({'coil', 'discrete_input'})
REGISTER_TABLES = frozenset({'holding', 'input'})

# Maximum quantity per request permitted by the Modbus Application Protocol Specification V1.1b3.
MAX_READ: dict[str, int] = {'coil': 2000, 'discrete_input': 2000, 'holding': 125, 'input': 125}
MAX_WRITE: dict[str, int] = {'coil': 1968, 'holding': 123}

# Accepted spellings of data types. Includes pymodbus DATATYPE names, modbus_tk names, and struct format codes.
_TYPE_NAMES: dict[str, RegisterDataType] = {
    'int16': DATATYPE.INT16, 'short': DATATYPE.INT16,
    'uint16': DATATYPE.UINT16, 'ushort': DATATYPE.UINT16, 'word': DATATYPE.UINT16,
    'int32': DATATYPE.INT32, 'int': DATATYPE.INT32, 'long': DATATYPE.INT32,
    'uint32': DATATYPE.UINT32, 'uint': DATATYPE.UINT32, 'ulong': DATATYPE.UINT32, 'dword': DATATYPE.UINT32,
    'int64': DATATYPE.INT64, 'uint64': DATATYPE.UINT64,
    'float32': DATATYPE.FLOAT32, 'float': DATATYPE.FLOAT32, 'single': DATATYPE.FLOAT32,
    'float64': DATATYPE.FLOAT64, 'double': DATATYPE.FLOAT64,
    'string': DATATYPE.STRING, 'str': DATATYPE.STRING, 'char': DATATYPE.STRING,
    'bits': DATATYPE.BITS, 'bit': DATATYPE.BITS, 'bool': DATATYPE.BITS, 'boolean': DATATYPE.BITS,
    'pad': PAD, 'padding': PAD, 'reserved': PAD, 'skip': PAD,
}
_STRUCT_CODES: dict[str, RegisterDataType] = {
    'h': DATATYPE.INT16, 'H': DATATYPE.UINT16,
    'i': DATATYPE.INT32, 'l': DATATYPE.INT32, 'I': DATATYPE.UINT32, 'L': DATATYPE.UINT32,
    'q': DATATYPE.INT64, 'Q': DATATYPE.UINT64,
    'f': DATATYPE.FLOAT32, 'd': DATATYPE.FLOAT64,
    's': DATATYPE.STRING, '?': DATATYPE.BITS, 'x': PAD,
}
# Struct codes whose repeat count is a byte count rather than an element count.
_BYTE_COUNTED_CODES = frozenset({'s', 'x'})
# Spellings that mean one boolean when they name a register-table point (modbus_tk's bool), as opposed to 'bits'.
_BOOLEAN_SPELLINGS = frozenset({'bool', 'boolean', 'bit', '?'})
_TYPE_PATTERN = re.compile(r'^\s*([<>=!@]?)\s*(\d*)\s*([A-Za-z?_][A-Za-z0-9_]*)\s*(?:\[\s*(\d+)\s*\])?\s*$')
# modbus_tk's array spelling: array(int16, 4) is four int16 values, the same as the struct form 4h or int16[4].
_ARRAY_PATTERN = re.compile(r'^\s*array\s*\(\s*([A-Za-z0-9_]+)\s*,\s*(\d+)\s*\)\s*$', re.IGNORECASE)


def parse_data_type(type_string: str) -> tuple[RegisterDataType, int | None]:
    """Parse a data type string into a (data_type, register_count) pair. See parse_type_spec for the byte order."""
    data_type, count, _ = parse_type_spec(type_string)
    return data_type, count


def parse_type_spec(type_string: str) -> tuple[RegisterDataType, int | None, bool]:
    """Parse a data type string into (data_type, register_count, little_endian).

    Accepts pymodbus DATATYPE names (``'UINT16'``), modbus_tk names (``'float'``, ``'string[8]'``, ``'pad[2]'``)
    and struct format strings (``'>f'``, ``'<H'``, ``'8s'``, ``'4H'``, ``'2x'``). The returned count is in registers
    (or bits, for coil tables) and is None when the string does not specify a length.

    A ``<`` prefix (or the native-order prefixes ``=`` and ``@``, which mean little-endian on the machines VOLTTRON
    runs on) requests the legacy interpretation of the pymodbus-based driver: the value is the little-endian reading of
    the register byte stream, i.e. the registers are taken in reverse order with the bytes of each swapped.
    """
    if array := _ARRAY_PATTERN.match(type_string):
        type_string = f'{array.group(1)}[{array.group(2)}]'
    match = _TYPE_PATTERN.match(type_string)
    if not match:
        raise ValueError(f"Unrecognized Modbus data type: {type_string!r}")
    byte_order, repeat, name, length = match.groups()
    little_endian = byte_order in ('<', '=', '@')
    if repeat and length:
        raise ValueError(f"Data type {type_string!r} may not have both a struct repeat count and a [length].")
    if name in _STRUCT_CODES and (repeat or len(name) == 1 and name not in _TYPE_NAMES):
        data_type = _STRUCT_CODES[name]
        if not repeat:
            return data_type, None, little_endian
        n = int(repeat)
        if name in _BYTE_COUNTED_CODES:
            return data_type, (n + 1) // 2, little_endian
        return data_type, n * (data_type.value[1] or 1), little_endian
    key = name.lower()
    if key not in _TYPE_NAMES:
        raise ValueError(f"Unrecognized Modbus data type: {type_string!r}")
    data_type = _TYPE_NAMES[key]
    if length is None:
        return data_type, None, little_endian
    n = int(length)
    if data_type is DATATYPE.STRING:
        return data_type, (n + 1) // 2, little_endian          # length is in characters
    return data_type, n * (data_type.value[1] or 1), little_endian  # length in elements (registers for PAD/BITS)


def is_boolean_spelling(type_string: str) -> bool:
    """Whether a data type is written as a single boolean (``bool``, ``boolean``, ``bit``, struct ``?``) rather than
    as a bit field (``bits``, ``BITS``). In a register table the former decodes to one boolean, the latter to a list."""
    if array := _ARRAY_PATTERN.match(type_string):
        type_string = f'{array.group(1)}[{array.group(2)}]'
    match = _TYPE_PATTERN.match(type_string)
    return bool(match) and match.group(3).lower() in _BOOLEAN_SPELLINGS and not match.group(2)


def swap_bytes(registers: Sequence[int]) -> list[int]:
    """Swap the two bytes of every 16-bit register."""
    return [((r & 0xff) << 8) | ((r >> 8) & 0xff) for r in registers]


class RegisterSpec:
    """The data type and extent of one point (or pad) starting at a Modbus address."""
    __slots__ = ('address', 'data_type', 'count', 'word_order', 'string_encoding', 'byte_swap', 'boolean')

    def __init__(self, address: int, data_type: RegisterDataType | str, count: int | None = None,
                 word_order: WordOrder | None = None, string_encoding: str = 'utf-8', byte_swap: bool = False,
                 boolean: bool | None = None):
        """
        :param word_order: order of the registers making up a multi-register value. Defaults to 'big', or to
            'little' when data_type is a '<'-prefixed struct format.
        :param byte_swap: swap the two bytes of every register before decoding (after encoding). Set automatically
            for '<'-prefixed struct formats; together with word_order='little' this reads the register byte stream
            little-endian, as the legacy pymodbus driver did.
        :param boolean: a BITS point in a register table that is one boolean (non-zero register is true), as
            modbus_tk's ``bool`` was, rather than a list of bits. Inferred from the spelling of a string data_type.
        """
        if isinstance(data_type, str):
            if boolean is None:
                boolean = is_boolean_spelling(data_type)
            data_type, parsed_count, little_endian = parse_type_spec(data_type)
            if count is None:
                count = parsed_count
            if little_endian:
                byte_swap = True
                if word_order is None:
                    word_order = 'little'
        if word_order is None:
            word_order = 'big'
        if not isinstance(data_type, (DATATYPE, PadType)):
            raise TypeError(f"data_type must be a pymodbus DATATYPE or PAD, not {type(data_type).__name__}")
        size = data_type.value[1]
        if count is None:
            count = size or 1
        if not isinstance(address, int) or address < 0:
            raise ValueError(f"Register address must be a non-negative integer, not {address!r}")
        if count < 1:
            raise ValueError(f"Register count must be positive, not {count}")
        if size and count % size:
            raise ValueError(f"Count {count} at address {address} is not a multiple of {data_type.name} size {size}")
        if word_order not in ('big', 'little'):
            raise ValueError(f"word_order must be 'big' or 'little', not {word_order!r}")
        self.address = address
        self.data_type = data_type
        self.count = count
        self.word_order = word_order
        self.string_encoding = string_encoding
        self.byte_swap = bool(byte_swap)
        self.boolean = bool(boolean) and data_type is DATATYPE.BITS and count == 1

    @property
    def is_pad(self) -> bool:
        return self.data_type is PAD

    @property
    def end(self) -> int:
        """Last address (inclusive) occupied by this spec."""
        return self.address + self.count - 1

    def decode(self, raw: Sequence[int | bool], bit_table: bool) -> Any:
        """Decode the raw registers (or coil bits) for this spec into a Python value."""
        if self.is_pad:
            raise ValueError(f"Pad registers at {self.address} cannot be decoded.")
        if len(raw) < self.count:
            raise ValueError(f"Expected {self.count} values for address {self.address}, received {len(raw)}")
        raw = list(raw[:self.count])
        if bit_table:
            if self.data_type is not DATATYPE.BITS:
                raise ValueError(f"Coil tables only hold BITS, not {self.data_type.name} (address {self.address})")
            bits = [bool(b) for b in raw]
            return bits[0] if self.count == 1 else bits
        if self.byte_swap:
            raw = swap_bytes(raw)
        if self.boolean:
            return raw[0] != 0          # a boolean held in one register (modbus_tk's bool): non-zero is true
        return ModbusClientMixin.convert_from_registers(raw, self.data_type, self.word_order, self.string_encoding)

    def encode(self, value: Any, bit_table: bool) -> list[int] | list[bool]:
        """Encode a Python value into the registers (or coil bits) for this spec.

        Strings are converted to the type the data type needs ('7' -> 7, '1.5' -> 1.5, 'true' -> True), since
        callers such as command-line tools deliver values as text.
        """
        if self.is_pad:
            raise ValueError(f"Pad registers at {self.address} cannot be written.")
        value = self._coerce(value, bit_table)
        if bit_table:
            bits = [bool(v) for v in value] if isinstance(value, (list, tuple)) else [bool(value)]
            if len(bits) != self.count:
                raise ValueError(f"Address {self.address} holds {self.count} coils, received {len(bits)} values")
            return bits
        if self.data_type is DATATYPE.STRING:
            registers = ModbusClientMixin.convert_to_registers(value, self.data_type, 'big', self.string_encoding)
            if len(registers) < self.count:
                registers.extend([0] * (self.count - len(registers)))
            if self.word_order == 'little':
                registers.reverse()
        elif self.boolean and not bit_table:
            registers = [1 if value else 0]
        else:
            if self.data_type is DATATYPE.BITS and not isinstance(value, list):
                value = list(value)
            registers = ModbusClientMixin.convert_to_registers(value, self.data_type, self.word_order)
        if len(registers) != self.count:
            raise ValueError(f"Value {value!r} encodes to {len(registers)} registers,"
                             f" but address {self.address} holds {self.count}")
        return swap_bytes(registers) if self.byte_swap else registers

    def _coerce(self, value: Any, bit_table: bool) -> Any:
        if isinstance(value, (list, tuple)):
            return [self._coerce(v, bit_table) for v in value]
        if not isinstance(value, str) or self.data_type is DATATYPE.STRING:
            return value
        text = value.strip()
        if bit_table or self.data_type is DATATYPE.BITS:
            if text.lower() in ('true', 't', 'on', 'yes', '1'):
                return True
            if text.lower() in ('false', 'f', 'off', 'no', '0'):
                return False
            raise ValueError(f"{value!r} is not a boolean")
        if self.data_type in (DATATYPE.FLOAT32, DATATYPE.FLOAT64):
            return float(text)
        return int(text, 0)

    def __repr__(self) -> str:
        return (f"RegisterSpec({self.address}, {self.data_type.name}, count={self.count},"
                f" word_order={self.word_order!r}{', byte_swap=True' if self.byte_swap else ''}"
                f"{', boolean=True' if self.boolean else ''})")


class ReadBlock(NamedTuple):
    """One contiguous read request and the specs whose values it satisfies."""
    start: int
    count: int
    specs: tuple[RegisterSpec, ...]

    @property
    def end(self) -> int:
        return self.start + self.count - 1


class QueryBlock(NamedTuple):
    """One contiguous read request covering one or more caller-supplied (start, count) queries."""
    start: int
    count: int
    queries: tuple[int, ...]     # Indices into the caller's query list.

    @property
    def end(self) -> int:
        return self.start + self.count - 1


def plan_ranges(queries: Sequence[tuple[int, int]], max_read: int, max_gap: int = 0) -> list[QueryBlock]:
    """Merge (start, count) queries into the fewest contiguous requests.

    Queries which overlap, touch, or are separated by no more than max_gap unread addresses are merged, provided the
    merged request does not exceed max_read. A single query larger than max_read is issued as-is. Each returned block
    records which of the caller's queries it satisfies, so responses can be sliced back per query.
    """
    blocks: list[QueryBlock] = []
    if not queries:
        return blocks
    order = sorted(range(len(queries)), key=lambda i: queries[i][0])
    start, end, members = None, None, []
    for i in order:
        q_start, q_count = queries[i]
        if q_count < 1 or q_start < 0:
            raise ValueError(f"Query ({q_start}, {q_count}) must have a non-negative start and a positive count.")
        q_end = q_start + q_count - 1
        if start is not None and q_start - end - 1 <= max_gap and max(end, q_end) - start + 1 <= max_read:
            end = max(end, q_end)
            members.append(i)
            continue
        if start is not None:
            blocks.append(QueryBlock(start, end - start + 1, tuple(members)))
        start, end, members = q_start, q_end, [i]
    blocks.append(QueryBlock(start, end - start + 1, tuple(members)))
    return blocks


class RegisterMap:
    """Specs for one Modbus table on one unit, with cached contiguous read blocks.

    Specs are stored in a dict keyed by starting address for O(1) lookup, alongside a sorted list of starting
    addresses so that containment and range queries are O(log n) and read blocks can be built in one pass.
    """
    __slots__ = ('table', 'max_gap', '_specs', '_starts', '_blocks')

    def __init__(self, table: str, max_gap: int = 0):
        if table not in MAX_READ:
            raise ValueError(f"Unknown Modbus table {table!r}. Must be one of: {', '.join(sorted(MAX_READ))}")
        if max_gap < 0:
            raise ValueError("max_gap must be non-negative")
        self.table = table
        self.max_gap = max_gap                   # Largest unconfigured gap to read through rather than split.
        self._specs: dict[int, RegisterSpec] = {}
        self._starts: list[int] = []
        self._blocks: tuple[ReadBlock, ...] | None = None

    @property
    def bit_table(self) -> bool:
        return self.table in BIT_TABLES

    @property
    def max_read(self) -> int:
        return MAX_READ[self.table]

    def __len__(self) -> int:
        return len(self._specs)

    def __iter__(self) -> Iterator[RegisterSpec]:
        return (self._specs[start] for start in self._starts)

    def __contains__(self, address: int) -> bool:
        return self.find(address) is not None

    def add(self, spec: RegisterSpec) -> RegisterSpec:
        """Add a spec, rejecting any overlap with existing specs."""
        if spec.count > self.max_read:
            raise ValueError(f"{spec!r} exceeds the {self.max_read} maximum for one {self.table} request")
        if self.bit_table and spec.data_type not in (DATATYPE.BITS, PAD):
            raise ValueError(f"{self.table} table only holds BITS or PAD, not {spec.data_type.name}")
        idx = bisect_right(self._starts, spec.address)
        if idx and self._specs[self._starts[idx - 1]].end >= spec.address:
            raise ValueError(f"{spec!r} overlaps {self._specs[self._starts[idx - 1]]!r}")
        if idx < len(self._starts) and self._starts[idx] <= spec.end:
            raise ValueError(f"{spec!r} overlaps {self._specs[self._starts[idx]]!r}")
        self._specs[spec.address] = spec
        insort(self._starts, spec.address)
        self._blocks = None
        return spec

    def add_pad(self, address: int, count: int = 1) -> RegisterSpec:
        return self.add(RegisterSpec(address, PAD, count))

    def remove(self, address: int) -> RegisterSpec:
        spec = self._specs.pop(address)
        del self._starts[bisect_left(self._starts, address)]
        self._blocks = None
        return spec

    def get(self, address: int) -> RegisterSpec | None:
        """Return the spec starting exactly at address, if any."""
        return self._specs.get(address)

    def find(self, address: int) -> RegisterSpec | None:
        """Return the spec whose extent contains address, if any."""
        idx = bisect_right(self._starts, address)
        if idx:
            spec = self._specs[self._starts[idx - 1]]
            if spec.end >= address:
                return spec
        return None

    def in_range(self, start: int, end: int) -> Iterator[RegisterSpec]:
        """Yield specs overlapping the inclusive address range [start, end], in address order."""
        idx = bisect_right(self._starts, start)
        if idx and self._specs[self._starts[idx - 1]].end >= start:
            idx -= 1
        for spec_start in self._starts[idx:]:
            if spec_start > end:
                break
            yield self._specs[spec_start]

    @property
    def blocks(self) -> tuple[ReadBlock, ...]:
        """Contiguous read requests covering every non-pad spec. Cached until the map changes."""
        if self._blocks is None:
            self._blocks = self._build_blocks(self)
        return self._blocks

    def plan_read(self, addresses: Iterable[int] | None = None) -> tuple[ReadBlock, ...]:
        """Read blocks covering the specs at the given starting addresses, or the whole map if None."""
        if addresses is None:
            return self.blocks
        wanted = set(addresses)
        missing = sorted(a for a in wanted if a not in self._specs)
        if missing:
            raise KeyError(f"No {self.table} register configured at address(es): {missing}")
        if not wanted:
            return ()
        # Configured specs lying between wanted ones are treated as pads: reading through them costs nothing extra
        # and avoids splitting the request, but their values are not decoded.
        first, last = min(wanted), max(wanted)
        specs = [s if s.is_pad or s.address in wanted else RegisterSpec(s.address, PAD, s.count)
                 for s in self.in_range(first, self._specs[last].end)]
        return self._build_blocks(specs)

    def _build_blocks(self, specs: Iterable[RegisterSpec]) -> tuple[ReadBlock, ...]:
        blocks: list[ReadBlock] = []
        current: list[RegisterSpec] = []

        def flush():
            while current and current[-1].is_pad:   # Never read trailing pads.
                current.pop()
            if current:
                blocks.append(ReadBlock(current[0].address, current[-1].end - current[0].address + 1, tuple(current)))
            current.clear()

        for spec in specs:
            if current:
                gap = spec.address - current[-1].end - 1
                if gap > self.max_gap or spec.end - current[0].address + 1 > self.max_read:
                    flush()
            if not current and spec.is_pad:       # Never start a block with a pad.
                continue
            current.append(spec)
        flush()
        return tuple(blocks)

    def decode(self, block: ReadBlock, raw: Sequence[int | bool]) -> dict[int, Any]:
        """Decode a block's raw response into {address: value}, skipping pads and inferred gaps."""
        if len(raw) < block.count:
            raise ValueError(f"Block at {block.start} expected {block.count} values, received {len(raw)}")
        values: dict[int, Any] = {}
        for spec in block.specs:
            if spec.is_pad:
                continue
            offset = spec.address - block.start
            values[spec.address] = spec.decode(raw[offset:offset + spec.count], self.bit_table)
        return values
