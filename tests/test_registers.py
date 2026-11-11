"""Tests for register data-type tracking: parse_data_type, RegisterSpec, and RegisterMap."""
import pytest

from protocol_proxy.protocol.modbus.registers import (DATATYPE, MAX_READ, PAD, QueryBlock, ReadBlock, RegisterMap,
                                                      RegisterSpec, parse_data_type, parse_type_spec, plan_ranges,
                                                      swap_bytes)


@pytest.mark.parametrize('type_string, expected', [
    ('>f', (DATATYPE.FLOAT32, None)),            # struct format, no length
    ('d', (DATATYPE.FLOAT64, None)),
    ('?', (DATATYPE.BITS, None)),
    ('8s', (DATATYPE.STRING, 4)),                # struct byte count -> registers
    ('4H', (DATATYPE.UINT16, 4)),                # struct repeat -> elements * size
    ('2x', (PAD, 1)),                            # struct pad bytes -> registers
    ('UINT16', (DATATYPE.UINT16, None)),         # pymodbus name
    ('Float64', (DATATYPE.FLOAT64, None)),       # case-insensitive
    ('float', (DATATYPE.FLOAT32, None)),         # modbus_tk name
    ('bool', (DATATYPE.BITS, None)),
    ('string[7]', (DATATYPE.STRING, 4)),         # characters -> registers, rounded up
    ('int32[2]', (DATATYPE.INT32, 4)),           # elements -> registers
    ('pad[3]', (PAD, 3)),
])
def test_parse_data_type(type_string, expected):
    assert parse_data_type(type_string) == expected


@pytest.mark.parametrize('bad', ['nope', '2f[3]', '', 'uint99', '[4]'])
def test_parse_data_type_rejects_unknown(bad):
    with pytest.raises(ValueError):
        parse_data_type(bad)


@pytest.mark.parametrize('type_string, little', [('>f', False), ('!H', False), ('f', False), ('<f', True), ('<H', True),
                                                  ('=i', True), ('@q', True), ('uint16', False)])
def test_parse_type_spec_byte_order(type_string, little):
    assert parse_type_spec(type_string)[2] is little
    assert parse_data_type(type_string) == parse_type_spec(type_string)[:2]


class TestLittleEndianTypes:
    """'<' types read the register byte stream little-endian, as the legacy pymodbus driver did with struct."""

    def test_spec_defaults(self):
        spec = RegisterSpec(0, '<f')
        assert spec.byte_swap is True and spec.word_order == 'little' and spec.count == 2
        assert RegisterSpec(0, '<f', word_order='big').word_order == 'big'        # explicit word order wins
        assert RegisterSpec(0, '>f').byte_swap is False

    @pytest.mark.parametrize('fmt, value', [('<H', 2 ** 16 - 1), ('<h', -(2 ** 16) // 2), ('<I', 2 ** 32 - 1),
                                            ('<i', (2 ** 32) // 2 - 1), ('<f', -1234.0), ('<Q', 2 ** 64 - 1),
                                            ('<q', -(2 ** 64) // 2), ('<d', 3.5)])
    def test_matches_struct_semantics(self, fmt, value):
        import struct
        spec = RegisterSpec(0, fmt)
        # A device holding this value would present registers whose big-endian byte stream is struct.pack(fmt).
        stream = struct.pack(fmt, value)
        registers = [int.from_bytes(stream[i:i + 2], 'big') for i in range(0, len(stream), 2)]
        assert spec.decode(registers, bit_table=False) == value
        assert spec.encode(value, bit_table=False) == registers

    def test_swap_bytes(self):
        assert swap_bytes([0x1234, 0x00ff]) == [0x3412, 0xff00]


class TestRegisterSpec:
    def test_defaults_count_from_type(self):
        assert RegisterSpec(1001, '>f').count == 2
        assert RegisterSpec(1001, DATATYPE.UINT64).count == 4
        assert RegisterSpec(1001, 'string').count == 1
        assert RegisterSpec(1001, PAD).count == 1

    def test_end_is_inclusive(self):
        spec = RegisterSpec(1001, '>f')
        assert spec.end == 1002

    @pytest.mark.parametrize('kwargs, message', [
        (dict(address=0, data_type='uint32', count=3), 'not a multiple'),
        (dict(address=0, data_type='uint16', count=0), 'must be positive'),
        (dict(address=-1, data_type='uint16'), 'non-negative'),
        (dict(address=0, data_type='uint16', word_order='middle'), 'word_order'),
    ])
    def test_rejects_invalid(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            RegisterSpec(**kwargs)

    def test_rejects_non_datatype(self):
        with pytest.raises(TypeError):
            RegisterSpec(0, 42)

    def test_float_round_trip(self):
        spec = RegisterSpec(1001, '>f')
        assert spec.encode(1.0, bit_table=False) == [0x3f80, 0]
        assert spec.decode([0x4049, 0x0fdb], bit_table=False) == pytest.approx(3.14159, abs=1e-4)

    def test_little_word_order_round_trip(self):
        spec = RegisterSpec(1, DATATYPE.FLOAT32, word_order='little')
        assert spec.encode(1.0, bit_table=False) == [0, 0x3f80]
        assert spec.decode([0, 0x3f80], bit_table=False) == 1.0

    def test_string_pads_to_configured_count(self):
        spec = RegisterSpec(10, 'string[6]')
        assert spec.count == 3
        assert spec.encode('ab', bit_table=False) == [0x6162, 0, 0]
        assert spec.decode([0x6162, 0, 0], bit_table=False) == 'ab'

    def test_little_endian_string_round_trip(self):
        spec = RegisterSpec(10, 'string[4]', word_order='little')
        assert spec.decode(spec.encode('abc', bit_table=False), bit_table=False) == 'abc'

    def test_array_round_trip_and_length_check(self):
        spec = RegisterSpec(20, 'uint16[3]')
        assert spec.encode([1, 2, 3], bit_table=False) == [1, 2, 3]
        assert spec.decode([1, 2, 3], bit_table=False) == [1, 2, 3]
        with pytest.raises(ValueError, match='encodes to 2 registers'):
            spec.encode([1, 2], bit_table=False)

    def test_bits_in_holding_register_unpack_to_16_bools(self):
        assert len(RegisterSpec(5, 'bits').decode([3], bit_table=False)) == 16

    def test_coils(self):
        single = RegisterSpec(7, 'bool')
        assert single.decode([1], bit_table=True) is True
        assert single.encode(True, bit_table=True) == [True]
        multi = RegisterSpec(7, 'bool', count=3)
        assert multi.decode([1, 0, 1], bit_table=True) == [True, False, True]
        assert multi.encode([1, 0, 1], bit_table=True) == [True, False, True]
        with pytest.raises(ValueError, match='holds 3 coils'):
            multi.encode([True], bit_table=True)

    def test_non_bits_type_rejected_on_coil_table(self):
        with pytest.raises(ValueError, match='only hold BITS'):
            RegisterSpec(7, 'uint16').decode([1], bit_table=True)

    def test_decode_requires_enough_values(self):
        with pytest.raises(ValueError, match='Expected 2 values'):
            RegisterSpec(1, '>f').decode([1], bit_table=False)

    def test_encode_coerces_strings(self):
        assert RegisterSpec(1, 'uint16').encode('7', bit_table=False) == [7]
        assert RegisterSpec(1, 'uint16').encode('0x10', bit_table=False) == [16]
        assert RegisterSpec(1, '>f').encode('1.0', bit_table=False) == [0x3f80, 0]
        assert RegisterSpec(1, 'uint16[2]').encode(['1', '2'], bit_table=False) == [1, 2]
        assert RegisterSpec(1, 'bool').encode('true', bit_table=True) == [True]
        assert RegisterSpec(1, 'bool', count=2).encode(['on', 'off'], bit_table=True) == [True, False]
        assert RegisterSpec(1, 'string[4]').encode('12', bit_table=False) == [0x3132, 0]    # strings stay strings
        with pytest.raises(ValueError):
            RegisterSpec(1, 'uint16').encode('seven', bit_table=False)
        with pytest.raises(ValueError, match='not a boolean'):
            RegisterSpec(1, 'bool').encode('maybe', bit_table=True)

    def test_pads_cannot_be_decoded_or_encoded(self):
        pad = RegisterSpec(0, PAD, 2)
        assert pad.is_pad
        with pytest.raises(ValueError):
            pad.decode([0, 0], bit_table=False)
        with pytest.raises(ValueError):
            pad.encode(1, bit_table=False)


@pytest.fixture
def holding_map():
    """1001-1002 float, 1003-1004 float, 1005-1009 pad, 1010 uint16 | 1100-1101 int32, 1102-1103 pad | 1200 pad, 1201 uint16"""
    m = RegisterMap('holding')
    m.add(RegisterSpec(1001, '>f'))
    m.add(RegisterSpec(1003, '>f'))
    m.add(RegisterSpec(1010, 'uint16'))
    m.add_pad(1005, 5)
    m.add(RegisterSpec(1100, 'int32'))
    m.add_pad(1102, 2)
    m.add_pad(1200, 1)
    m.add(RegisterSpec(1201, 'uint16'))
    return m


def blocks_of(reg_map_or_blocks):
    blocks = reg_map_or_blocks.blocks if isinstance(reg_map_or_blocks, RegisterMap) else reg_map_or_blocks
    return [(b.start, b.count) for b in blocks]


class TestRegisterMap:
    def test_unknown_table_rejected(self):
        with pytest.raises(ValueError, match='Unknown Modbus table'):
            RegisterMap('bogus')

    def test_negative_gap_rejected(self):
        with pytest.raises(ValueError):
            RegisterMap('holding', max_gap=-1)

    @pytest.mark.parametrize('address', [1004, 1002, 1000])   # 1000-1001 overlaps the float at 1001
    def test_overlap_rejected(self, holding_map, address):
        with pytest.raises(ValueError, match='overlaps'):
            holding_map.add(RegisterSpec(address, 'uint32'))

    def test_oversize_spec_rejected(self):
        with pytest.raises(ValueError, match='exceeds the 125 maximum'):
            RegisterMap('holding').add(RegisterSpec(0, 'string', count=126))

    def test_coil_table_rejects_register_types(self):
        with pytest.raises(ValueError, match='only holds BITS or PAD'):
            RegisterMap('coil').add(RegisterSpec(0, 'uint16'))

    def test_lookup(self, holding_map):
        assert len(holding_map) == 8
        assert holding_map.get(1003).address == 1003
        assert holding_map.get(1004) is None                 # get() is exact-start only
        assert holding_map.find(1004).address == 1003        # find() is containment
        assert holding_map.find(1009).is_pad
        assert holding_map.find(1050) is None
        assert 1002 in holding_map and 1050 not in holding_map
        assert [s.address for s in holding_map] == [1001, 1003, 1005, 1010, 1100, 1102, 1200, 1201]

    def test_in_range(self, holding_map):
        assert [s.address for s in holding_map.in_range(1002, 1100)] == [1001, 1003, 1005, 1010, 1100]
        assert list(holding_map.in_range(1011, 1099)) == []

    def test_blocks_bridge_explicit_pads_and_trim_edges(self, holding_map):
        # Pad 1005-1009 joins 1001-1004 to 1010; trailing pad 1102 and leading pad 1200 are not read.
        assert blocks_of(holding_map) == [(1001, 10), (1100, 2), (1201, 1)]

    def test_blocks_are_cached_and_invalidated(self, holding_map):
        assert holding_map.blocks is holding_map.blocks
        holding_map.remove(1005)
        assert blocks_of(holding_map)[:2] == [(1001, 4), (1010, 1)]

    def test_remove_returns_spec(self, holding_map):
        assert holding_map.remove(1010).address == 1010
        assert holding_map.get(1010) is None
        with pytest.raises(KeyError):
            holding_map.remove(1010)

    def test_max_gap_merges_small_unconfigured_gaps(self):
        m = RegisterMap('holding', max_gap=6)
        m.add(RegisterSpec(1003, '>f'))
        m.add(RegisterSpec(1010, 'uint16'))     # gap of 5 -> merged
        m.add(RegisterSpec(1020, 'uint16'))     # gap of 9 -> split
        assert blocks_of(m) == [(1003, 8), (1020, 1)]

    @pytest.mark.parametrize('table, span, size', [('input', 300, 2), ('coil', 4500, 1)])
    def test_blocks_split_at_protocol_limit(self, table, span, size):
        m = RegisterMap(table)
        kind = 'uint32' if table == 'input' else 'bool'
        for a in range(0, span, size):
            m.add(RegisterSpec(a, kind))
        limit = MAX_READ[table]
        blocks = m.blocks
        assert all(b.count <= limit for b in blocks)
        assert blocks[0].start == 0 and blocks[-1].end == span - 1
        assert sum(b.count for b in blocks) == span

    def test_decode_skips_pads(self, holding_map):
        raw = [0x3f80, 0, 0x4000, 0] + [9] * 5 + [42]
        assert holding_map.decode(holding_map.blocks[0], raw) == {1001: 1.0, 1003: 2.0, 1010: 42}

    def test_decode_requires_full_block(self, holding_map):
        with pytest.raises(ValueError, match='expected 10 values'):
            holding_map.decode(holding_map.blocks[0], [0] * 9)

    def test_plan_read_whole_map(self, holding_map):
        assert holding_map.plan_read() is holding_map.blocks

    def test_plan_read_subset_reads_through_unrequested_specs(self, holding_map):
        # 1003 lies between the requested 1001 and 1010; it is read as a pad rather than splitting the request.
        assert blocks_of(holding_map.plan_read([1001, 1010])) == [(1001, 10)]
        assert blocks_of(holding_map.plan_read([1001, 1201])) == [(1001, 2), (1201, 1)]
        assert blocks_of(holding_map.plan_read([1010])) == [(1010, 1)]
        assert holding_map.plan_read([]) == ()

    def test_plan_read_subset_decodes_only_requested(self, holding_map):
        block = holding_map.plan_read([1001, 1010])[0]
        raw = [0x3f80, 0, 0x4000, 0] + [9] * 5 + [42]
        assert holding_map.decode(block, raw) == {1001: 1.0, 1010: 42}

    def test_plan_read_unknown_address(self, holding_map):
        with pytest.raises(KeyError, match='1004'):
            holding_map.plan_read([1001, 1004])     # 1004 is inside a spec but is not a start address


def test_read_block_end():
    assert ReadBlock(10, 5, ()).end == 14


class TestPlanRanges:
    @staticmethod
    def plan(queries, max_read=125, max_gap=0):
        return [(b.start, b.count, b.queries) for b in plan_ranges(queries, max_read, max_gap)]

    def test_empty(self):
        assert plan_ranges([], 125) == []

    def test_disjoint_queries_stay_separate(self):
        assert self.plan([(1001, 2), (1010, 1)]) == [(1001, 2, (0,)), (1010, 1, (1,))]

    def test_adjacent_overlapping_and_unordered_queries_merge(self):
        assert self.plan([(1003, 2), (1001, 3), (1004, 1), (1005, 1)]) == [(1001, 5, (1, 0, 2, 3))]

    def test_duplicate_queries_merge(self):
        assert self.plan([(7, 1), (7, 1)]) == [(7, 1, (0, 1))]

    def test_max_gap(self):
        assert self.plan([(1, 2), (6, 1)], max_gap=2) == [(1, 2, (0,)), (6, 1, (1,))]   # gap of 3
        assert self.plan([(1, 2), (6, 1)], max_gap=3) == [(1, 6, (0, 1))]

    def test_protocol_limit_splits(self):
        assert self.plan([(0, 100), (100, 100)]) == [(0, 100, (0,)), (100, 100, (1,))]
        assert self.plan([(0, 60), (60, 60)]) == [(0, 120, (0, 1))]

    def test_oversize_single_query_passes_through(self):
        assert self.plan([(0, 300)]) == [(0, 300, (0,))]

    @pytest.mark.parametrize('bad', [(1, 0), (-1, 2)])
    def test_invalid_query(self, bad):
        with pytest.raises(ValueError):
            plan_ranges([bad], 125)

    def test_query_block_end(self):
        assert QueryBlock(10, 5, (0,)).end == 14


def test_array_spelling_is_an_alias_of_the_struct_form():
    from protocol_proxy.protocol.modbus.registers import parse_type_spec
    assert parse_type_spec('array(int16, 4)') == parse_type_spec('4h') == parse_type_spec('int16[4]')
    assert parse_type_spec('ARRAY(uint16,2)')[1] == 2


def test_single_register_boolean():
    from protocol_proxy.protocol.modbus.registers import RegisterSpec, DATATYPE
    spec = RegisterSpec(10, 'bool')
    assert spec.boolean and spec.count == 1 and spec.decode([0], False) is False and spec.decode([7], False) is True
    assert spec.encode(True, False) == [1] and spec.encode('false', False) == [0]
    assert RegisterSpec(3, 'bool').decode([True], True) is True                 # coil tables unchanged
    assert len(RegisterSpec(10, 'bits').decode([1], False)) == 16              # a bit field stays a list
    assert len(RegisterSpec(10, 'bits[2]').decode([1, 2], False)) == 32
    assert RegisterSpec(10, DATATYPE.BITS, 1, boolean=True).decode([2], False) is True   # as the interface declares it
    assert not RegisterSpec(10, DATATYPE.BITS, 1).boolean and not RegisterSpec(10, 'bool[2]').boolean
