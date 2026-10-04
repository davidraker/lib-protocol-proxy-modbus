# Protocol Proxy Modbus Library
![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)
![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
[![Passing?](https://github.com/eclipse-volttron/lib-protocol-proxy-modbus/actions/workflows/run-tests.yml/badge.svg)](https://github.com/eclipse-volttron/lib-protocol-proxy-modbus/actions/workflows/run-tests.yml)
[![pypi version](https://img.shields.io/pypi/v/protocol-proxy-modbus.svg)](https://pypi.org/project/protocol-proxy-modbus/)

This library provides support for communication with Modbus devices to a
[Protocol Proxy](https://github.com/eclipse-volttron/lib-protocol-proxy) Manager.
Communication happens in a separate proxy process which holds the connections to the Modbus gateways and devices,
knows the data type of every configured register, and returns decoded values to its callers. One proxy process
serves any number of devices over any mix of transports (TCP, UDP, TLS, and serial).

## Automatically installed dependencies
- python = ">=3.10,<4.0"
- protocol-proxy = ">=2.0.0rc2"
- pymodbus = ">=3.13,<4.0"
- psutil = ">=5.9.0,<8.0.0"

[//]: # (# Documentation)

[//]: # (More detailed documentation can be found on [ReadTheDocs]&#40;https://eclipse-volttron.readthedocs.io/en/latest/external-docs/lib-protocol-proxy-modbus/index.html. The RST source)

[//]: # (of the documentation for this component is located in the "docs" directory of this repository.)

# Installation
This library, along with its dependencies, can be installed using pip:

```shell
pip install protocol-proxy-modbus
```

Note that this is rarely necessary as this library will typically be used as a dependency of an application acting as a
Protocol Proxy Manager (such as the [VOLTTRON Modbus Driver Interface](https://github.com/VOLTTRON/volttron-lib-modbus-driver)),
and will be installed as a dependency of that application.

# How it works

A Protocol Proxy Manager launches the proxy with:

```shell
python -m protocol_proxy.proxy protocol_proxy.protocol.modbus.modbus_proxy:ModbusProxy [manager options]
```

The Modbus proxy takes no protocol-specific launch options. Everything about a device arrives in messages:

| Message | Purpose |
|---|---|
| `REGISTER_DEVICE` | Create (or reuse) the client for a gateway and remember the caller's remote id (protocol-proxy header version 2) with the client and `unit_id`, which is how the caller's later reads, writes and register configuration name them. Carries `device_address`, `device_type` (`tcp`, `udp`, `tls`, `serial`), `port`, and pymodbus client options (`timeout`, `retries`, serial settings). May also carry the `CONFIGURE_REGISTERS` fields to declare the register map in the same round trip. |
| `CONFIGURE_REGISTERS` | Declare the data types of the registers on one unit: `unit_id`, `tables`, optional `max_gap` and `clear_others`. Listed tables are replaced whole; others are kept unless `clear_others` is true. The build is atomic: one bad entry rejects the whole message and leaves the current map untouched. |
| `READ_REGISTERS` | Read from one table (`register_map`) on one unit. `queries` are `[start, count]` pairs. With `decode` true, values are decoded using the declared data types and returned keyed by address; omit `queries` to read the whole table in the fewest requests. |
| `WRITE_REGISTERS` | Write to one table on one unit. `queries` are `[start, count, values]`. With `encode` true, `values` is the Python value for the point at `start` and is encoded using the declared data type. |

Replies have the form `{"result": ..., "error": ...}`. For decoded reads, `result` maps register address to value and
`error` maps the start address of any failed request to its error. For raw reads and for writes, both are lists aligned
with the queries.

## Register maps

A `tables` block maps a table name to a list of register specifications:

```json
{
  "unit_id": 1,
  "tables": {
    "input":   [{"address": 1001, "data_type": "FLOAT32"}, {"address": 1010, "data_type": "INT16"}],
    "holding": [{"address": 1003, "data_type": ">f"}, {"address": 1005, "data_type": "string[6]"},
                {"address": 1008, "data_type": "pad", "count": 2}, {"address": 1010, "data_type": "UINT16"}],
    "coil":    [{"address": 5, "data_type": "bool"}]
  }
}
```

- **Tables** are `coil`, `discrete_input`, `holding`, and `input`.
- **Data types** may be spelled as pymodbus names (`UINT16`, `FLOAT32`, `STRING`, `BITS`), as the names used by the
  modbus_tk driver (`float`, `uint16`, `string[8]`, `bool`), or as struct format strings (`>f`, `4H`, `8s`). Each
  specification may also carry `count`, `word_order` (`big` or `little`), and `string_encoding`.
- **Pads** are registers that are read but never decoded or returned. Two points separated by a pad are fetched in one
  request. `max_gap` lets the proxy read through short unconfigured gaps as well.

## Request planning

Requests to one gateway share one connection and run one at a time, so units on a serial bus behind a TCP gateway are
never polled concurrently. Different gateways are independent. Reads are merged into the fewest requests permitted by the
protocol (125 registers or 2000 coils). If a merged request is rejected by the device (for example an illegal data
address), its parts are retried individually so that one bad address does not fail its neighbours; a device that does not
answer at all is not retried piecemeal.

Requests may queue behind other units' timeout-and-retry cycles, so the proxy allows each message 120 seconds by default
before abandoning it (`ModbusProxy(callback_timeout=...)`). Callers should wait longer than that for a reply.

# Testing

```shell
pytest tests
```

The tests need no network or hardware. They exercise the register map, the client against a fake pymodbus device, and
every endpoint through the real IPC base class.

# Development
This library is maintained by the VOLTTRON Development Team.

Please see the following [guidelines](https://github.com/eclipse-volttron/volttron-core/blob/develop/CONTRIBUTING.md)
for contributing to this and/or other VOLTTRON repositories.

[//]: # (Please see the following helpful guide about [using the Protocol Proxy]&#40;https://github.com/eclipse-volttron/lib-protocol-proxy/blob/develop/developing_with_protocol_proxy.md&#41;)

[//]: # (in your VOLTTRON agent or other applications.)

# Disclaimer Notice

This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or any
information, apparatus, product, software, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.
