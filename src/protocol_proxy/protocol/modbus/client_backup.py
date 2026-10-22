import logging

from typing import Any, Coroutine, Iterable

from pymodbus import ModbusException
from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient, AsyncModbusTlsClient, AsyncModbusUdpClient
from pymodbus.pdu import ModbusPDU

_log = logging.getLogger(__name__)
logging.getLogger('pymodbus').setLevel(logging.INFO)


class ModbusClient:
    def __init__(self, client, device_address, device_type: str = 'tcp', modbus_port=502, **kwargs):
        self.client = client
        self.device_address = device_address
        self.device_type = device_type
        self.modbus_port = modbus_port

    @classmethod
    async def create(cls, device_address, device_type: str = 'tcp', modbus_port=502, **kwargs):
        match device_type:
            case 'serial':
                # TODO: Handle params: framer: FramerType = FramerType.RTU, baudrate: int = 19200, bytesize: int = 8,
                #  parity: str = 'N', stopbits: int = 1, handle_local_echo: bool = False, name: str = 'comm',
                #  reconnect_delay: float = 0.1, reconnect_delay_max: float = 300, timeout: float = 3, retries: int = 3,
                #  trace_packet: Callable[[bool, bytes], bytes] | None = None,
                #  trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
                #  trace_connect: Callable[[bool], None] | None = None)
                client = AsyncModbusSerialClient(**kwargs)
            case 'tcp':
                # TODO: Handle params: host: str, *, framer: FramerType = FramerType.SOCKET, port: int = 502,
                #  name: str = 'comm', source_address: tuple[str, int] | None = None, reconnect_delay: float = 0.1,
                #  reconnect_delay_max: float = 300, timeout: float = 3, retries: int = 3,
                #  trace_packet: Callable[[bool, bytes], bytes] | None = None,
                #  trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
                #  trace_connect: Callable[[bool], None] | None = None
                client = AsyncModbusTcpClient(device_address, port=modbus_port, **kwargs)  # Create client object
            case 'tls':
                # TODO: Handle params: host: str, *, sslctx: ~ssl.SSLContext = <ssl.SSLContext object>,
                #  framer: ~pymodbus.framer.base.FramerType = FramerType.TLS, port: int = 802, name: str = 'comm',
                #  source_address: tuple[str, int] | None = None, reconnect_delay: float = 0.1,
                #  reconnect_delay_max: float = 300, timeout: float = 3, retries: int = 3,
                #  trace_packet: ~collections.abc.Callable[[bool, bytes], bytes] | None = None,
                #  trace_pdu: ~collections.abc.Callable[[bool, ~pymodbus.pdu.pdu.ModbusPDU], ~pymodbus.pdu.pdu.ModbusPDU] | None = None,
                #  trace_connect: ~collections.abc.Callable[[bool], None] | None = None)
                client = AsyncModbusTlsClient(device_address, port=modbus_port, **kwargs) # TODO:
            case 'udp':
                # TODO: Handle params: host: str, *, framer: FramerType = FramerType.SOCKET, port: int = 502,
                #  name: str = 'comm', source_address: tuple[str, int] | None = None, reconnect_delay: float = 0.1,
                #  reconnect_delay_max: float = 300, timeout: float = 3, retries: int = 3,
                #  trace_packet: Callable[[bool, bytes], bytes] | None = None,
                #  trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
                #  trace_connect: Callable[[bool], None] | None = None
                client = AsyncModbusUdpClient(device_address, port=modbus_port, **kwargs)
            case _:
                raise ValueError(f"Unknown Modbus device type: {device_type}. Must be one of 'serial', 'tcp', 'tls', or 'udp'.")
        return cls(client, device_address, device_type, modbus_port, **kwargs)

    async def read(self, queries: Iterable[tuple[int, int]], register_map: str = 'holding', unit_id: int = 1, **kwargs):
        # TODO: This function reads from one register map on one device. Should it or another func handle many at once?
        responses = {'results': [], 'errors': []}
        async with self.client:
            for start_register, count in queries:
                try:
                    # TODO: Should we try to find/track pad registers here, if feasible, or in the interface?
                    # TODO: This may need different parameters for different query types. Needs more fleshed out.
                    # TODO: Utilize client.convert_from_registers (with client.DATATYPE enum).
                    # TODO: Consider how to handle various error conditions instead of just returning them.
                    #       For instance, should we try smaller chunks if we encounter and unknown register error?
                    # TODO: Should we track pad an unknown registers here?
                    request = self._get_read_coroutine(register_map, start_register, count=count, device_id=unit_id,
                                                       **kwargs)
                    if request is not None:
                        # TODO: Should we await each request if this is, say, a real TCP device?
                        response = await request
                        if response.isError():
                            responses['results'].append(None)
                            responses['errors'].append(response)
                        else:
                            responses['results'].append(response.bits if register_map == 'coils' else response.registers)
                            responses['errors'].append(None)
                except ModbusException as e:
                    responses['results'].append(None)
                    responses['errors'].append(f"Error in Modbus Client: {e}")
                except ValueError as e:
                    responses['results'].append(None)
                    responses['errors'].append(f"Error formulating Modbus query: {e}")
                except Exception as e:
                    responses['results'].append(None)
                    responses['errors'].append(f'Unexpected error while reading Modbus device: {e}')
        return responses

    async def write(self, device_address: str, start_register: int, count: int, values: list[int],
                    register_map: str = 'holding', unit_id: int = 1, **kwargs):
        responses = {'results': [], 'errors': []}
        async with self.client:
            try:
                # TODO: This may need different parameters for different query types. Needs more fleshed out.
                # TODO: Utilize client.convert_to_registers (with client.DATATYPE enum).
                request = self._get_write_coroutine(register_map, start_register, count=count, device_id=unit_id,
                                                   values=values, **kwargs)
                if request is not None:
                    response = await request
                    if response.isError():
                        responses['results'].append(None)
                        responses['errors'].append(response)
            except ModbusException as e:
                responses['results'].append(None)
                responses['errors'].append(f"Error in Modbus Client: {e}")
            except ValueError as e:
                responses['results'].append(None)
                responses['errors'].append(f"Error formulating Modbus query: {e}")
            except Exception as e:
                responses['results'].append(None)
                responses['errors'].append(f'Unexpected error while reading Modbus device: {e}')
            else:
                responses['results'].append(response.bits if register_map == 'coils' else response.registers)
                responses['errors'].append(None)
            return responses

    def _get_read_coroutine(self, register_map: str, start, **kwargs) -> Coroutine[Any, Any, ModbusPDU] | None:
        # TODO: Most take same params except as indicated below. Should probably validate each appropriately.
        #  Standard params are address (start), device_id (opt), no_repsonse_expected (opt), count
        match register_map:
            # TODO: Should report_device_id be one of these?
            case 'coil':
                return self.client.read_coils(start, **kwargs)
            case 'device_information':
                return self.client.read_device_information(**kwargs) # TODO: Different params: (no address nor count), read_code: int | None = None, object_id: int = 0
            case 'discrete_input':
                return self.client.read_discrete_inputs(start, **kwargs)
            case 'exception_status':
                return self.client.read_exception_status(**kwargs) # TODO: Different params: (no address nor count)
            case 'fifo_queue':
                return self.client.read_fifo_queue(**kwargs) # TODO: Different params: (no count)
            case 'file_record':
                return self.client.read_file_record(**kwargs) # TODO: Different params (no address nor count) records (list of FileRecord objects).
            case 'holding':
                return self.client.read_holding_registers(start, **kwargs)
            case 'input':
                return self.client.read_input_registers(start, **kwargs)
            case _:
                raise ValueError(f"Attempt to query unknown Modbus register map: {register_map}")

    def _get_write_coroutine(self, register_map: str, start, **kwargs) -> Coroutine[Any, Any, ModbusPDU] | None:
        match register_map:
            case 'holding':
                return self.client.write_registers(start, **kwargs)
            case 'coil':
                return self.client.write_coils(start, **kwargs)
            case _:
                raise ValueError(f"Attempt to query unknown Modbus register map: {register_map}")
