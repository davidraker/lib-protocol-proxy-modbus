"""The proxy's client over every transport, against the proxy's own served listeners: TCP, UDP, TLS with mutual
authentication, and serial over a bridged pseudo-terminal pair."""
import datetime
import ipaddress
import ssl

import pytest

from protocol_proxy.protocol.modbus.client import ModbusClient
from protocol_proxy.protocol.modbus.server import ModbusListener
from test_server import PtyBridge, TABLES, served_unit, wait_for

SPECS = {'holding': [{'address': 1003, 'data_type': '>f'}, {'address': 1010, 'data_type': 'uint16'}],
         'input': [{'address': 1001, 'data_type': '>f'}], 'coil': [{'address': 5, 'data_type': 'bool'}]}


def make_certs(tmp_path):
    """A CA, a server certificate for 127.0.0.1 and a client certificate, all signed by the CA."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
    now = datetime.datetime.now(datetime.timezone.utc)
    before, after = now - datetime.timedelta(days=1), now + datetime.timedelta(days=1)

    def write(name, cert, key):
        (tmp_path / f'{name}.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        (tmp_path / f'{name}.key').write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                                 serialization.NoEncryption()))
        return str(tmp_path / f'{name}.pem'), str(tmp_path / f'{name}.key')
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'modbus test CA')])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name).public_key(ca_key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(before).not_valid_after(after)
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(ca_key, hashes.SHA256()))
    ca_path, _ = write('ca', ca, ca_key)

    def leaf(name, usage, san=None):
        key = ec.generate_private_key(ec.SECP256R1())
        builder = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
                   .issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(before).not_valid_after(after)
                   .add_extension(x509.ExtendedKeyUsage([usage]), critical=False))
        if san:
            builder = builder.add_extension(x509.SubjectAlternativeName(san), critical=False)
        return write(name, builder.sign(ca_key, hashes.SHA256()), key)
    server_cert, server_key = leaf('server', ExtendedKeyUsageOID.SERVER_AUTH, [x509.IPAddress(ipaddress.ip_address('127.0.0.1'))])
    client_cert, client_key = leaf('client', ExtendedKeyUsageOID.CLIENT_AUTH)
    return {'ca': ca_path, 'server_cert': server_cert, 'server_key': server_key, 'client_cert': client_cert, 'client_key': client_key}


async def exercise(client: ModbusClient, listener: ModbusListener):
    """Read the starting values, write a holding register and a coil, and see the served side push them."""
    client.configure_registers(SPECS, unit_id=7)
    result = await client.read(register_map='input', unit_id=7, decode=True)
    assert result['errors'] == {} and result['results'] == {1001: 412.5}
    result = await client.read(register_map='holding', unit_id=7, decode=True)
    assert result['results'] == {1003: 1000.0, 1010: 3}
    result = await client.write([(1010, None, 44), (1003, None, 950.5)], register_map='holding', unit_id=7, encode=True)
    assert result['errors'] == [None, None]
    result = await client.write([(5, None, True)], register_map='coil', unit_id=7, encode=True)
    assert result['errors'] == [None]
    unit = listener.units[7]
    assert await wait_for(lambda: unit.pushes.latest('s/FanCmd') is True)
    assert unit.pushes.latest('s/Mode') == 44 and unit.pushes.latest('s/CO2Stpt') == 950.5


async def test_tcp_client():
    listener = ModbusListener('tcp', '127.0.0.1', 0)
    listener.add_unit(served_unit())
    await listener.start()
    try:
        client = await ModbusClient.create('127.0.0.1', 'tcp', port=listener.bound_port, timeout=3, retries=1)
        await exercise(client, listener)
        client.close()
    finally:
        await listener.stop()


async def test_udp_client():
    listener = ModbusListener('udp', '127.0.0.1', 0)
    listener.add_unit(served_unit())
    await listener.start()
    try:
        client = await ModbusClient.create('127.0.0.1', 'udp', port=listener.bound_port, timeout=3, retries=1)
        await exercise(client, listener)
        client.close()
    finally:
        await listener.stop()


async def test_tls_client_with_mutual_authentication(tmp_path):
    certs = make_certs(tmp_path)
    listener = ModbusListener('tls', '127.0.0.1', 0, cert_path=certs['server_cert'], key_path=certs['server_key'],
                              ca_path=certs['ca'], client_auth=True)
    listener.add_unit(served_unit())
    await listener.start()
    try:
        client = await ModbusClient.create('127.0.0.1', 'tls', port=listener.bound_port, timeout=3, retries=1,
                                           ca_path=certs['ca'], cert_path=certs['client_cert'], key_path=certs['client_key'])
        await exercise(client, listener)
        client.close()
        # Without a client certificate the server refuses the handshake.
        anonymous = await ModbusClient.create('127.0.0.1', 'tls', port=listener.bound_port, timeout=1, retries=0, ca_path=certs['ca'])
        result = await anonymous.read([(1010, 1)], register_map='holding', unit_id=7)
        assert result['errors'][0] and 'Not connected' in result['errors'][0] and result['results'] == [None]
        anonymous.close()
    finally:
        await listener.stop()


def test_tls_context_settings(tmp_path):
    certs = make_certs(tmp_path)
    ctx = ModbusClient.tls_context(certs['ca'], certs['client_cert'], certs['client_key'])
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname
    assert any(c['subject'] for c in ctx.get_ca_certs())                           # the CA is loaded
    unverified = ModbusClient.tls_context(None, tls_verify=False)
    assert unverified.verify_mode == ssl.CERT_NONE and not unverified.check_hostname
    with pytest.raises(FileNotFoundError):
        ModbusClient.tls_context(None, '/nonexistent/cert.pem')


async def test_serial_client():
    bridge = PtyBridge()
    listener = ModbusListener('serial', bridge.a_name, baudrate=19200)
    listener.add_unit(served_unit())
    try:
        await listener.start()
        client = await ModbusClient.create(bridge.b_name, 'serial', baudrate=19200, bytesize=8, parity='N', stopbits=1,
                                           timeout=3, retries=1)
        await exercise(client, listener)
        client.close()
    finally:
        await listener.stop()
        bridge.close()
