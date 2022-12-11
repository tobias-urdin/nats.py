import sys
import ssl
import socket
import io
from typing import Union
from nats import errors
from nats.common import client as common_client
from nats.common import transport as common_transport
from urllib.parse import ParseResult


class TcpTransport(common_transport.Transport):

    def __init__(self):
        self._sock = None
        self._sock_file = None

    def _connect(
        self, uri: ParseResult, connect_timeout: int
    ):
        self._sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)

        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.settimeout(connect_timeout)

        self._sock.connect((uri.hostname, uri.port))
        self._sock_file = self._sock.makefile("rb")

    def _connect_tls(self, uri: ParseResult):
        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        self._sock = ctx.wrap_socket(self._sock, server_hostname=uri.hostname)
        self._sock_file = self._sock.makefile("rb")

    def _server_info(self, tls_required: bool = False):
        # TODO: verify server required tls
        #_, result = self._recv(INFO_RE)
        #server_info = json.loads(result.group(1))
        pass

    def connect(
        self, uri: ParseResult, buffer_size: int, connect_timeout: int
    ):
        self._connect(uri, connect_timeout)
        self._server_info()

    def connect_tls(
        self,
        uri: Union[str, ParseResult],
        ssl_context: ssl.SSLContext,
        buffer_size: int,
        connect_timeout: int,
    ):
        self._connect(uri, connect_timeout)
        self._server_info(tls_required=True)
        self._wrap_tls(uri)

    def write(self, payload):
        #print("write: %s" % str(payload))
        return self._sock.sendall(payload)

    def writelines(self, payload):
        pass

    def read(self, buffer_size: int):
        r = self._sock.recv(buffer_size)
        #print('read: type=%s data=%s' % (type(r), str(r)))
        return r

    def readline(self):
        read = io.BytesIO()
        while True:
            line = self._sock_file.readline()
            if not line:
                raise errors.UnexpectedEOF()

            read.write(line)

            if line.endswith(common_client._CRLF_):  # pragma: no branch
                break

        r = read.getvalue()
        #print('readline: %s' % str(r))
        return r

    def drain(self):
        pass

    def wait_closed(self):
        pass

    def close(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock_file.close()
        self._sock.close()

    def at_eof(self):
        pass

    def __bool__(self):
        return bool(self._sock)
