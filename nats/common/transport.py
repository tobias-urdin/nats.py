import abc
import ssl
from typing import Union, List
from urllib.parse import ParseResult


class Transport(abc.ABC):

    @abc.abstractmethod
    def connect(
        self, uri: ParseResult, buffer_size: int, connect_timeout: int
    ):
        """
        Connects to a server using the implemented transport. The uri passed is of type ParseResult that can be
        obtained calling urllib.parse.urlparse.
        """
        pass

    @abc.abstractmethod
    def connect_tls(
        self,
        uri: Union[str, ParseResult],
        ssl_context: ssl.SSLContext,
        buffer_size: int,
        connect_timeout: int,
    ):
        """
        connect_tls is similar to connect except it tries to connect to a secure endpoint, using the provided ssl
        context. The uri can be provided as string in case the hostname differs from the uri hostname, in case it
        was provided as 'tls_hostname' on the options.
        """
        pass

    @abc.abstractmethod
    def write(self, payload: bytes):
        """
        Write bytes to underlying transport. Needs a call to drain() to be successfully written.
        """
        pass

    @abc.abstractmethod
    def writelines(self, payload: List[bytes]):
        """
        Writes a list of bytes, one by one, to the underlying transport. Needs a call to drain() to be successfully
        written.
        """
        pass

    @abc.abstractmethod
    def read(self, buffer_size: int) -> bytes:
        """
        Reads a sequence of bytes from the underlying transport, up to buffer_size. The buffer_size is ignored in case
        the transport carries already frames entire messages (i.e. websocket).
        """
        pass

    @abc.abstractmethod
    def readline(self) -> bytes:
        """
        Reads one whole frame of bytes (or message) from the underlying transport.
        """
        pass

    @abc.abstractmethod
    def drain(self):
        """
        Flushes the bytes queued for transmission when calling write() and writelines().
        """
        pass

    @abc.abstractmethod
    def wait_closed(self):
        """
        Waits until the connection is successfully closed.
        """
        pass

    @abc.abstractmethod
    def close(self):
        """
        Closes the underlying transport.
        """
        pass

    @abc.abstractmethod
    def at_eof(self) -> bool:
        """
        Returns if underlying transport is at eof.
        """
        pass

    @abc.abstractmethod
    def __bool__(self):
        """
        Returns if the transport was initialized, either by calling connect of connect_tls.
        """
        pass
