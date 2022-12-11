import abc
import ipaddress
from dataclasses import dataclass
import ssl
import json
from random import shuffle
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Union,
    List
)
from urllib.parse import ParseResult, urlparse

from nats.nuid import NUID

from nats.common.subscription import (
    DEFAULT_SUB_PENDING_MSGS_LIMIT,
    DEFAULT_SUB_PENDING_BYTES_LIMIT,
    Subscription,
)

from nats.common.msg import Msg


__version__ = '2.2.0'
__lang__ = 'python3'

PROTOCOL = 1
DEFAULT_INBOX_PREFIX = b'_INBOX'

DEFAULT_PENDING_SIZE = 2 * 1024 * 1024
DEFAULT_BUFFER_SIZE = 32768
DEFAULT_RECONNECT_TIME_WAIT = 2  # in seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 60
DEFAULT_PING_INTERVAL = 120  # in seconds
DEFAULT_MAX_OUTSTANDING_PINGS = 2
DEFAULT_MAX_PAYLOAD_SIZE = 1048576
DEFAULT_MAX_FLUSHER_QUEUE_SIZE = 1024
DEFAULT_FLUSH_TIMEOUT = 10  # in seconds
DEFAULT_CONNECT_TIMEOUT = 2  # in seconds
DEFAULT_DRAIN_TIMEOUT = 30  # in seconds
MAX_CONTROL_LINE_SIZE = 1024

JWTCallback = Callable[[], Union[bytearray, bytes]]
Credentials = Union[str, Tuple[str, str]]
SignatureCallback = Callable[[str], bytes]

INFO_OP = b'INFO'
CONNECT_OP = b'CONNECT'
PING_OP = b'PING'
PONG_OP = b'PONG'
OK_OP = b'+OK'
ERR_OP = b'-ERR'
_CRLF_ = b'\r\n'
_CRLF_LEN_ = len(_CRLF_)
_SPC_ = b' '
_SPC_BYTE_ = 32
EMPTY = ""

PING_PROTO = PING_OP + _CRLF_
PONG_PROTO = PONG_OP + _CRLF_


@dataclass
class Srv:
    """
    Srv is a helper data structure to hold state of a server.
    """
    uri: ParseResult
    reconnects: int = 0
    last_attempt: Optional[float] = None
    did_connect: bool = False
    discovered: bool = False
    tls_name: Optional[str] = None
    server_version: Optional[str] = None


class ServerVersion:

    def __init__(self, server_version: str):
        self._server_version = server_version
        self._major_version = None
        self._minor_version = None
        self._patch_version = None
        self._dev_version = None

    def parse_version(self):
        v = (self._server_version).split('-')
        if len(v) > 1:
            self._dev_version = v[1]
        tokens = v[0].split('.')
        n = len(tokens)
        if n > 1:
            self._major_version = int(tokens[0])
        if n > 2:
            self._minor_version = int(tokens[1])
        if n > 3:
            self._patch_version = int(tokens[2])

    @property
    def major(self) -> int:
        version = self._major_version
        if not version:
            self.parse_version()
        return self._major_version

    @property
    def minor(self) -> int:
        version = self._minor_version
        if not version:
            self.parse_version()
        return self._minor_version

    @property
    def patch(self) -> int:
        version = self._patch_version
        if not version:
            self.parse_version()
        return self._patch_version

    @property
    def dev(self) -> int:
        version = self._dev_version
        if not version:
            self.parse_version()
        return self._dev_version

    def __repr__(self) -> str:
        return f"<nats server v{self._server_version}>"


class Client(abc.ABC):

    # FIXME: Use an enum instead.
    DISCONNECTED = 0
    CONNECTED = 1
    CLOSED = 2
    RECONNECTING = 3
    CONNECTING = 4
    DRAINING_SUBS = 5
    DRAINING_PUBS = 6

    def __repr__(self) -> str:
        return f"<nats client v{__version__}>"

    def __init__(self) -> None:
        self._current_server: Optional[Srv] = None
        self._server_info: Dict[str, Any] = {}
        self._server_pool: List[Srv] = []
        self._transport: Optional[Transport] = None
        self._err: Optional[Exception] = None

        self._max_payload: int = DEFAULT_MAX_PAYLOAD_SIZE

        # client id that the NATS server knows about.
        self._client_id: Optional[str] = None
        self._status: int = Client.DISCONNECTED

        self._nuid = NUID()
        self._inbox_prefix = bytearray(DEFAULT_INBOX_PREFIX)

        # NKEYS support
        #
        # user_jwt_cb is used to fetch and return the account
        # signed JWT for this user.
        self._user_jwt_cb: Optional[JWTCallback] = None

        # signature_cb is used to sign a nonce from the server while
        # authenticating with nkeys. The user should sign the nonce and
        # return the base64 encoded signature.
        self._signature_cb: Optional[SignatureCallback] = None

        # user credentials file can be a tuple or single file.
        self._user_credentials: Optional[Credentials] = None

        # file that contains the nkeys seed and its public key as a string.
        self._nkeys_seed: Optional[str] = None
        self._public_nkey: Optional[str] = None

        self.options: Dict[str, Any] = {}
        self.stats = {
            'in_msgs': 0,
            'out_msgs': 0,
            'in_bytes': 0,
            'out_bytes': 0,
            'reconnects': 0,
            'errors_received': 0,
        }

    @abc.abstractmethod
    def connect(
        self,
        servers: Union[str, List[str]] = ["nats://localhost:4222"],
        error_cb = None,
        disconnected_cb = None,
        closed_cb = None,
        discovered_server_cb = None,
        reconnected_cb = None,
        name: Optional[str] = None,
        pedantic: bool = False,
        verbose: bool = False,
        allow_reconnect: bool = True,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
        reconnect_time_wait: int = DEFAULT_RECONNECT_TIME_WAIT,
        max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        max_outstanding_pings: int = DEFAULT_MAX_OUTSTANDING_PINGS,
        dont_randomize: bool = False,
        flusher_queue_size: int = DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
        no_echo: bool = False,
        tls: Optional[ssl.SSLContext] = None,
        tls_hostname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        drain_timeout: int = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: Optional[SignatureCallback] = None,
        user_jwt_cb: Optional[JWTCallback] = None,
        user_credentials: Optional[Credentials] = None,
        nkeys_seed: Optional[str] = None,
        inbox_prefix: Union[str, bytes] = DEFAULT_INBOX_PREFIX,
        pending_size: int = DEFAULT_PENDING_SIZE,
        flush_timeout: Optional[float] = None,
    ) -> None:
        self._setup_server_pool(servers)

        # NKEYS support
        self._signature_cb = signature_cb
        self._user_jwt_cb = user_jwt_cb
        self._user_credentials = user_credentials
        self._nkeys_seed = nkeys_seed

        # Customizable options
        self.options["verbose"] = verbose
        self.options["pedantic"] = pedantic
        self.options["name"] = name
        self.options["allow_reconnect"] = allow_reconnect
        self.options["dont_randomize"] = dont_randomize
        self.options["reconnect_time_wait"] = reconnect_time_wait
        self.options["max_reconnect_attempts"] = max_reconnect_attempts
        self.options["ping_interval"] = ping_interval
        self.options["max_outstanding_pings"] = max_outstanding_pings
        self.options["no_echo"] = no_echo
        self.options["user"] = user
        self.options["password"] = password
        self.options["token"] = token
        self.options["connect_timeout"] = connect_timeout
        self.options["drain_timeout"] = drain_timeout

        if tls:
            self.options['tls'] = tls
        if tls_hostname:
            self.options['tls_hostname'] = tls_hostname

        if self._user_credentials is not None or self._nkeys_seed is not None:
            self._setup_nkeys_connect()

        # Custom inbox prefix
        if isinstance(inbox_prefix, str):
            inbox_prefix = inbox_prefix.encode()
        assert isinstance(inbox_prefix, bytes)
        self._inbox_prefix = bytearray(inbox_prefix)

        # Max size of buffer used for flushing commands to the server.
        self._max_pending_size = pending_size

        # Max duration for a force flush (happens when a buffer is full).
        self._flush_timeout = flush_timeout

        if self.options["dont_randomize"] is False:
            shuffle(self._server_pool)

    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    def drain(self) -> None:
        pass

    @abc.abstractmethod
    def publish(
        self,
        subject: str,
        payload: bytes = b'',
        reply: str = '',
        headers: Optional[Dict[str, str]] = None
    ) -> None:
        pass

    @abc.abstractmethod
    def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb = None,
        future = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> Subscription:
        pass

    @abc.abstractmethod
    def request(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: float = 0.5,
        old_style: bool = False,
        headers: Dict[str, Any] = None,
    ) -> Msg:
        pass

    def new_inbox(self) -> str:
        """
        new_inbox returns a unique inbox that can be used
        for NATS requests or subscriptions::

           # Create unique subscription to receive direct messages.
           inbox = nc.new_inbox()
           sub = await nc.subscribe(inbox)
           nc.publish('broadcast', b'', reply=inbox)
           msg = sub.next_msg()
        """
        next_inbox = self._inbox_prefix[:]
        next_inbox.extend(b'.')
        next_inbox.extend(self._nuid.next())
        return next_inbox.decode()

    @abc.abstractmethod
    def flush(self, timeout: int = DEFAULT_FLUSH_TIMEOUT) -> None:
        pass

    def __aenter__(self) -> "Client":
        pass

    def __aexit__(self, *exc_info) -> None:
        pass

    @property
    def connected_url(self) -> Optional[ParseResult]:
        if self._current_server and self.is_connected:
            return self._current_server.uri
        return None

    @property
    def servers(self) -> List[ParseResult]:
        servers = []
        for srv in self._server_pool:
            servers.append(srv.uri)
        return servers

    @property
    def discovered_servers(self) -> List[ParseResult]:
        servers = []
        for srv in self._server_pool:
            if srv.discovered:
                servers.append(srv.uri)
        return servers

    @property
    def max_payload(self) -> int:
        """
        Returns the max payload which we received from the servers INFO
        """
        return self._max_payload

    @property
    def client_id(self) -> Optional[str]:
        """
        Returns the client id which we received from the servers INFO
        """
        return self._client_id

    @property
    def last_error(self) -> Optional[Exception]:
        """
        Returns the last error which may have occurred.
        """
        return self._err

    @property
    def pending_data_size(self) -> int:
        return self._pending_data_size

    @property
    def is_closed(self) -> bool:
        return self._status == Client.CLOSED

    @property
    def is_reconnecting(self) -> bool:
        return self._status == Client.RECONNECTING

    @property
    def is_connected(self) -> bool:
        return (self._status == Client.CONNECTED) or self.is_draining

    @property
    def is_connecting(self) -> bool:
        return self._status == Client.CONNECTING

    @property
    def is_draining(self) -> bool:
        return (
            self._status == Client.DRAINING_SUBS
            or self._status == Client.DRAINING_PUBS
        )

    @property
    def is_draining_pubs(self) -> bool:
        return self._status == Client.DRAINING_PUBS

    @property
    def connected_server_version(self) -> ServerVersion:
        """
        Returns the ServerVersion of the server to which the client
        is currently connected.
        """
        if self._current_server and self._current_server.server_version:
            return ServerVersion(self._current_server.server_version)
        return ServerVersion("0.0.0-unknown")

    @property
    def ssl_context(self) -> ssl.SSLContext:
        ssl_context: Optional[ssl.SSLContext] = None
        if "tls" in self.options:
            ssl_context = self.options.get('tls')
        else:
            ssl_context = ssl.create_default_context()
        if ssl_context is None:
            raise errors.Error('nats: no ssl context provided')
        return ssl_context

    # Helper code below this point that does not define the baseclass
    # interface for a client.

    def _setup_server_pool(self, connect_url: Union[str, List[str]]) -> None:
        if isinstance(connect_url, str):
            try:
                if "nats://" in connect_url or "tls://" in connect_url:
                    # Closer to how the Go client handles this.
                    # e.g. nats://localhost:4222
                    uri = urlparse(connect_url)
                elif "ws://" in connect_url or "wss://" in connect_url:
                    uri = urlparse(connect_url)
                elif ":" in connect_url:
                    # Expand the scheme for the user
                    # e.g. localhost:4222
                    uri = urlparse(f"nats://{connect_url}")
                else:
                    # Just use the endpoint with the default NATS port.
                    # e.g. demo.nats.io
                    uri = urlparse(f"nats://{connect_url}:4222")

                # In case only endpoint with scheme was set.
                # e.g. nats://demo.nats.io or localhost:
                # the ws and wss do not need a default port as the transport will assume 80 and 443, respectively
                if uri.port is None and uri.scheme not in ("ws", "wss"):
                    uri = urlparse(f"nats://{uri.hostname}:4222")
            except ValueError:
                raise errors.Error("nats: invalid connect url option")

            if uri.hostname is None or uri.hostname == "none":
                raise errors.Error("nats: invalid hostname in connect url")
            self._server_pool.append(Srv(uri))
        elif isinstance(connect_url, list):
            try:
                for server in connect_url:
                    uri = urlparse(server)
                    self._server_pool.append(Srv(uri))
            except ValueError:
                raise errors.Error("nats: invalid connect url option")
            # make sure protocols aren't mixed
            if not (all(server.uri.scheme in ("nats", "tls")
                        for server in self._server_pool)
                    or all(server.uri.scheme in ("ws", "wss")
                           for server in self._server_pool)):
                raise errors.Error(
                    "nats: mixing of websocket and non websocket URLs is not allowed"
                )
        else:
            raise errors.Error("nats: invalid connect url option")

    def _setup_nkeys_connect(self) -> None:
        if self._user_credentials is not None:
            self._setup_nkeys_jwt_connect()
        else:
            self._setup_nkeys_seed_connect()

    def _setup_nkeys_jwt_connect(self) -> None:
        assert self._user_credentials, "_user_credentials required"
        import os

        import nkeys

        creds = self._user_credentials
        if isinstance(creds, tuple):
            assert len(creds) == 2

            def user_cb() -> bytearray:
                contents = None
                with open(creds[0], 'rb') as f:
                    contents = bytearray(os.fstat(f.fileno()).st_size)
                    f.readinto(contents)  # type: ignore[attr-defined]
                return contents

            self._user_jwt_cb = user_cb

            def sig_cb(nonce: str) -> bytes:
                seed = None
                with open(creds[1], 'rb') as f:
                    seed = bytearray(os.fstat(f.fileno()).st_size)
                    f.readinto(seed)  # type: ignore[attr-defined]
                kp = nkeys.from_seed(seed)
                raw_signed = kp.sign(nonce.encode())
                sig = base64.b64encode(raw_signed)

                # Best effort attempt to clear from memory.
                kp.wipe()
                del kp
                del seed
                return sig

            self._signature_cb = sig_cb
        else:
            # Define the functions to be able to sign things using nkeys.
            def user_cb() -> bytearray:
                assert isinstance(creds, str)
                user_jwt = None
                with open(creds, 'rb') as f:
                    while True:
                        line = bytearray(f.readline())
                        if b'BEGIN NATS USER JWT' in line:
                            user_jwt = bytearray(f.readline())
                            break
                # Remove trailing line break but reusing same memory view.
                return user_jwt[:len(user_jwt) - 1]

            self._user_jwt_cb = user_cb

            def sig_cb(nonce: str) -> bytes:
                assert isinstance(creds, str)
                user_seed = None
                with open(creds, 'rb', buffering=0) as f:
                    for line in f:
                        # Detect line where the NKEY would start and end,
                        # then seek and read into a fixed bytearray that
                        # can be wiped.
                        if b'BEGIN USER NKEY SEED' in line:
                            nkey_start_pos = f.tell()
                            try:
                                next(f)
                            except StopIteration:
                                raise ErrInvalidUserCredentials
                            nkey_end_pos = f.tell()
                            nkey_size = nkey_end_pos - nkey_start_pos - 1
                            f.seek(nkey_start_pos)

                            # Only gather enough bytes for the user seed
                            # into the pre allocated bytearray.
                            user_seed = bytearray(nkey_size)
                            f.readinto(user_seed)  # type: ignore[attr-defined]
                kp = nkeys.from_seed(user_seed)
                raw_signed = kp.sign(nonce.encode())
                sig = base64.b64encode(raw_signed)

                # Delete all state related to the keys.
                kp.wipe()
                del user_seed
                del kp
                return sig

            self._signature_cb = sig_cb

    def _setup_nkeys_seed_connect(self) -> None:
        assert self._nkeys_seed, "Client.connect must be called first"
        import os

        import nkeys

        seed = None
        creds = self._nkeys_seed
        with open(creds, 'rb') as f:
            seed = bytearray(os.fstat(f.fileno()).st_size)
            f.readinto(seed)  # type: ignore[attr-defined]
        kp = nkeys.from_seed(seed)
        self._public_nkey = kp.public_key.decode()
        kp.wipe()
        del kp
        del seed

        def sig_cb(nonce: str) -> bytes:
            seed = None
            with open(creds, 'rb') as f:
                seed = bytearray(os.fstat(f.fileno()).st_size)
                f.readinto(seed)  # type: ignore[attr-defined]
            kp = nkeys.from_seed(seed)
            raw_signed = kp.sign(nonce.encode())
            sig = base64.b64encode(raw_signed)

            # Best effort attempt to clear from memory.
            kp.wipe()
            del kp
            del seed
            return sig

        self._signature_cb = sig_cb

    def _connect_command(self) -> bytes:
        '''
        Generates a JSON string with the params to be used
        when sending CONNECT to the server.

          ->> CONNECT {"lang": "python3"}

        '''
        options = {
            "verbose": self.options["verbose"],
            "pedantic": self.options["pedantic"],
            "lang": __lang__,
            "version": __version__,
            "protocol": PROTOCOL
        }
        if "headers" in self._server_info:
            options["headers"] = self._server_info["headers"]
            options["no_responders"] = self._server_info["headers"]

        if "auth_required" in self._server_info:
            if self._server_info["auth_required"]:
                if "nonce" in self._server_info and self._signature_cb is not None:
                    sig = self._signature_cb(self._server_info["nonce"])
                    options["sig"] = sig.decode()

                    if self._user_jwt_cb is not None:
                        jwt = self._user_jwt_cb()
                        options["jwt"] = jwt.decode()
                    elif self._public_nkey is not None:
                        options["nkey"] = self._public_nkey
                # In case there is no password, then consider handle
                # sending a token instead.
                elif self.options["user"] is not None and self.options[
                        "password"] is not None:
                    options["user"] = self.options["user"]
                    options["pass"] = self.options["password"]
                elif self.options["token"] is not None:
                    options["auth_token"] = self.options["token"]
                elif self._current_server and self._current_server.uri.username is not None:
                    if self._current_server.uri.password is None:
                        options["auth_token"
                                ] = self._current_server.uri.username
                    else:
                        options["user"] = self._current_server.uri.username
                        options["pass"] = self._current_server.uri.password

        if self.options["name"] is not None:
            options["name"] = self.options["name"]
        if self.options["no_echo"] is not None:
            options["echo"] = not self.options["no_echo"]

        connect_opts = json.dumps(options, sort_keys=True)
        return b''.join([CONNECT_OP + _SPC_ + connect_opts.encode() + _CRLF_])

    def _process_info(
        self, info: Dict[str, Any], initial_connection: bool = False
    ) -> None:
        """
        Process INFO lines sent by the server to reconfigure client
        with latest updates from cluster to enable server discovery.
        """
        assert self._current_server, "Client.connect must be called first"
        if 'connect_urls' in info:
            if info['connect_urls']:
                connect_urls = []
                for connect_url in info['connect_urls']:
                    scheme = ''
                    if self._current_server.uri.scheme == 'tls':
                        scheme = 'tls'
                    else:
                        scheme = 'nats'

                    uri = urlparse(f"{scheme}://{connect_url}")
                    srv = Srv(uri)
                    srv.discovered = True

                    # Check whether we should reuse the original hostname.
                    if 'tls_required' in self._server_info and self._server_info['tls_required'] \
                            and self._host_is_ip(uri.hostname):
                        srv.tls_name = self._current_server.uri.hostname

                    # Filter for any similar server in the server pool already.
                    should_add = True
                    for s in self._server_pool:
                        if uri.netloc == s.uri.netloc:
                            should_add = False
                    if should_add:
                        connect_urls.append(srv)

                if self.options["dont_randomize"] is not True:
                    shuffle(connect_urls)
                for srv in connect_urls:
                    self._server_pool.append(srv)

                if not initial_connection and connect_urls and self._discovered_server_cb:
                    self._discovered_server_cb()

    def _host_is_ip(self, connect_url: Optional[str]) -> bool:
        try:
            ipaddress.ip_address(connect_url)
            return True
        except Exception:
            return False
