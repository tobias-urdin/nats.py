# Copyright 2016-2022 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import ssl
import logging
import time
import queue
import json
import socket
from secrets import token_hex
from random import shuffle

from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    List,
    Union,
)

from nats import errors
from nats.common import client as common_client

from nats.protocol import command as prot_command
from nats.protocol.parser import (
    AUTHORIZATION_VIOLATION,
    PERMISSIONS_ERR,
    PONG,
    STALE_CONNECTION,
    SyncParser,
)

from nats.common.subscription import (
    DEFAULT_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_SUB_PENDING_MSGS_LIMIT
)

from nats.sync.subscription import Subscription
from nats.sync.msg import Msg
from .transport import TcpTransport


__version__ = common_client.__version__
__lang__ = common_client.__lang__
_logger = logging.getLogger(__name__)

Callback = Callable[[], None]
ErrorCallback = Callable[[Exception], None]


def _default_error_callback(ex: Exception) -> None:
    """
    Provides a default way to handle errors if the user
    does not provide one.
    """
    _logger.error('nats: encountered error', exc_info=ex)


class Client(common_client.Client):
    """
    Synchronous client for NATS.
    """

    def __init__(self) -> None:
        super(Client, self).__init__()

        self._ps: SyncParser = SyncParser(self)

        # callbacks
        self._error_cb: ErrorCallback = _default_error_callback
        self._disconnected_cb: Optional[Callback] = None
        self._closed_cb: Optional[Callback] = None
        self._discovered_server_cb: Optional[Callback] = None
        self._reconnected_cb: Optional[Callback] = None

        # New style request/response
        self._resp_map: Dict[str, Optional[Msg]] = {}
        self._resp_sub_prefix: Optional[bytearray] = None

    def connect(
        self,
        servers: Union[str, List[str]] = ["nats://localhost:4222"],
        error_cb: Optional[ErrorCallback] = None,
        disconnected_cb: Optional[Callback] = None,
        closed_cb: Optional[Callback] = None,
        discovered_server_cb: Optional[Callback] = None,
        reconnected_cb: Optional[Callback] = None,
        name: Optional[str] = None,
        pedantic: bool = False,
        verbose: bool = False,
        allow_reconnect: bool = True,
        connect_timeout: int = common_client.DEFAULT_CONNECT_TIMEOUT,
        reconnect_time_wait: int = common_client.DEFAULT_RECONNECT_TIME_WAIT,
        max_reconnect_attempts: int = common_client.DEFAULT_MAX_RECONNECT_ATTEMPTS,
        ping_interval: int = common_client.DEFAULT_PING_INTERVAL,
        max_outstanding_pings: int = common_client.DEFAULT_MAX_OUTSTANDING_PINGS,
        dont_randomize: bool = False,
        flusher_queue_size: int = common_client.DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
        no_echo: bool = False,
        tls: Optional[ssl.SSLContext] = None,
        tls_hostname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        drain_timeout: int = common_client.DEFAULT_DRAIN_TIMEOUT,
        signature_cb: Optional[common_client.SignatureCallback] = None,
        user_jwt_cb: Optional[common_client.JWTCallback] = None,
        user_credentials: Optional[common_client.Credentials] = None,
        nkeys_seed: Optional[str] = None,
        inbox_prefix: Union[str, bytes] = common_client.DEFAULT_INBOX_PREFIX,
        pending_size: int = common_client.DEFAULT_PENDING_SIZE,
        flush_timeout: Optional[float] = None,
    ) -> None:
        for cb in [error_cb, disconnected_cb, closed_cb, reconnected_cb,
                   discovered_server_cb]:
            if cb and not callable(cb):
                raise errors.InvalidCallbackTypeError

        super(Client, self).connect(
            servers,
            error_cb,
            disconnected_cb,
            closed_cb,
            discovered_server_cb,
            reconnected_cb,
            name,
            pedantic,
            verbose,
            allow_reconnect,
            connect_timeout,
            reconnect_time_wait,
            max_reconnect_attempts,
            ping_interval,
            max_outstanding_pings,
            dont_randomize,
            flusher_queue_size,
            no_echo,
            tls,
            tls_hostname,
            user,
            password,
            token,
            drain_timeout,
            signature_cb,
            user_jwt_cb,
            user_credentials,
            nkeys_seed,
            inbox_prefix,
            pending_size,
            flush_timeout,
        )

        self._error_cb = error_cb or _default_error_callback
        self._closed_cb = closed_cb
        self._discovered_server_cb = discovered_server_cb
        self._reconnected_cb = reconnected_cb
        self._disconnected_cb = disconnected_cb

        # client id that the NATS server knows about.
        self._sid: int = 0
        self._subs: Dict[int, Subscription] = {}

        # Queue used to trigger flushes to the socket.
        self._flush_queue = queue.Queue(maxsize=flusher_queue_size)

        while True:
            try:
                self._select_next_server()
                self._process_connect_init()
                assert self._current_server, "the current server must be set by _select_next_server"
                self._current_server.reconnects = 0
                break
            except errors.NoServersError as e:
                if self.options["max_reconnect_attempts"] < 0:
                    # Never stop reconnecting
                    continue
                self._err = e
                raise e
            except (OSError, errors.Error) as e:
                self._err = e
                self._error_cb(e)

                # Bail on first attempt if reconnecting is disallowed.
                if not self.options["allow_reconnect"]:
                    raise e

                #self._close(Client.DISCONNECTED, False)
                self.close()
                if self._current_server is not None:
                    self._current_server.last_attempt = time.monotonic()
                    self._current_server.reconnects += 1

    def close(self) -> None:
        pass

    def drain(self) -> None:
        pass

    def publish(
        self,
        subject: str,
        payload: bytes = b'',
        reply: str = '',
        headers: Optional[Dict[str, str]] = None
    ) -> None:
        if self.is_closed:
            raise errors.ConnectionClosedError
        if self.is_draining_pubs:
            raise errors.ConnectionDrainingError

        payload_size = len(payload)
        if not self.is_connected:
            if self._max_pending_size <= 0 or payload_size + self._pending_data_size > self._max_pending_size:
                # Cannot publish during a reconnection when the buffering is disabled,
                # or if pending buffer is already full.
                raise errors.OutboundBufferLimitError

        if payload_size > self._max_payload:
            raise errors.MaxPayloadError
        self._send_publish(
            subject, reply, payload, payload_size, headers
        )

    def _send_publish(
        self,
        subject: str,
        reply: str,
        payload: bytes,
        payload_size: int,
        headers: Optional[Dict[str, Any]],
    ) -> None:
        """
        Sends PUB command to the NATS server.
        """
        if subject == "":
            # Avoid sending messages with empty replies.
            raise errors.BadSubjectError

        pub_cmd = None
        if headers is None:
            pub_cmd = prot_command.pub_cmd(subject, reply, payload)
        else:
            hdr = bytearray()
            hdr.extend(common_client.NATS_HDR_LINE)
            hdr.extend(common_client._CRLF_)
            for k, v in headers.items():
                key = k.strip()
                if not key:
                    # Skip empty keys
                    continue
                hdr.extend(key.encode())
                hdr.extend(b': ')
                value = v.strip()
                hdr.extend(value.encode())
                hdr.extend(common_client._CRLF_)
            hdr.extend(common_client._CRLF_)
            pub_cmd = prot_command.hpub_cmd(subject, reply, hdr, payload)

        self.stats['out_msgs'] += 1
        self.stats['out_bytes'] += payload_size
        #print('_send_publish: %s' % str(pub_cmd))
        self._transport.write(pub_cmd)

    def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb : Optional[Callable[[Msg], None]] = None,
        future = None, # Not used only for compat
        max_msgs: int = 0,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> Subscription:
        if not subject or (' ' in subject):
            raise errors.BadSubjectError

        if queue and (' ' in queue):
            raise errors.BadSubjectError

        if self.is_closed:
            raise errors.ConnectionClosedError

        if self.is_draining:
            raise errors.ConnectionDrainingError

        self._sid += 1
        sid = self._sid

        sub = Subscription(
            self,
            sid,
            subject,
            queue=queue,
            cb=cb,
            max_msgs=max_msgs,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

        self._subs[sid] = sub
        self._send_subscribe(sub)
        return sub

    def _send_subscribe(self, sub: Subscription) -> None:
        sub_cmd = None
        if sub._queue is None:
            sub_cmd = prot_command.sub_cmd(sub._subject, EMPTY, sub._id)
        else:
            sub_cmd = prot_command.sub_cmd(sub._subject, sub._queue, sub._id)
        self._transport.write(sub_cmd)

    def request(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: float = 0.5,
        old_style: bool = False,
        headers: Dict[str, Any] = None,
    ) -> Msg:
        """
        Implements the request/response pattern via pub/sub
        using a single wildcard subscription that handles
        the responses.

        """
        if old_style:
            # FIXME: Support headers in old style requests.
            return self._request_old_style(
                subject, payload, timeout=timeout
            )
        else:
            msg = self._request_new_style(
                subject, payload, timeout=timeout, headers=headers
            )
        if msg.headers and msg.headers.get(nats.js.api.Header.STATUS
                                           ) == NO_RESPONDERS_STATUS:
            raise errors.NoRespondersError
        return msg

    def _init_request_sub(self) -> None:
        self._resp_map = {}

        self._resp_sub_prefix = self._inbox_prefix[:]
        self._resp_sub_prefix.extend(b'.')
        self._resp_sub_prefix.extend(self._nuid.next())
        self._resp_sub_prefix.extend(b'.')
        resp_mux_subject = self._resp_sub_prefix[:]
        resp_mux_subject.extend(b'*')
        self.subscribe(
            resp_mux_subject.decode(), cb=self._request_sub_callback
        )

    def _request_sub_callback(self, msg: Msg) -> None:
        token = msg.subject[len(self._inbox_prefix) + 22 + 2:]
        fut = self._resp_map.get(token, None)
        if fut is not None:
            return
        self._resp_map[token] = msg

    def _request_new_style(
        self,
        subject: str,
        payload: bytes,
        timeout: float = 1,
        headers: Dict[str, Any] = None,
    ) -> Msg:
        if self.is_draining_pubs:
            raise errors.ConnectionDrainingError

        if not self._resp_sub_prefix:
            self._init_request_sub()
        assert self._resp_sub_prefix

        token = self._nuid.next()
        token.extend(token_hex(2).encode())
        inbox = self._resp_sub_prefix[:]
        inbox.extend(token)
        self._resp_map[token.decode()] = None
        def cb(msg):
            self._resp_map[token.decode()] = msg
        sub = self.subscribe(inbox.decode(), cb=cb)
        self.publish(
            subject, payload, reply=inbox.decode(), headers=headers
        )
        while self._resp_map[token.decode()] is None:
            self.wait()
        return self._resp_map.pop(token.decode())

    def _request_old_style(
        self, subject: str, payload: bytes, timeout: float = 1
    ) -> Msg:
        """
        Implements the request/response pattern via pub/sub
        using an ephemeral subscription which will be published
        with a limited interest of 1 reply returning the response
        or raising a Timeout error.
        """
        inbox = self.new_inbox()

        sub = self.subscribe(inbox, max_msgs=1)
        sub.unsubscribe(limit=1)
        resp = None
        def cb(msg):
            resp = msg
        self.publish(subject, payload, reply=inbox, cb=cb)
        while resp is None:
            self.wait()
        return resp

    def flush(self, timeout: int = common_client.DEFAULT_FLUSH_TIMEOUT) -> None:
        pass

    def __aenter__(self) -> "Client":
        """For when NATS client is used in a context manager"""
        return self

    def __aexit__(self, *exc_info) -> None:
        """Close connection to NATS when used in a context manager"""
        pass

    # Helper code below this point that does not define the baseclass
    # interface for a client.

    def _select_next_server(self) -> None:
        """
        Looks up in the server pool for an available server
        and attempts to connect.
        """

        while True:
            if len(self._server_pool) == 0:
                self._current_server = None
                raise errors.NoServersError

            now = time.monotonic()
            s = self._server_pool.pop(0)
            if self.options["max_reconnect_attempts"] > 0:
                if s.reconnects > self.options["max_reconnect_attempts"]:
                    # Discard server since already tried to reconnect too many times
                    continue

            # Not yet exceeded max_reconnect_attempts so can still use
            # this server in the future.
            self._server_pool.append(s)
            if s.last_attempt is not None and now < s.last_attempt + self.options[
                    "reconnect_time_wait"]:
                # Backoff connecting to server if we attempted recently.
                time.sleep(self.options["reconnect_time_wait"])
            try:
                s.last_attempt = time.monotonic()
                if not self._transport:
                    self._transport = TcpTransport()
                self._transport.connect(
                    s.uri,
                    buffer_size=common_client.DEFAULT_BUFFER_SIZE,
                    connect_timeout=self.options['connect_timeout']
                )
                self._current_server = s
                break
            except Exception as e:
                s.last_attempt = time.monotonic()
                s.reconnects += 1

                self._err = e
                self._error_cb(e)
                continue

    def _process_disconnect(self) -> None:
        """
        Process disconnection from the server and set client status
        to DISCONNECTED.
        """
        self._status = Client.DISCONNECTED

    def _process_connect_init(self) -> None:
        """
        Process INFO received from the server and CONNECT to the server
        with authentication.  It is also responsible of setting up the
        reading and ping interval tasks from the client.
        """
        assert self._transport, "must be called only from Client.connect"
        assert self._current_server, "must be called only from Client.connect"
        self._status = Client.CONNECTING

        info_line = self._transport.readline()
        if common_client.INFO_OP not in info_line:
            raise errors.Error(
                "nats: empty response from server when expecting INFO message"
            )

        _, info = info_line.split(common_client.INFO_OP + common_client._SPC_, 1)

        try:
            srv_info = json.loads(info.decode())
        except Exception:
            raise errors.Error("nats: info message, json parse error")

        self._server_info = srv_info
        self._process_info(srv_info, initial_connection=True)

        if 'version' in self._server_info:
            self._current_server.server_version = self._server_info['version']

        if 'max_payload' in self._server_info:
            self._max_payload = self._server_info["max_payload"]

        if 'client_id' in self._server_info:
            self._client_id = self._server_info["client_id"]

        if 'tls_required' in self._server_info and self._server_info[
                'tls_required']:
            # Check whether to reuse the original hostname for an implicit route.
            hostname = None
            if "tls_hostname" in self.options:
                hostname = self.options["tls_hostname"]
            elif self._current_server.tls_name is not None:
                hostname = self._current_server.tls_name
            else:
                hostname = self._current_server.uri.hostname

            self._transport.drain()  # just in case something is left

            # connect to transport via tls
            self._transport.connect_tls(
                hostname,
                self.ssl_context,
                common_client.DEFAULT_BUFFER_SIZE,
                self.options['connect_timeout'],
            )

        # Refresh state of parser upon reconnect.
        if self.is_reconnecting:
            self._ps.reset()

        assert self._transport
        connect_cmd = self._connect_command()
        self._transport.write(connect_cmd)
        self._transport.drain()
        if self.options["verbose"]:
            next_op = self._transport.readline()
            if common_client.OK_OP in next_op:
                # Do nothing
                pass
            elif common_client.ERR_OP in next_op:
                err_line = next_op.decode()
                _, err_msg = err_line.split(" ", 1)

                # FIXME: Maybe handling could be more special here,
                # checking for errors.AuthorizationError for example.
                # self._process_err(err_msg)
                raise errors.Error("nats: " + err_msg.rstrip('\r\n'))

        self._transport.write(common_client.PING_PROTO)
        self._transport.drain()

        next_op = self._transport.readline()

        if common_client.PONG_PROTO in next_op:
            self._status = Client.CONNECTED
        elif common_client.ERR_OP in next_op:
            err_line = next_op.decode()
            _, err_msg = err_line.split(" ", 1)

            # FIXME: Maybe handling could be more special here,
            # checking for ErrAuthorization for example.
            # self._process_err(err_msg)
            raise errors.Error("nats: " + err_msg.rstrip('\r\n'))

        if common_client.PONG_PROTO in next_op:
            self._status = Client.CONNECTED

        #self._reading_task = asyncio.get_running_loop().create_task(
        #    self._read_loop()
        #)
        self._pongs = []
        self._pings_outstanding = 0
        #self._ping_interval_task = asyncio.get_running_loop().create_task(
        #    self._ping_interval()
        #)

        # Task for kicking the flusher queue
        #self._flusher_task = asyncio.get_running_loop().create_task(
        #    self._flusher()
        #)

    def _attempt_reconnect(self) -> None:
        assert self._current_server, "Client.connect must be called first"

        if self._transport is not None:
            self._transport.close()
            try:
                self._transport.wait_closed()
            except Exception as e:
                self._error_cb(e)

        self._err = None
        if self._disconnected_cb is not None:
            self._disconnected_cb()

        if self.is_closed:
            return

        if "dont_randomize" not in self.options or not self.options[
                "dont_randomize"]:
            shuffle(self._server_pool)

        while True:
            try:
                # Try to establish a TCP connection to a server in
                # the cluster then send CONNECT command to it.
                self._select_next_server()
                assert self._transport, "_select_next_server must've set _transport"
                self._process_connect_init()

                # Consider a reconnect to be done once CONNECT was
                # processed by the server successfully.
                self.stats["reconnects"] += 1

                # Reset reconnect attempts for this server
                # since have successfully connected.
                self._current_server.did_connect = True
                self._current_server.reconnects = 0

                # Replay all the subscriptions in case there were some.
                subs_to_remove = []
                for sid, sub in self._subs.items():
                    max_msgs = 0
                    if sub._max_msgs > 0:
                        # If we already hit the message limit, remove the subscription and don't
                        # resubscribe.
                        if sub._received >= sub._max_msgs:
                            subs_to_remove.append(sid)
                            continue
                        # auto unsubscribe the number of messages we have left
                        max_msgs = sub._max_msgs - sub._received

                    sub_cmd = prot_command.sub_cmd(
                        sub._subject, sub._queue, sid
                    )
                    self._transport.write(sub_cmd)

                    if max_msgs > 0:
                        unsub_cmd = prot_command.unsub_cmd(sid, max_msgs)
                        self._transport.write(unsub_cmd)

                for sid in subs_to_remove:
                    self._subs.pop(sid)

                self._transport.drain()

                self._status = Client.CONNECTED
                self.flush()
                if self._reconnected_cb is not None:
                    self._reconnected_cb()
                break
            except errors.NoServersError as e:
                self._err = e
                self.close()
                break
            except (OSError, errors.Error, socket.timeout) as e:
                self._err = e
                self._error_cb(e)
                self._status = Client.RECONNECTING
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1

    def _process_op_err(self, e: Exception) -> None:
        """
        Process errors which occurred while reading or parsing
        the protocol. If allow_reconnect is enabled it will
        try to switch the server to which it is currently connected
        otherwise it will disconnect.
        """
        #print('_process_op_err: %s' % str(e))
        if self.is_connecting or self.is_closed or self.is_reconnecting:
            return

        if self.options["allow_reconnect"] and self.is_connected:
            self._status = Client.RECONNECTING
            self._ps.reset()

            self._attempt_reconnect()
        else:
            self._process_disconnect()
            self._err = e
            self._close(Client.CLOSED, True)

    def _process_err(self, err_msg: str) -> None:
        """
        Processes the raw error message sent by the server
        and close connection with current server.
        """
        assert self._error_cb, "Client.connect must be called first"
        if STALE_CONNECTION in err_msg:
            self._process_op_err(errors.StaleConnectionError())
            return

        if AUTHORIZATION_VIOLATION in err_msg:
            self._err = errors.AuthorizationError()
        else:
            prot_err = err_msg.strip("'")
            m = f"nats: {prot_err}"
            err = errors.Error(m)
            self._err = err

            if PERMISSIONS_ERR in m:
                self._error_cb(err)
                return

        do_cbs = False
        if not self.is_connecting:
            do_cbs = True

        # FIXME: Some errors such as 'Invalid Subscription'
        # do not cause the server to close the connection.
        # For now we handle similar as other clients and close.
        self._close(Client.CLOSED, do_cbs)

    def _process_ping(self) -> None:
        """
        Process PING sent by server.
        """
        self._transport.write(PONG)

    def _process_pong(self) -> None:
        """
        Process PONG sent by server.
        """
        pass
        #if len(self._pongs) > 0:
        #    future = self._pongs.pop(0)
        #    future.set_result(True)
        #    self._pongs_received += 1
        #    self._pings_outstanding = 0

    def _is_control_message(self, data, header: Dict[str,
                                                     str]) -> Optional[str]:
        if len(data) > 0:
            return None
        status = header.get(nats.js.api.Header.STATUS)
        if status == CTRL_STATUS:
            return header.get(nats.js.api.Header.DESCRIPTION)
        return None

    def _build_message(
        self,
        subject: bytes,
        reply: bytes,
        data: bytes,
        headers: Optional[Dict[str, str]],
    ):
        return Msg(
            subject=subject.decode(),
            reply=reply.decode(),
            data=data,
            headers=headers,
            _client=self,
        )

    def _process_headers(self, headers) -> Optional[Dict[str, str]]:
        if not headers:
            return None

        hdr = None
        raw_headers = headers[NATS_HDR_LINE_SIZE:]

        # If the first character is an empty space, then this is
        # an inline status message sent by the server.
        #
        # NATS/1.0 404\r\n\r\n
        # NATS/1.0 503\r\n\r\n
        # NATS/1.0 404 No Messages\r\n\r\n
        #
        # Note: it is possible to receive a message with both inline status
        # and a set of headers.
        #
        # NATS/1.0 100\r\nIdle Heartbeat\r\nNats-Last-Consumer: 1016\r\nNats-Last-Stream: 1024\r\n\r\n
        #
        if raw_headers[0] == _SPC_BYTE_:
            # Special handling for status messages.
            line = headers[len(NATS_HDR_LINE) + 1:]
            status = line[:STATUS_MSG_LEN]
            desc = line[STATUS_MSG_LEN + 1:len(line) - _CRLF_LEN_ - _CRLF_LEN_]
            stripped_status = status.strip().decode()

            # Process as status only when it is a valid integer.
            hdr = {}
            if stripped_status.isdigit():
                hdr[nats.js.api.Header.STATUS] = stripped_status

            # Move the raw_headers to end of line
            i = raw_headers.find(_CRLF_)
            raw_headers = raw_headers[i + _CRLF_LEN_:]

            if len(desc) > 0:
                # Heartbeat messages can have both headers and inline status,
                # check that there are no pending headers to be parsed.
                i = desc.find(_CRLF_)
                if i > 0:
                    hdr[nats.js.api.Header.DESCRIPTION] = desc[:i].decode()
                    parsed_hdr = self._hdr_parser.parsebytes(
                        desc[i + _CRLF_LEN_:]
                    )
                    for k, v in parsed_hdr.items():
                        hdr[k] = v
                else:
                    # Just inline status...
                    hdr[nats.js.api.Header.DESCRIPTION] = desc.decode()

        if not len(raw_headers) > _CRLF_LEN_:
            return hdr

        #
        # Example header without status:
        #
        # NATS/1.0\r\nfoo: bar\r\nhello: world
        #
        raw_headers = headers[NATS_HDR_LINE_SIZE + _CRLF_LEN_:]
        try:
            parsed_hdr = self._hdr_parser.parsebytes(raw_headers)
            if len(parsed_hdr.items()) == 0:
                return hdr
            else:
                if not hdr:
                    hdr = {}
                for k, v in parsed_hdr.items():
                    hdr[k.strip()] = v.strip()
        except Exception as e:
            self._error_cb(e)
            return hdr

        return hdr

    def _process_msg(
        self,
        sid: int,
        subject: bytes,
        reply: bytes,
        data: bytes,
        headers: bytes,
    ) -> None:
        """
        Process MSG sent by server.
        """
        assert self._error_cb, "Client.connect must be called first"
        #print('_process_msg: sid=%d subject=%s data=%s' % (sid, str(subject), str(data)))
        payload_size = len(data)
        self.stats['in_msgs'] += 1
        self.stats['in_bytes'] += payload_size

        sub = self._subs.get(sid)
        if not sub:
            # Skip in case no subscription present.
            return

        #print('sub: %d' % sid)

        sub._received += 1
        if sub._max_msgs > 0 and sub._received >= sub._max_msgs:
            # Enough messages so can throwaway subscription now, the
            # pending messages will still be in the subscription
            # internal queue and the task will finish once the last
            # message is processed.
            self._subs.pop(sid, None)

        hdr = self._process_headers(headers)
        msg = self._build_message(subject, reply, data, hdr)
        if not msg:
            return

        # Process flow control messages in case of using a JetStream context.
        ctrl_msg = None
        fcReply = None
        if sub._jsi:
            #########################################
            #                                       #
            # JetStream Control Messages Processing #
            #                                       #
            #########################################
            jsi = sub._jsi
            if hdr:
                ctrl_msg = self._is_control_message(data, hdr)

                # Check if the heartbeat has a "Consumer Stalled" header, if
                # so, the value is the FC reply to send a nil message to.
                # We will send it at the end of this function.
                if ctrl_msg and ctrl_msg.startswith("Idle"):
                    fcReply = hdr.get(nats.js.api.Header.CONSUMER_STALLED)

            # OrderedConsumer: checkOrderedMsgs
            if not ctrl_msg and jsi._ordered and msg.reply:
                did_reset = None
                tokens = Msg.Metadata._get_metadata_fields(msg.reply)
                # FIXME: Support JS Domains.
                sseq = int(tokens[5])
                dseq = int(tokens[6])
                if dseq != jsi._dseq:
                    # Pick up from where we last left.
                    did_reset = jsi.reset_ordered_consumer(jsi._sseq + 1)
                else:
                    # Update our tracking
                    jsi._dseq = dseq + 1
                    jsi._sseq = sseq
                if did_reset:
                    return

        # Skip processing if this is a control message.
        if not ctrl_msg:
            # Check if it is an old style request.
            #if sub._future:
            #    if sub._future.cancelled():
                    # Already gave up, nothing to do.
            #        return
            #    sub._future.set_result(msg)
            #    return

            # Let subscription wait_for_msgs coroutine process the messages,
            # but in case sending to the subscription task would block,
            # then consider it to be an slow consumer and drop the message.
            sub._pending_size += payload_size
            # allow setting pending_bytes_limit to 0 to disable
            if sub._pending_bytes_limit > 0 and sub._pending_size >= sub._pending_bytes_limit:
                # Subtract the bytes since the message will be thrown away
                # so it would not be pending data.
                sub._pending_size -= payload_size

                self._error_cb(
                    errors.SlowConsumerError(
                        subject=msg.subject,
                        reply=msg.reply,
                        sid=sid,
                        sub=sub
                    )
                )
                return

            sub._cb(msg)

            # Store the ACK metadata from the message to
            # compare later on with the received heartbeat.
            if sub._jsi:
                sub._jsi.track_sequences(msg.reply)
        elif ctrl_msg.startswith("Flow") and msg.reply and sub._jsi:
            # This is a flow control message.
            # We will schedule the send of the FC reply once we have delivered the
            # DATA message that was received before this flow control message, which
            # has sequence `jsi.fciseq`. However, it is possible that this message
            # has already been delivered, in that case, we need to send the FC reply now.
            if sub.delivered >= sub._jsi._fciseq:
                fcReply = msg.reply
            else:
                # Schedule a reply after the previous message is delivered.
                sub._jsi.schedule_flow_control_response(msg.reply)

        # Handle flow control response.
        if fcReply:
            self.publish(fcReply)

        if ctrl_msg and not msg.reply and ctrl_msg.startswith("Idle"):
            if sub._jsi:
                sub._jsi.check_for_sequence_mismatch(msg)

    def wait(self) -> None:
        try:
            should_bail = self.is_closed or self.is_reconnecting
            if should_bail or self._transport is None:
                return
            if self.is_connected and self._transport.at_eof():
                err = errors.UnexpectedEOF()
                self._error_cb(err)
                self._process_op_err(err)
                return

            try:
                b = self._transport.read(common_client.DEFAULT_BUFFER_SIZE)
            except socket.timeout:
                # FIXME
                # If we timeout just continue to poll for now
                return
            self._ps.parse(b)
        except errors.ProtocolError:
            self._process_op_err(errors.ProtocolError())
        except OSError as e:
            self._process_op_err(e)
        except Exception as ex:
            _logger.error('nats: encountered error', exc_info=ex)
