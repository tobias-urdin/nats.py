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

from typing import (
    Callable,
    Iterator,
    List,
    Optional,
)

from nats.common import subscription as common_sub
from .msg import Msg

import queue as queue_mod


class Subscription(common_sub.Subscription):
    """
    A Subscription represents interest in a particular subject.

    A Subscription should not be constructed directly, rather
    `connection.subscribe()` should be used to get a subscription.

    ::

        nc = nats.sync_connect()

        def cb(msg):
          print('Received', msg)
        nc.subscribe('foo', cb=cb)

        # Sync Subscription
        sub = nc.subscribe('foo')
        msg = sub.next_msg()
        print('Received', msg)

    """

    def __init__(
        self,
        conn,
        id: int = 0,
        subject: str = '',
        queue: str = '',
        cb: Optional[Callable[['Msg'], None]] = None,
        future = None, # Not used in sync, here for compat
        max_msgs: int = 0,
        pending_msgs_limit: int = common_sub.DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = common_sub.DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> None:
        super(Subscription, self).__init__(conn, id, subject, queue, cb,
                                           future, max_msgs, pending_msgs_limit,
                                           pending_bytes_limit)
        self._conn = conn
        self._id = id
        self._subject = subject
        self._queue = queue
        self._max_msgs = max_msgs
        self._received = 0
        self._cb = cb
        self._closed = False

        # Per subscription message processor.
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_queue: "queue_mod.Queue[Msg]" = queue_mod.Queue(
            maxsize=pending_msgs_limit
        )
        self._pending_size = 0
        self._message_iterator = None

        # For JetStream enabled subscriptions.
        self._jsi: Optional["JetStreamContext._JSI"] = None

    @property
    def subject(self) -> str:
        """
        Returns the subject of the `Subscription`.
        """
        return self._subject

    @property
    def queue(self) -> str:
        """
        Returns the queue name of the `Subscription` if part of a queue group.
        """
        return self._queue

    @property
    def messages(self) -> Iterator['Msg']:
        """
        Retrieves an iterator for the messages from the subscription.

        This is only available if a callback isn't provided when creating a
        subscription.
        """
        if not self._message_iterator:
            raise errors.Error(
                "cannot iterate over messages with a non iteration subscription type"
            )

        return self._message_iterator

    @property
    def pending_msgs(self) -> int:
        """
        Number of delivered messages by the NATS Server that are being buffered
        in the pending queue.
        """
        return self._pending_queue.qsize()

    @property
    def pending_bytes(self) -> int:
        """
        Size of data sent by the NATS Server that is being buffered
        in the pending queue.
        """
        return self._pending_size

    @property
    def delivered(self) -> int:
        """
        Number of delivered messages to this subscription so far.
        """
        return self._received

    def next_msg(self, timeout: Optional[float] = 1.0) -> Msg:
        """
        :params timeout: Time in seconds to wait for next message before timing out.
        :raises nats.errors.TimeoutError:

        next_msg can be used to retrieve the next message
        from a stream of messages using syntax, this
        only works when not passing a callback on `subscribe`::

            sub = nc.subscribe('hello')
            msg = sub.next_msg(timeout=1)

        """
        pass

    def drain(self):
        """
        Removes interest in a subject, but will process remaining messages.
        """
        pass

    def unsubscribe(self, limit: int = 0):
        """
        :param limit: Max number of messages to receive before unsubscribing.

        Removes interest in a subject, remaining messages will be discarded.

        If `limit` is greater than zero, interest is not immediately removed,
        rather, interest will be automatically removed after `limit` messages
        are received.
        """
        pass


class _SubscriptionMessageIterator:

    def __init__(self, sub: Subscription) -> None:
        self._sub: Subscription = sub
        self._queue: "queue.Queue[Msg]" = sub._pending_queue
        self._unsubscribed_future = None

    def _cancel(self) -> None:
        if not self._unsubscribed_future.done():
            self._unsubscribed_future.set_result(True)

    def __aiter__(self) -> "_SubscriptionMessageIterator":
        return self

    def __anext__(self) -> Msg:
        pass
