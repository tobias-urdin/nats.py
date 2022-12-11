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

from .msg import Msg


DEFAULT_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_SUB_PENDING_BYTES_LIMIT = 128 * 1024 * 1024


class Subscription:
    """
    A Subscription represents interest in a particular subject.

    A Subscription should not be constructed directly, rather
    `connection.subscribe()` should be used to get a subscription.
    """

    def __init__(
        self,
        conn,
        id: int = 0,
        subject: str = '',
        queue: str = '',
        cb = None,
        future = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> None:
        pass

    @property
    def subject(self) -> str:
        """
        Returns the subject of the `Subscription`.
        """
        pass

    @property
    def queue(self) -> str:
        """
        Returns the queue name of the `Subscription` if part of a queue group.
        """
        pass

    @property
    def messages(self) -> Iterator['Msg']:
        """
        Retrieves an async iterator for the messages from the subscription.

        This is only available if a callback isn't provided when creating a
        subscription.
        """
        pass

    @property
    def pending_msgs(self) -> int:
        """
        Number of delivered messages by the NATS Server that are being buffered
        in the pending queue.
        """
        pass

    @property
    def pending_bytes(self) -> int:
        """
        Size of data sent by the NATS Server that is being buffered
        in the pending queue.
        """
        pass

    @property
    def delivered(self) -> int:
        """
        Number of delivered messages to this subscription so far.
        """
        pass

    def next_msg(self, timeout: Optional[float] = 1.0) -> Msg:
        """
        :params timeout: Time in seconds to wait for next message before timing out.
        :raises nats.errors.TimeoutError:

        next_msg can be used to retrieve the next message
        from a stream of messages, this only works when not
        passing a callback on `subscribe`
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
