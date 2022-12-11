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
from dataclasses import dataclass
from typing import Optional, Union

from nats.common import msg as common_msg


@dataclass
class Msg(common_msg.Msg):
    def respond(self, data: bytes) -> None:
        """
        respond replies to the inbox of the message if there is one.
        """
        if not self.reply:
            raise Error('no reply subject available')
        if not self._client:
            raise Error('client not set')

        self._client.publish(self.reply, data, headers=self.headers)

    def ack(self) -> None:
        """
        ack acknowledges a message delivered by JetStream.
        """
        self._check_reply()
        self._client.publish(self.reply)
        self._ackd = True

    def ack_sync(self, timeout: float = 1.0) -> "Msg":
        """
        ack_sync waits for the acknowledgement to be processed by the server.
        """
        self._check_reply()
        resp = self._client.request(self.reply, timeout=timeout)
        self._ackd = True
        return resp

    def nak(self, delay: Optional[Union[int, float]] = None) -> None:
        """
        nak negatively acknowledges a message delivered by JetStream triggering a redelivery.
        if `delay` is provided, redelivery is delayed for `delay` seconds
        """
        self._check_reply()
        payload = Msg.Ack.Nak
        json_args = dict()
        if delay:
            json_args['delay'] = int(delay * 10**9)  # to seconds to ns
        if json_args:
            payload += (b' ' + json.dumps(json_args).encode())
        self._client.publish(self.reply, payload)
        self._ackd = True

    def in_progress(self) -> None:
        """
        in_progress acknowledges a message delivered by JetStream is still being worked on.
        Unlike other types of acks, an in-progress ack (+WPI) can be done multiple times.
        """
        if self.reply is None or self.reply == '':
            raise NotJSMessageError
        self._client.publish(self.reply, Msg.Ack.Progress)

    def term(self) -> None:
        """
        term terminates a message delivered by JetStream and disables redeliveries.
        """
        self._check_reply()

        self._client.publish(self.reply, Msg.Ack.Term)
        self._ackd = True
