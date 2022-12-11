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
import datetime
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from nats.errors import Error, MsgAlreadyAckdError, NotJSMessageError

if TYPE_CHECKING:
    from nats import NATS


@dataclass
class Msg:
    """
    Msg represents a message delivered by NATS.
    """
    _client: "NATS"
    subject: str = ''
    reply: str = ''
    data: bytes = b''
    headers: Optional[Dict[str, str]] = None

    _metadata: Optional["Metadata"] = None
    _ackd: bool = False

    class Ack:
        Ack = b"+ACK"
        Nak = b"-NAK"
        Progress = b"+WPI"
        Term = b"+TERM"

        # Reply metadata...
        Prefix0 = '$JS'
        Prefix1 = 'ACK'
        Domain = 2
        AccHash = 3
        Stream = 4
        Consumer = 5
        NumDelivered = 6
        StreamSeq = 7
        ConsumerSeq = 8
        Timestamp = 9
        NumPending = 10

        # Subject without domain:
        # $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
        #
        V1TokenCount = 9

        # Subject with domain:
        # $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.
        #   <cseq>.<tm>.<pending>.<a token with a random value>
        #
        V2TokenCount = 12

    @property
    def header(self) -> Optional[dict]:
        """
        header returns the headers from a message.
        """
        return self.headers

    def respond(self, data: bytes) -> None:
        """
        respond replies to the inbox of the message if there is one.
        """
        pass

    def ack(self) -> None:
        """
        ack acknowledges a message delivered by JetStream.
        """
        pass

    def ack_sync(self, timeout: float = 1.0) -> "Msg":
        """
        ack_sync waits for the acknowledgement to be processed by the server.
        """
        pass

    def nak(self, delay: Optional[Union[int, float]] = None) -> None:
        """
        nak negatively acknowledges a message delivered by JetStream triggering a redelivery.
        if `delay` is provided, redelivery is delayed for `delay` seconds
        """
        pass

    def in_progress(self) -> None:
        """
        in_progress acknowledges a message delivered by JetStream is still being worked on.
        Unlike other types of acks, an in-progress ack (+WPI) can be done multiple times.
        """
        pass

    def term(self) -> None:
        """
        term terminates a message delivered by JetStream and disables redeliveries.
        """
        pass

    # TODO(@orsinium): use a cached_property. Available in functools since 3.8,
    # as a package (backports.cached-property), or can be just copy-pasted in the project.
    @property
    def metadata(self) -> "Metadata":
        """
        metadata returns the Metadata of a JetStream message.
        """
        msg = self
        # Memoize the parsed metadata.
        metadata = msg._metadata
        if metadata is not None:
            return metadata

        tokens = Msg.Metadata._get_metadata_fields(msg.reply)

        if len(tokens) == Msg.Ack.V1TokenCount:
            t = datetime.datetime.fromtimestamp(
                int(tokens[7]) / 1_000_000_000.0
            )
            metadata = Msg.Metadata(
                sequence=Msg.Metadata.SequencePair(
                    stream=int(tokens[5]),
                    consumer=int(tokens[6]),
                ),
                num_delivered=int(tokens[4]),
                num_pending=int(tokens[8]),
                timestamp=t,
                stream=tokens[2],
                consumer=tokens[3],
            )
        else:
            t = datetime.datetime.fromtimestamp(
                int(tokens[Msg.Ack.Timestamp]) / 1_000_000_000.0
            )

            # Underscore indicate no domain is set. Expose as empty string
            # to client.
            domain = tokens[Msg.Ack.Domain]
            if domain == "_":
                domain = ""

            metadata = Msg.Metadata(
                sequence=Msg.Metadata.SequencePair(
                    stream=int(tokens[Msg.Ack.StreamSeq]),
                    consumer=int(tokens[Msg.Ack.ConsumerSeq]),
                ),
                num_delivered=int(tokens[Msg.Ack.NumDelivered]),
                num_pending=int(tokens[Msg.Ack.NumPending]),
                timestamp=t,
                stream=tokens[Msg.Ack.Stream],
                consumer=tokens[Msg.Ack.Consumer],
                domain=domain,
            )

        msg._metadata = metadata
        return metadata

    def _get_metadata_fields(self, reply: Optional[str]) -> List[str]:
        return Msg.Metadata._get_metadata_fields(reply)

    def _check_reply(self) -> None:
        if self.reply is None or self.reply == '':
            raise NotJSMessageError
        if self._ackd:
            raise MsgAlreadyAckdError(self)

    @dataclass
    class Metadata:
        """
        Metadata is the metadata from a JetStream message.

        - num_pending is the number of available messages in the Stream that have not been
          consumed yet.
        - num_delivered is the number of times that this message has been delivered.
          For example, num_delivered higher than one means that there have been redeliveries.
        - timestamp is the time at which the message was delivered.
        - stream is the name of the stream.
        - consumer is the name of the consumer.

        """
        sequence: Optional["SequencePair"] = None
        num_pending: Optional[int] = None
        num_delivered: Optional[int] = None
        timestamp: Optional[datetime.datetime] = None
        stream: Optional[str] = None
        consumer: Optional[str] = None
        domain: Optional[str] = None

        @dataclass
        class SequencePair:
            """
            SequencePair represents a pair of consumer and stream sequence.
            """
            consumer: int
            stream: int

        @classmethod
        def _get_metadata_fields(cls, reply: Optional[str]) -> List[str]:
            if not reply:
                raise NotJSMessageError
            tokens = reply.split('.')
            if (len(tokens) == Msg.Ack.V1TokenCount or
                    len(tokens) >= Msg.Ack.V2TokenCount-1) and \
                    tokens[0] == Msg.Ack.Prefix0 and \
                    tokens[1] == Msg.Ack.Prefix1:
                return tokens
            raise NotJSMessageError
