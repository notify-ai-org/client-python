"""Kafka transport interface. The SDK chooses topic, partition, key and headers
exactly as the Java SDK does; a transport only writes and reads records."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, List

try:
    from typing import Protocol
except ImportError:  # pragma: no cover
    Protocol = object  # type: ignore

EVENTS_TOPIC = "notify-v1-events"
SCHEDULED_EVENTS_TOPIC = "notify-v1-scheduled-events"


@dataclass
class KafkaRecord:
    topic: str
    partition: int
    key: str
    value: bytes
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class KafkaCredentials:
    """From the acp-server registration response."""

    client_id: str = ""
    api_key: str = ""
    api_secret: str = ""


class KafkaTransport(Protocol):
    def partitions(self, topic: str) -> int: ...

    def produce(self, records: List[KafkaRecord]) -> None: ...

    def consume(self, topic: str, handle: Callable[[bytes], None], stop: threading.Event) -> None:
        """Deliver record values to ``handle`` until ``stop`` is set."""

    def close(self) -> None: ...


KafkaFactory = Callable[[KafkaCredentials], KafkaTransport]
