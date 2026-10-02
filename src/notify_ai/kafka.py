"""Kafka transport on confluent-kafka (``pip install notify-ai-client[kafka]``).

    from notify_ai.kafka import KafkaConfig, kafka_factory
    client = NotifyClient(Config(..., kafka_enabled=True),
                          kafka=kafka_factory(KafkaConfig(brokers="broker:9092")))
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .transport import KafkaCredentials, KafkaFactory, KafkaRecord

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
except ImportError as e:  # pragma: no cover
    raise ImportError("notify_ai.kafka requires confluent-kafka: pip install 'notify-ai-client[kafka]'") from e

log = logging.getLogger("notify_ai.kafka")

DEFAULT_BROKER = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092"


@dataclass
class KafkaConfig:
    """Mirrors the Java SDK's kafka.* properties."""

    brokers: str = DEFAULT_BROKER
    group_id: str = "vocab-agent-group"
    #: Default "<clientToken>-producer" / "-consumer".
    client_id: str = ""
    #: PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL. Registration API key/secret force SASL_SSL + PLAIN.
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_username: str = ""
    sasl_password: str = ""
    compression: str = "snappy"
    linger_ms: int = 5
    auto_offset_reset: str = "earliest"
    #: Extra librdkafka settings applied to producer and consumer.
    extra: Dict[str, Any] = field(default_factory=dict)


class ConfluentKafkaTransport:
    def __init__(self, cfg: KafkaConfig, creds: KafkaCredentials) -> None:
        self.cfg = cfg
        common: Dict[str, Any] = {"bootstrap.servers": cfg.brokers}
        protocol, mechanism, user, password = cfg.security_protocol, cfg.sasl_mechanism, cfg.sasl_username, cfg.sasl_password
        if creds.api_key and creds.api_secret:
            protocol, mechanism, user, password = "SASL_SSL", "PLAIN", creds.api_key, creds.api_secret
        if protocol and protocol.upper() != "PLAINTEXT":
            common["security.protocol"] = protocol
            if protocol.upper().startswith("SASL"):
                common.update({"sasl.mechanism": mechanism, "sasl.username": user, "sasl.password": password})
        common.update(cfg.extra)

        def cid(role: str) -> str:
            return cfg.client_id or (f"{creds.client_id}-{role}" if creds.client_id else "notify-ai-client")

        self._consumer_conf = dict(common, **{
            "client.id": cid("consumer"), "group.id": cfg.group_id,
            "auto.offset.reset": cfg.auto_offset_reset, "enable.auto.commit": False,
        })
        self._producer = Producer(dict(common, **{
            "client.id": cid("producer"), "acks": "all", "enable.idempotence": True,
            "compression.type": cfg.compression, "linger.ms": cfg.linger_ms,
        }))

    def partitions(self, topic: str) -> int:
        meta = self._producer.list_topics(topic, timeout=15)
        t = meta.topics.get(topic)
        if t is None or t.error is not None:
            raise KafkaException(t.error if t else KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
        return len(t.partitions)

    def produce(self, records: List[KafkaRecord]) -> None:
        errors: List[Any] = []

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(err)

        for r in records:
            self._producer.produce(r.topic, value=r.value, key=r.key.encode(), partition=r.partition,
                                   headers=[(k, v.encode()) for k, v in r.headers.items()], on_delivery=on_delivery)
        remaining = self._producer.flush(30)
        if errors:
            raise KafkaException(errors[0])
        if remaining:
            raise TimeoutError(f"{remaining} Kafka record(s) not delivered")

    def consume(self, topic: str, handle: Callable[[bytes], None], stop: threading.Event) -> None:
        consumer = Consumer(self._consumer_conf)
        consumer.subscribe([topic])
        log.info("consuming scheduled events from %s (group %s)", topic, self.cfg.group_id)
        try:
            while not stop.is_set():
                msgs = consumer.consume(num_messages=100, timeout=0.5)
                for msg in msgs:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            log.warning("fetch error: %s", msg.error())
                        continue
                    handle(msg.value())
                if msgs:
                    consumer.commit(asynchronous=False)
        finally:
            consumer.close()

    def close(self) -> None:
        self._producer.flush(10)


def kafka_factory(cfg: Optional[KafkaConfig] = None) -> KafkaFactory:
    return lambda creds: ConfluentKafkaTransport(cfg or KafkaConfig(), creds)
