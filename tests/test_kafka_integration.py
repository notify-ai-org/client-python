"""Runs the SDK over a real Kafka protocol endpoint. Opt-in:

    NOTIFY_TEST_KAFKA_BROKERS=localhost:9092 pytest tests/test_kafka_integration.py

The broker needs topics notify-v1-events (12 partitions) and
notify-v1-scheduled-events.
"""

import json
import os
import time

import pytest

confluent_kafka = pytest.importorskip("confluent_kafka")
BROKERS = os.environ.get("NOTIFY_TEST_KAFKA_BROKERS")
pytestmark = pytest.mark.skipif(not BROKERS, reason="NOTIFY_TEST_KAFKA_BROKERS not set")

from notify_ai import (EVENTS_TOPIC, SCHEDULED_EVENTS_TOPIC, Config, ModelRegistry, NotifyClient, SmsSubject,
                       current_schedule)
from notify_ai.kafka import KafkaConfig, kafka_factory


def test_client_over_real_kafka(acp):
    acp.token = acp.token  # tenant-42
    client = NotifyClient(Config(acp_server_url=acp.url, client_token="client-123", kafka_enabled=True,
                                 flush_interval=0.01, flush_on_exit=False),
                          kafka=kafka_factory(KafkaConfig(brokers=BROKERS, group_id=f"py-{time.time()}")),
                          registry=ModelRegistry())
    emit = client.emitter("ORDER_PLACED", priority=2)

    @client.subject_supplier("ORDER_PLACED")
    def subjects(p):
        return [SmsSubject(p["customerId"])]

    @client.vocabulary_supplier("REMINDER")
    def reminder(_):
        return {"orderId": "scheduled-" + current_schedule()["id"]}

    client.start()
    try:
        assert client.wait_ready(20)
        emit({"orderId": "O-1", "customerId": "CUST-1"})
        pub = confluent_kafka.Producer({"bootstrap.servers": BROKERS})
        pub.produce(SCHEDULED_EVENTS_TOPIC, b'{"id":"s-1","eventName":"REMINDER","triggerType":"DELAY"}')
        pub.flush(10)

        reader = confluent_kafka.Consumer({"bootstrap.servers": BROKERS, "group.id": f"reader-{time.time()}",
                                           "auto.offset.reset": "earliest"})
        reader.subscribe([EVENTS_TOPIC])
        got = {}
        deadline = time.time() + 30
        while len(got) < 3 and time.time() < deadline:
            for msg in reader.consume(num_messages=50, timeout=0.5):
                if not msg.error():
                    got[msg.key().decode()] = msg
        reader.close()
    finally:
        client.close()

    expected = {"tenant-42:ORDER_PLACED": 7, "tenant-42:ORDER_PLACED:CUST-1": 11, "tenant-42:REMINDER": 7}
    assert {k: got[k].partition() for k in expected if k in got} == expected
    for key in expected:
        assert dict(got[key].headers())["Authorization"] == ("Bearer " + acp.token).encode()
    scheduled = json.loads(got["tenant-42:REMINDER"].value())
    assert scheduled["event"]["eventType"] == "SCHEDULED" and scheduled["payload"] == {"orderId": "scheduled-s-1"}
