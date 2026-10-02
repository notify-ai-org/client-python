import asyncio
import dataclasses
import json
import threading
import time
from typing import Annotated, List

import pytest

from notify_ai import (AFTER, BEFORE, Config, EmailSubject, KafkaRecord, ModelRegistry, NotifyClient, SmsSubject,
                       Vocabulary, current_schedule, model, EVENTS_TOPIC)
from conftest import fake_jwt

REGISTRY = ModelRegistry()


@dataclasses.dataclass
class OrderPayload:
    order_id: Annotated[str, Vocabulary("orderId", "Unique order identifier")]
    customer_id: Annotated[str, Vocabulary("customerId", "Customer who placed the order")] = ""
    amount: Annotated[float, Vocabulary("amount", "Total order amount in USD")] = 0.0


OrderPayload.__notify_description__ = "Payload for order placement events"


def make_client(acp, **kw):
    cfg = Config(acp_server_url=acp.url, application_name="test-app", client_token="client-123",
                 base_package="shop", flush_interval=0.01, flush_on_exit=False, **kw.pop("cfg", {}))
    return NotifyClient(cfg, registry=kw.pop("registry", ModelRegistry()), **kw)


def ready(client):
    assert client.wait_ready(5), "registration did not complete"


def make_service(client):
    class OrderService:
        def __init__(self):
            self.placed = []

        @client.event("ORDER_PLACED", description="Customer placed an order", event_type="static",
                      schedule_intent="immediate", preferred_time_window="09:00-18:00", priority=2)
        def place_order(self, payload: OrderPayload) -> OrderPayload:
            if payload.amount < 0:
                raise ValueError("negative amount")
            self.placed.append(payload.order_id)
            return payload

    return OrderService()


def test_end_to_end_http(acp):
    client = make_client(acp)
    svc = make_service(client)
    calls = []

    @client.subject_supplier("ORDER_PLACED", "order customer")
    def subjects(p: OrderPayload):
        return [EmailSubject(f"{p.customer_id}@example.com" if p.customer_id else "", attributes={"firstName": "A"})]

    @client.rule("fraud-check", event="ORDER_PLACED", description="Blocks large orders")
    def fraud(p):
        return p.amount < 1000

    @client.rule("always")
    def always(p):
        return True

    @client.callback("ORDER_PLACED", BEFORE)
    def before(p):
        calls.append("before:" + p.order_id)

    @client.callback("ORDER_PLACED", AFTER)
    def after(p):
        calls.append("after:" + p.order_id)
        raise RuntimeError("ignored")

    svc.place_order(OrderPayload("O-0", "alice", 5))  # before start: sent after registration records
    client.start()
    try:
        ready(client)
        assert svc.place_order(OrderPayload("O-1", "alice", 120)).order_id == "O-1"
        with pytest.raises(ValueError):
            svc.place_order(OrderPayload("O-2", "", -1))
        client.flush()
    finally:
        client.close()

    assert calls == ["before:O-0", "after:O-0", "before:O-1", "after:O-1", "before:O-2", "after:O-2"]
    assert acp.registrations[0] == {"clientId": "client-123", "applicationName": "test-app", "basePackage": "shop"}
    assert acp.captures[0]["event"]["eventType"] == "REGISTER"
    (vocab,) = acp.vocabulary
    assert vocab[0]["className"] == "OrderPayload" and vocab[0]["classDescription"] == "Payload for order placement events"
    assert [r["ruleName"] for r in acp.rules] == ["fraud-check", "always"] and acp.rules[1]["eventName"] == "*"

    (register,) = acp.of_type("REGISTER")
    assert register["event"]["priority"] == 2 and register["event"]["scheduleIntent"] == "immediate"
    md = register["payload"]["EventMetadata"]
    assert md["methodName"] == "place_order" and md["version"] == "v1"
    assert md["declaringClassName"].endswith("OrderService") and md["returnTypeName"].endswith("OrderPayload")
    assert register["serviceName"] == "OrderService"

    users = acp.of_type("USER")
    assert len(users) == 3
    ok = users[1]
    assert ok["payload"] == {"orderId": "O-1", "customerId": "alice", "amount": 120}
    assert ok["timestamp"].endswith("Z") and ok["occuredAt"] == ok["timestamp"]
    assert ok["result"]["success"] and '"order_id":"O-1"' in ok["result"]["returnValue"]
    assert ok["subjectResult"]["subjects"][0]["email"] == "alice@example.com"
    assert [r["result"] for r in ok["ruleResults"]] == [True, True]
    assert "test_end_to_end_http" in ok["callStack"]["frames"][0]["methodName"]

    failed = users[2]
    assert not failed["result"]["success"] and failed["exception"]["message"] == "negative amount"
    assert failed["exception"]["exceptionType"] == "builtins.ValueError"
    assert "without a destination address" in failed["subjectResult"]["errorMessage"]

    m = client.metrics()
    assert m.event_capture_count == 3 and m.rule_invoke_count == 6 and m.subject_supplier_invoke_count == 3


async def test_async_events_and_hooks(acp):
    client = make_client(acp)

    @client.event("ASYNC_EVENT", priority=1)
    async def handle(payload: OrderPayload) -> str:
        await asyncio.sleep(0)
        return "done"

    @client.subject_supplier("ASYNC_EVENT")
    async def subjects(p):
        await asyncio.sleep(0)
        return [SmsSubject("+1555")]

    @client.rule("async-rule", event="ASYNC_EVENT")
    async def rule(p):
        return p.amount > 1

    client.start()
    try:
        assert await asyncio.to_thread(client.wait_ready, 5)
        assert await handle(OrderPayload("A-1", amount=2)) == "done"
        await asyncio.to_thread(client.flush)
    finally:
        client.close()
    (capture,) = acp.of_type("USER")
    assert capture["subjectResult"]["subjects"][0]["phoneNumber"] == "+1555"
    assert capture["ruleResults"][0]["result"] is True and capture["result"]["returnValue"] == '"done"'


def test_async_hook_on_sync_event_is_reported(acp):
    client = make_client(acp)
    emit = client.emitter("E", priority=1)

    @client.subject_supplier("E")
    async def subjects(p):
        return []

    client.start()
    try:
        ready(client)
        emit(OrderPayload("X"))
        client.flush()
    finally:
        client.close()
    (capture,) = acp.of_type("USER")
    assert "cannot run for a synchronous event" in capture["subjectResult"]["errorMessage"]


def test_late_registration_and_model_decorator(acp):
    registry = ModelRegistry()
    client = make_client(acp, registry=registry)
    client.start()
    try:
        ready(client)

        @dataclasses.dataclass
        class Late:
            x: int

        registry.register(Late, "late model")
        emit = client.emitter("LATE", priority=1)
        client.rule("late-rule", event="LATE")(lambda p: True)
        emit({"x": 1})
        client.flush()
    finally:
        client.close()
    assert len(acp.of_type("REGISTER")) == 1 and acp.of_type("USER")[0]["payload"] == {"x": 1}
    assert [m[0]["className"] for m in acp.vocabulary] == ["Late"] and len(acp.rules) == 1


def test_token_refresh_on_401(acp):
    client = make_client(acp)
    emit = client.emitter("E", priority=1)
    client.start()
    try:
        ready(client)
        client.flush()
        acp.reject_401_once = True
        emit({"orderId": "X"})
        client.flush()
    finally:
        client.close()
    assert acp.refreshed == 1 and acp.captures[-1]["payload"] == {"orderId": "X"}


def test_registration_retries_and_buffers(acp):
    acp.fail_registers = 1
    client = make_client(acp)
    emit = client.emitter("E", priority=1)
    client.start()
    try:
        emit({"orderId": "queued"})
        ready(client)
        deadline = time.time() + 5
        while not acp.of_type("USER") and time.time() < deadline:
            time.sleep(0.02)
    finally:
        client.close()
    assert len(acp.of_type("USER")) == 1


def test_invalid_specs_raise():
    client = NotifyClient(Config(flush_on_exit=False), registry=ModelRegistry())
    with pytest.raises(ValueError):
        client.event("", priority=1)
    with pytest.raises(ValueError):
        client.event("K", priority=6)
    with pytest.raises(ValueError):
        client.rule("")


class FakeKafka:
    def __init__(self):
        self.records: List[KafkaRecord] = []
        self.creds = None
        self.schedule = []
        self.lock = threading.Lock()

    def partitions(self, topic):
        return 12

    def produce(self, records):
        with self.lock:
            self.records.extend(records)

    def consume(self, topic, handle, stop):
        while not stop.is_set():
            if self.schedule:
                handle(self.schedule.pop())
            stop.wait(0.01)

    def close(self):
        pass


def test_kafka_partitioning_and_scheduled_events(acp):
    kafka = FakeKafka()

    def factory(creds):
        kafka.creds = creds
        return kafka

    client = make_client(acp, kafka=factory, cfg={"kafka_enabled": True})
    emit = client.emitter("ORDER_PLACED", priority=2)
    leak = client.emitter("LEAK", priority=1)

    @client.subject_supplier("ORDER_PLACED")
    def subjects(p):
        return [SmsSubject("CUST-1"), EmailSubject("alice@example.com")]

    @client.vocabulary_supplier("REMINDER")
    def reminder(_payload):
        return {"orderId": "from-" + current_schedule()["id"]}

    client.start()
    try:
        ready(client)
        assert kafka.creds.client_id == "client-123"
        emit(OrderPayload("K-1"))
        leak({"password": "hunter2"})
        kafka.schedule.append(b'{"id":"s-1","eventName":"REMINDER","triggerType":"DELAY"}')
        deadline = time.time() + 5
        while len(kafka.records) < 5 and time.time() < deadline:
            time.sleep(0.02)
        client.flush()
    finally:
        client.close()

    by_key = {r.key: r for r in kafka.records}
    for r in kafka.records:
        assert r.topic == EVENTS_TOPIC and r.headers["Authorization"] == "Bearer " + acp.token
        assert b"hunter2" not in r.value
    # Must equal Java's Math.abs(hashCode) % 12.
    assert {k: by_key[k].partition for k in (
        "tenant-42:ORDER_PLACED:CUST-1", "tenant-42:ORDER_PLACED:alice@example.com",
        "tenant-42:ORDER_PLACED", "tenant-42:REMINDER")} == {
        "tenant-42:ORDER_PLACED:CUST-1": 11, "tenant-42:ORDER_PLACED:alice@example.com": 1,
        "tenant-42:ORDER_PLACED": 7, "tenant-42:REMINDER": 7}
    scheduled = json.loads(by_key["tenant-42:REMINDER"].value)
    assert scheduled["event"]["eventType"] == "SCHEDULED" and scheduled["payload"] == {"orderId": "from-s-1"}
    assert not acp.of_type("USER")


def test_basic_profile_forces_http(acp):
    acp.token = fake_jwt({"tenantId": "t", "profile": "basic"})
    kafka = FakeKafka()
    client = make_client(acp, kafka=lambda creds: kafka, cfg={"kafka_enabled": True})
    emit = client.emitter("E", priority=1)
    client.start()
    try:
        ready(client)
        emit({"orderId": "B"})
        client.flush()
    finally:
        client.close()
    assert not kafka.records and len(acp.of_type("USER")) == 1
