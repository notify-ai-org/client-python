<p align="center">
  <img src="assets/notify-ai-logo.svg" alt="Notify.ai" width="96" />
</p>

<h1 align="center">Notify.ai</h1>

<p align="center"><b>Python Client SDK</b> — event capture and metadata transmission for Python services</p>

---

## 📖 Overview

The Python counterpart of the Java client SDK (`dev.notify-ai:notify-ai-agent-client`) and the Go SDK. It speaks the same protocol to acp-server (registration, vocabulary, rules and event captures over HTTP or Kafka), so Python, Go and Java services look the same to the control plane.

The core has no dependencies (stdlib only). Kafka support is an optional extra.

| Java | Python |
|---|---|
| `@EnableNotify` + `notify.ai.properties.*` | `NotifyClient(Config(...))` + `client.start()` (or `Config.from_env()`) |
| `@Model` / `@Vocabulary` | `@model("description")` + `Annotated[T, Vocabulary("name", "description")]` |
| `@Event` | `@client.event("KEY", priority=..., ...)` on a function or method (sync or `async`) |
| `@Rule` | `@client.rule("name", event="KEY")` |
| `@SubjectSupplier` | `@client.subject_supplier("KEY")` |
| `@VocabularySupplier` | `@client.vocabulary_supplier("KEY")` |
| `@Callback(when = BEFORE/AFTER)` | `@client.callback("KEY", BEFORE)` / `AFTER` |
| `EmailSubject`, `SmsSubject`, … | `EmailSubject(...)`, `SmsSubject(...)`, … (same JSON) |

## 🚀 Quick start

```bash
pip install notify-ai-client            # HTTP transport
pip install 'notify-ai-client[kafka]'   # + Kafka transport (confluent-kafka)
```

```python
from dataclasses import dataclass
from typing import Annotated

from notify_ai import Config, EmailSubject, NotifyClient, Vocabulary, model

@model("Payload for order placement events")
@dataclass
class OrderPayload:
    order_id: Annotated[str, Vocabulary("orderId", "Unique order identifier")]
    customer_id: Annotated[str, Vocabulary("customerId", "Customer who placed the order")]
    amount: Annotated[float, Vocabulary("amount", "Total order amount in USD")]

client = NotifyClient(Config(acp_server_url="http://localhost:8080",
                             application_name="orders-service", client_token="client-..."))

class OrderService:
    @client.event("ORDER_PLACED", description="Customer placed an order", event_type="static",
                  schedule_intent="immediate", preferred_time_window="09:00-18:00", priority=2)
    def place_order(self, payload: OrderPayload) -> OrderPayload:
        ...

@client.subject_supplier("ORDER_PLACED", "Order customer")
def order_subjects(p: OrderPayload):
    c = customers.get(p.customer_id)
    return [EmailSubject(c.email, attributes={"firstName": c.first_name})]

@client.rule("fraud-check", event="ORDER_PLACED")
def fraud_check(p: OrderPayload) -> bool:
    return p.amount < 1000

client.start()   # non-blocking; captures are flushed at exit (or call client.close())
```

**The payload** is the first argument of the event function. For methods it is the first argument after `self`/`cls`, which are recognized by name. The payload's type annotation registers it as a model automatically. Payloads can be:
- dataclasses, pydantic models or annotated classes: their vocabulary fields become the payload. If no field is annotated with `Vocabulary`, every field is used.
- dicts: their own keys become the payload.

`client.emitter("KEY", priority=1)` returns `emit(payload)` for call sites that just need to fire an event.

A complete port of the e-commerce example (every hook type) is in [examples/ecommerce.py](examples/ecommerce.py).

## 🔁 What a decorated call does

Same sequence as the Java `EventListener` aspect:

1. `BEFORE` callbacks (errors are logged and ignored)
2. Your function. Exceptions are recorded and re-raised unchanged.
3. `AFTER` callbacks
4. The subject supplier and every rule for the event, plus rules registered without an event (`"*"`). Each subject must have a non-blank address.
5. A capture is buffered with the flattened payload, the call stack (5 application frames), duration, result or exception, subjects and rule results

A background thread sends buffered records to acp-server, so this works the same in sync, threaded and asyncio apps.

**Async:** `async def` events are wrapped as coroutines, and their hooks may also be `async`. A synchronous event cannot await an `async` hook; that hook's result is recorded as an error.

## ⚙️ Configuration

| `Config` field | Env var (`Config.from_env()`) | Default |
|---|---|---|
| `acp_server_url` | `NOTIFY_AI_ACP_SERVER_URL` | `https://app.notify-ai.dev` |
| `application_name` | `NOTIFY_AI_APPLICATION_NAME` | `notify-client` |
| `client_token` | `NOTIFY_AI_CLIENT_TOKEN` | — |
| `base_package` | `NOTIFY_AI_BASE_PACKAGE` | — |
| `buffer_batch_size` | `NOTIFY_AI_BUFFER_BATCH_SIZE` | 100 |
| `flush_interval` (seconds) | `NOTIFY_AI_FLUSH_INTERVAL_MS` | 0.1 |
| `max_buffer_size` | `NOTIFY_AI_MAX_BUFFER_SIZE` | 10000 (negative = unbounded) |
| `kafka_enabled` | `NOTIFY_AI_KAFKA_ENABLED` | False |
| `flush_on_exit` | — | True (flushes in an `atexit` hook) |

Logs go to the `notify_ai` logger.

## 📨 Kafka

```python
from notify_ai.kafka import KafkaConfig, kafka_factory

client = NotifyClient(Config(..., kafka_enabled=True),
                      kafka=kafka_factory(KafkaConfig(brokers="broker:9092")))
```

Behavior matches the Java dispatcher and the Go SDK:
- Captures go to `notify-v1-events`: one record per subject with key `tenantId:event:subjectId`, partitioned by Java's `String.hashCode` so a subject lands on the same partition from any SDK.
- The `Authorization: Bearer …` header carries the Kafka header token from registration.
- An access token with `profile: basic` forces HTTP.
- An API key and secret returned at registration switch the connection to SASL_SSL / PLAIN.
- Scheduled triggers from `notify-v1-scheduled-events` become `SCHEDULED` captures whose payload comes from the event's vocabulary supplier. Inside it, `current_schedule()` returns the trigger.
- Payloads with credential-like fields are rejected before publishing (`validate_credential_free`).

## ↔️ Differences from the Java SDK

Wire format and endpoints are identical. Runtime behavior follows the Go SDK:
- Registration is retried with backoff instead of disabling the SDK.
- The buffer is capped (oldest capture dropped first).
- Failed sends are requeued in order with backoff.
- Token refresh happens before expiry as well as on 401, and sends the registered client id.
- On Kafka, a capture with no subjects is sent as one tenant-keyed record.
- `close()` and process exit flush pending captures.

See the [Go SDK README](../client-go/README.md#️-differences-from-the-java-sdk) for the full table.

## 🧪 Development

```bash
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
# Optional, against a real broker with notify-v1-events (12 partitions) and notify-v1-scheduled-events:
NOTIFY_TEST_KAFKA_BROKERS=localhost:9092 .venv/bin/pytest tests/test_kafka_integration.py
```
