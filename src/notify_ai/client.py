"""The SDK runtime: Bootstrapper, EventListener, InvokeManager, Dispatcher,
AcpServerClient and TokenHolder from the Java SDK in one client."""

from __future__ import annotations

import asyncio
import atexit
import contextvars
import datetime as _dt
import functools
import inspect
import json
import logging
import os
import sys
import threading
import time
import traceback
import typing
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, TypeVar

from .acp import AcpClient, NotifyHTTPError
from .config import Config
from .policy import CredentialsInPayloadError, validate_credential_free
from .subjects import Subject
from .transport import EVENTS_TOPIC, SCHEDULED_EVENTS_TOPIC, KafkaCredentials, KafkaFactory, KafkaRecord, KafkaTransport
from .util import dumps, has_basic_profile, iso_instant, java_partition, tenant_id_from_token, to_jsonable
from .vocabulary import ModelRegistry, _ModelMeta, default_registry

log = logging.getLogger("notify_ai")

F = TypeVar("F", bound=Callable[..., Any])

BEFORE = "BEFORE"
AFTER = "AFTER"

_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__)) + os.sep
_current_schedule: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar("notify_schedule", default=None)


def current_schedule() -> Optional[Dict[str, Any]]:
    """Inside a vocabulary supplier invoked for a scheduled trigger, the EventSchedule."""
    return _current_schedule.get()


@dataclass
class Metrics:
    """MetricsManager counters (+ dropped_capture_count)."""

    rule_invoke_count: int = 0
    vocabulary_supplier_invoke_count: int = 0
    subject_supplier_invoke_count: int = 0
    event_capture_count: int = 0
    total_rule_invoke_ms: int = 0
    total_vocabulary_supplier_ms: int = 0
    total_subject_supplier_ms: int = 0
    dropped_capture_count: int = 0


@dataclass
class _EventMeta:
    key: str
    description: str
    preferred_time_window: str
    event_type: str
    schedule_intent: str
    priority: int
    version: str
    method_name: str
    declaring_class_name: str
    return_type_name: str
    parameter_type_names: List[str]
    service_name: str


@dataclass
class _Hook:
    name: str
    event: str
    description: str
    fn: Callable[..., Any]


VOCABULARY, RULE, EVENT_CAPTURE = "VOCABULARY", "RULE", "EVENT_CAPTURE"


@dataclass
class _Record:
    type: str
    payload: Any


class _Buffer:
    """Thread-safe queue; capped, dropping the oldest event capture when full."""

    def __init__(self, max_size: int, on_drop: Callable[[], None]) -> None:
        self._lock = threading.Lock()
        self._records: Deque[_Record] = deque()
        self._max = max_size
        self._on_drop = on_drop
        self.wake = threading.Event()

    def add(self, record: _Record) -> None:
        with self._lock:
            if self._max > 0 and len(self._records) >= self._max:
                for old in self._records:
                    if old.type == EVENT_CAPTURE:
                        self._records.remove(old)
                        self._on_drop()
                        break
            self._records.append(record)
        self.wake.set()

    def drain(self, n: int) -> List[_Record]:
        with self._lock:
            out = []
            while self._records and len(out) < n:
                out.append(self._records.popleft())
            return out

    def requeue(self, records: List[_Record]) -> None:
        with self._lock:
            self._records.extendleft(reversed(records))

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)


class _Tokens:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.access: Optional[str] = None
        self.refresh: Optional[str] = None
        self.header: Optional[str] = None
        self._expires_at = 0.0

    def set(self, access: Optional[str], expires_in_ms: int, refresh: Optional[str] = None,
            header: Optional[str] = None, all_tokens: bool = False) -> None:
        with self._lock:
            self.access = access
            if all_tokens:
                if refresh:
                    self.refresh = refresh
                self.header = header
            self._expires_at = time.time() + expires_in_ms / 1000 if expires_in_ms and expires_in_ms > 0 else float("inf")

    def expired(self) -> bool:
        with self._lock:
            return bool(self.access) and time.time() >= self._expires_at

    def kafka_header(self) -> Optional[str]:
        with self._lock:
            return self.header or self.access


def _qualified(tp: Any) -> str:
    if tp is None or tp is inspect.Signature.empty:
        return "Any"
    if tp is type(None):
        return "None"
    if isinstance(tp, type):
        return tp.__name__ if tp.__module__ == "builtins" else f"{tp.__module__}.{tp.__qualname__}"
    return str(tp).replace("typing.", "")


def _owner(fn: Callable[..., Any]) -> Tuple[str, str]:
    """(declaring class name, service name) for a function or method."""
    parts = [p for p in fn.__qualname__.split(".") if p != "<locals>"]
    module = fn.__module__ or ""
    if len(parts) > 1:
        cls = parts[-2]
        return f"{module}.{cls}", cls
    return module, module.rsplit(".", 1)[-1]


class NotifyClient:
    """Register hooks with the decorators, then call :meth:`start`.

    Decorated event functions may be sync or async. Hooks (rules, suppliers,
    callbacks) receive the event payload; they may be async for async events.
    """

    def __init__(self, config: Optional[Config] = None, *, kafka: Optional[KafkaFactory] = None,
                 registry: ModelRegistry = default_registry) -> None:
        self.config = config or Config()
        self._acp = AcpClient(self.config.acp_server_url)
        self._registry = registry
        self._kafka_factory = kafka
        self._kafka: Optional[KafkaTransport] = None
        self._partitions = 0
        self._tokens = _Tokens()
        self._metrics = Metrics()
        self._metrics_lock = threading.Lock()
        self._buffer = _Buffer(self.config.max_buffer_size, lambda: self._count(dropped_capture_count=1))

        self._lock = threading.RLock()  # guards registration state
        self._events: List[_EventMeta] = []
        self._rules: List[_Hook] = []
        self._subject_suppliers: Dict[str, _Hook] = {}
        self._vocabulary_suppliers: Dict[str, _Hook] = {}
        self._callbacks: Dict[str, Dict[str, List[_Hook]]] = {BEFORE: {}, AFTER: {}}
        self._registered = False
        self._client_id = self.config.client_token
        self._registration: Dict[str, Any] = {}

        self._started = False
        self._closed = False
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._dispatch_lock = threading.Lock()
        self._threads: List[threading.Thread] = []
        registry.subscribe(self._on_model_registered)

    # ── registration (the annotation equivalents) ─────────────────────────────

    def register_model(self, cls: type, description: str = "") -> None:
        """Mirror of @Model for classes not decorated with :func:`notify_ai.model`."""
        self._registry.register(cls, description)

    def event(self, key: str, *, priority: int, description: str = "", preferred_time_window: str = "",
              event_type: str = "", schedule_intent: str = "", version: str = "v1",
              service_name: Optional[str] = None) -> Callable[[F], F]:
        """Mirror of @Event. The payload is the first argument (after ``self``/``cls``)."""
        if not key or not key.strip():
            raise ValueError("event key must not be blank")
        if not 1 <= priority <= 5:
            raise ValueError(f"event {key!r} has priority={priority}; must be between 1 and 5")

        def decorate(fn: F) -> F:
            params = list(inspect.signature(fn).parameters.values())
            index = 1 if params and params[0].name in ("self", "cls") else 0
            payload_param = params[index].name if len(params) > index else None
            try:
                hints = typing.get_type_hints(fn)
            except Exception:
                hints = {}
            payload_type = hints.get(payload_param) if payload_param else None
            if isinstance(payload_type, type):
                self._registry.register(payload_type, getattr(payload_type, "__notify_description__", ""))
            declaring, service = _owner(fn)
            meta = _EventMeta(
                key=key, description=description, preferred_time_window=preferred_time_window,
                event_type=event_type, schedule_intent=schedule_intent, priority=priority, version=version,
                method_name=fn.__name__, declaring_class_name=declaring,
                return_type_name=_qualified(hints.get("return")),
                parameter_type_names=[_qualified(hints.get(p.name)) for p in params[index:]],
                service_name=service_name or service,
            )
            self._add_event(meta)

            def payload_of(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
                if len(args) > index:
                    return args[index]
                return kwargs.get(payload_param) if payload_param else None

            if inspect.iscoroutinefunction(fn):
                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    payload = payload_of(args, kwargs)
                    frames = _caller_frames()
                    start = time.monotonic()
                    await self._run_callbacks_async(BEFORE, key, payload)
                    try:
                        result = await fn(*args, **kwargs)
                    except BaseException as exc:
                        await self._run_callbacks_async(AFTER, key, payload)
                        await self._capture_async(meta, payload, None, exc, start, frames)
                        raise
                    await self._run_callbacks_async(AFTER, key, payload)
                    await self._capture_async(meta, payload, result, None, start, frames)
                    return result

                return typing.cast(F, async_wrapper)

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                payload = payload_of(args, kwargs)
                frames = _caller_frames()
                start = time.monotonic()
                self._run_callbacks(BEFORE, key, payload)
                try:
                    result = fn(*args, **kwargs)
                except BaseException as exc:
                    self._run_callbacks(AFTER, key, payload)
                    self._capture(meta, payload, None, exc, start, frames, self._call_sync)
                    raise
                self._run_callbacks(AFTER, key, payload)
                self._capture(meta, payload, result, None, start, frames, self._call_sync)
                return result

            return typing.cast(F, wrapper)

        return decorate

    def emitter(self, key: str, **spec: Any) -> Callable[[Any], None]:
        """Register an event with no business logic; returns ``emit(payload)``."""

        @self.event(key, **spec)
        def emit(payload: Any) -> None:
            return None

        emit.__qualname__ = emit.__name__ = "emit_" + key.lower()
        return emit

    def rule(self, name: str, event: str = "", description: str = "") -> Callable[[F], F]:
        """Mirror of @Rule. An empty event applies the rule to every event ("*")."""
        if not name or not name.strip():
            raise ValueError("rule name must not be blank")

        def decorate(fn: F) -> F:
            hook = _Hook(name, event or "*", description, fn)
            with self._lock:
                self._rules.append(hook)
                if self._registered:
                    self._buffer.add(_rule_record(hook))
            return fn

        return decorate

    def subject_supplier(self, event: str, description: str = "") -> Callable[[F], F]:
        """Mirror of @SubjectSupplier: returns the list of Subjects for an event."""
        return self._supplier(self._subject_suppliers, event, description)

    def vocabulary_supplier(self, event: str, description: str = "") -> Callable[[F], F]:
        """Mirror of @VocabularySupplier: supplies the payload for scheduled triggers.
        Receives ``None``; use :func:`current_schedule` to read the trigger."""
        return self._supplier(self._vocabulary_suppliers, event, description)

    def _supplier(self, table: Dict[str, _Hook], event: str, description: str) -> Callable[[F], F]:
        if not event or not event.strip():
            raise ValueError("supplier event must not be blank")

        def decorate(fn: F) -> F:
            with self._lock:
                table[event] = _Hook(fn.__name__, event, description, fn)
            return fn

        return decorate

    def callback(self, event: str, when: str = BEFORE) -> Callable[[F], F]:
        """Mirror of @Callback. Errors are logged and never affect the event."""
        if not event or not event.strip():
            raise ValueError("callback event must not be blank")
        if when not in (BEFORE, AFTER):
            raise ValueError("when must be BEFORE or AFTER")

        def decorate(fn: F) -> F:
            with self._lock:
                self._callbacks[when].setdefault(event, []).append(_Hook(fn.__name__, event, "", fn))
            return fn

        return decorate

    def _add_event(self, meta: _EventMeta) -> None:
        with self._lock:
            self._events.append(meta)
            if self._registered:
                self._buffer.add(_Record(EVENT_CAPTURE, _register_capture(meta)))

    def _on_model_registered(self, meta: _ModelMeta) -> None:
        with self._lock:
            if self._registered:
                self._buffer.add(_Record(VOCABULARY, [self._registry.class_model(meta)]))

    # ── capture (EventListener#aroundEvent) ──────────────────────────────────

    def _count(self, **deltas: int) -> None:
        with self._metrics_lock:
            for k, v in deltas.items():
                setattr(self._metrics, k, getattr(self._metrics, k) + v)

    def _base_capture(self, meta: _EventMeta, payload: Any, result: Any, exc: Optional[BaseException],
                      start: float, frames: List[Dict[str, Any]]) -> Dict[str, Any]:
        capture: Dict[str, Any] = {
            "timestamp": iso_instant(_dt.datetime.now(_dt.timezone.utc)),
            "payload": self._registry.flatten(payload),
            "event": {"name": meta.key, "eventType": "USER", "description": meta.description, "priority": 0},
            "callStack": {"frames": frames},
            "durationMillis": int((time.monotonic() - start) * 1000),
            "serviceName": meta.service_name,
        }
        capture["occuredAt"] = capture["timestamp"]
        if exc is not None:
            capture["exception"] = {
                "exceptionType": f"{type(exc).__module__}.{type(exc).__qualname__}",
                "message": str(exc),
                "stackTrace": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            }
            capture["result"] = {"success": False, "returnValue": None}
        else:
            capture["result"] = {"success": True, "returnValue": None if result is None else dumps(result)}
        return capture

    def _capture(self, meta: _EventMeta, payload: Any, result: Any, exc: Optional[BaseException],
                 start: float, frames: List[Dict[str, Any]], call: Callable[[Callable[..., Any], Any], Any]) -> None:
        try:
            capture = self._base_capture(meta, payload, result, exc, start, frames)
            capture["subjectResult"] = self._subject_result(meta.key, payload, call)
            capture["ruleResults"] = self._rule_results(meta.key, payload, call)
            self._enqueue_capture(capture)
        except Exception:
            log.exception("notify: failed to build capture for %s", meta.key)

    async def _capture_async(self, meta: _EventMeta, payload: Any, result: Any, exc: Optional[BaseException],
                             start: float, frames: List[Dict[str, Any]]) -> None:
        # Resolve async hooks first, then build the capture with their results.
        resolved: Dict[int, Any] = {}
        hooks = [h for h in [self._subject_suppliers.get(meta.key)] if h] + self._rules_for(meta.key)
        for hook in hooks:
            try:
                value = hook.fn(payload)
                if inspect.isawaitable(value):
                    value = await value
                resolved[id(hook)] = (value, None)
            except Exception as e:  # noqa: BLE001
                resolved[id(hook)] = (None, e)

        def call(fn: Callable[..., Any], _payload: Any) -> Any:
            for hook in hooks:
                if hook.fn is fn:
                    value, error = resolved[id(hook)]
                    if error:
                        raise error
                    return value
            return fn(_payload)

        self._capture(meta, payload, result, exc, start, frames, call)

    @staticmethod
    def _call_sync(fn: Callable[..., Any], payload: Any) -> Any:
        value = fn(payload)
        if inspect.isawaitable(value):
            if hasattr(value, "close"):
                value.close()
            raise TypeError(f"async hook {fn.__name__} cannot run for a synchronous event")
        return value

    def _rules_for(self, key: str) -> List[_Hook]:
        with self._lock:
            return [r for r in self._rules if r.event == key] + [r for r in self._rules if r.event == "*"]

    def _subject_result(self, key: str, payload: Any, call: Callable[..., Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {"eventKey": key, "subjects": [], "executionTimeMillis": 0, "success": True,
                                  "errorMessage": None}
        hook = self._subject_suppliers.get(key)
        if hook is None:
            return result
        start = time.monotonic()
        try:
            subjects = call(hook.fn, payload)
            subjects = [] if subjects is None else (list(subjects) if isinstance(subjects, (list, tuple)) else [subjects])
            _validate_subjects(key, subjects)
            result["subjects"] = subjects
        except Exception as e:  # noqa: BLE001
            result["success"], result["errorMessage"] = False, str(e)
        elapsed = int((time.monotonic() - start) * 1000)
        result["executionTimeMillis"] = elapsed
        self._count(subject_supplier_invoke_count=1, total_subject_supplier_ms=elapsed)
        return result

    def _rule_results(self, key: str, payload: Any, call: Callable[..., Any]) -> List[Dict[str, Any]]:
        out = []
        for rule in self._rules_for(key):
            start = time.monotonic()
            entry: Dict[str, Any] = {"ruleName": rule.name, "eventKey": key, "result": None, "success": True,
                                     "errorMessage": None}
            try:
                entry["result"] = to_jsonable(call(rule.fn, payload))
            except Exception as e:  # noqa: BLE001
                entry["success"], entry["errorMessage"] = False, str(e)
            elapsed = int((time.monotonic() - start) * 1000)
            entry["executionTimeMillis"] = elapsed
            self._count(rule_invoke_count=1, total_rule_invoke_ms=elapsed)
            out.append(entry)
        return out

    def _callbacks_for(self, when: str, key: str) -> List[_Hook]:
        with self._lock:
            return list(self._callbacks[when].get(key, []))

    def _run_callbacks(self, when: str, key: str, payload: Any) -> None:
        for cb in self._callbacks_for(when, key):
            try:
                self._call_sync(cb.fn, payload)
            except Exception as e:  # noqa: BLE001
                log.warning("notify: %s callback %s for %s failed: %s", when, cb.name, key, e)

    async def _run_callbacks_async(self, when: str, key: str, payload: Any) -> None:
        for cb in self._callbacks_for(when, key):
            try:
                value = cb.fn(payload)
                if inspect.isawaitable(value):
                    await value
            except Exception as e:  # noqa: BLE001
                log.warning("notify: %s callback %s for %s failed: %s", when, cb.name, key, e)

    def _enqueue_capture(self, capture: Dict[str, Any]) -> None:
        self._buffer.add(_Record(EVENT_CAPTURE, capture))
        self._count(event_capture_count=1)

    def _on_scheduled(self, schedule: Dict[str, Any]) -> None:
        """EventListener#onScheduledEvent: a trigger from acp-server becomes a
        SCHEDULED capture whose payload comes from the vocabulary supplier."""
        name = schedule.get("eventName")
        now = iso_instant(_dt.datetime.now(_dt.timezone.utc))
        capture: Dict[str, Any] = {"timestamp": now, "occuredAt": now, "payload": None,
                                   "event": {"name": name, "eventType": "SCHEDULED", "priority": 0},
                                   "durationMillis": 0}
        hook = self._vocabulary_suppliers.get(name or "")
        if hook is not None:
            token = _current_schedule.set(schedule)
            start = time.monotonic()
            try:
                value = hook.fn(None)
                if inspect.isawaitable(value):
                    value = asyncio.run(_await(value))
                if value is not None:
                    capture["payload"] = self._registry.flatten(value)
            except Exception as e:  # noqa: BLE001
                log.warning("notify: vocabulary supplier for %s failed: %s", name, e)
            finally:
                _current_schedule.reset(token)
                elapsed = int((time.monotonic() - start) * 1000)
                self._count(vocabulary_supplier_invoke_count=1, total_vocabulary_supplier_ms=elapsed)
        self._enqueue_capture(capture)

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> "NotifyClient":
        """Register with acp-server and start the dispatcher in background threads.
        Does not block; registration is retried with backoff until it succeeds."""
        with self._lock:
            if self._started:
                raise RuntimeError("notify client already started")
            if self._closed:
                raise RuntimeError("notify client is closed")
            self._started = True
        thread = threading.Thread(target=self._run, name="notify-dispatcher", daemon=True)
        self._threads.append(thread)
        thread.start()
        if self.config.flush_on_exit:
            atexit.register(self._atexit)
        return self

    def wait_ready(self, timeout: Optional[float] = None) -> bool:
        """Block until registration completes. Returns False on timeout."""
        return self._ready.wait(timeout)

    def metrics(self) -> Metrics:
        with self._metrics_lock:
            return Metrics(**vars(self._metrics))

    def flush(self, timeout: float = 10.0) -> None:
        """Send everything currently buffered (waits for any in-flight batch)."""
        if not self._ready.is_set():
            raise RuntimeError("notify client is not registered with acp-server yet")
        deadline = time.monotonic() + timeout
        while True:
            if time.monotonic() > deadline:
                raise TimeoutError(f"{len(self._buffer)} record(s) not flushed")
            self._dispatch_once()
            if len(self._buffer) == 0:
                return

    def close(self, timeout: float = 10.0) -> None:
        """Stop background threads, flush within ``timeout``, close Kafka."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._stop.set()
        self._buffer.wake.set()
        for t in self._threads:
            t.join(timeout)
        self._registry.unsubscribe(self._on_model_registered)
        try:
            if self._ready.is_set():
                self.flush(timeout)
            elif len(self._buffer):
                log.warning("notify: closed before registration; %d record(s) discarded", len(self._buffer))
        finally:
            if self._kafka is not None:
                self._kafka.close()

    def _atexit(self) -> None:
        try:
            self.close(timeout=5)
        except Exception as e:  # noqa: BLE001
            log.warning("notify: flush at exit failed: %s", e)

    def __enter__(self) -> "NotifyClient":
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _run(self) -> None:
        if not self._register_with_retry():
            return
        if self.config.kafka_enabled:
            self._init_kafka()
        else:
            log.info("notify: Kafka disabled; event captures use HTTP transport")
        self._enqueue_registrations()
        self._ready.set()
        self._dispatch_loop()

    def _register_with_retry(self) -> bool:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                resp = self._acp.register({"clientId": self.config.client_token,
                                           "applicationName": self.config.application_name,
                                           "basePackage": self.config.base_package})
            except Exception as e:  # noqa: BLE001
                log.warning("notify: registration failed (%s); retrying in %.0fs", e, backoff)
                if self._stop.wait(backoff):
                    return False
                backoff = min(backoff * 2, 60)
                continue
            self._registration = resp
            self._client_id = resp.get("clientId") or self.config.client_token
            if resp.get("token"):
                self._tokens.set(resp["token"], resp.get("expiresInMs") or 0, resp.get("refreshToken"),
                                 resp.get("kafkaHeaderToken"), all_tokens=True)
                log.info("notify: registered with acp-server at %s", self.config.acp_server_url)
            else:
                log.warning("notify: registration response did not contain tokens")
            return True
        return False

    def _init_kafka(self) -> None:
        if self._kafka_factory is None:
            log.warning("notify: kafka_enabled is set but no kafka factory was supplied; using HTTP")
            return
        try:
            transport = self._kafka_factory(KafkaCredentials(self.config.client_token,
                                                             self._registration.get("apiKey") or "",
                                                             self._registration.get("apiSecret") or ""))
            partitions = transport.partitions(EVENTS_TOPIC)
        except Exception as e:  # noqa: BLE001
            log.error("notify: Kafka transport unavailable (%s); using HTTP", e)
            return
        if partitions <= 0:
            log.error("notify: %s has no partitions; using HTTP", EVENTS_TOPIC)
            transport.close()
            return
        self._kafka, self._partitions = transport, partitions

        def handle(value: bytes) -> None:
            try:
                schedule = json.loads(value)
            except ValueError as e:
                log.warning("notify: invalid scheduled event: %s", e)
                return
            self._on_scheduled(schedule)

        def consume() -> None:
            try:
                transport.consume(SCHEDULED_EVENTS_TOPIC, handle, self._stop)
            except Exception:  # noqa: BLE001
                if not self._stop.is_set():
                    log.exception("notify: scheduled-event consumer stopped")

        thread = threading.Thread(target=consume, name="notify-kafka-consumer", daemon=True)
        self._threads.append(thread)
        thread.start()
        log.info("notify: Kafka transport ready (%d partitions)", partitions)

    def _enqueue_registrations(self) -> None:
        """REGISTER captures, vocabulary, then rules — ahead of buffered captures."""
        with self._lock:
            records = [_Record(EVENT_CAPTURE, _register_capture(e)) for e in self._events]
            models = self._registry.class_models()
            if models:
                records.append(_Record(VOCABULARY, models))
            records += [_rule_record(r) for r in self._rules]
            self._buffer.requeue(records)
            self._registered = True

    def _dispatch_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            self._buffer.wake.wait(self.config.flush_interval)
            self._buffer.wake.clear()
            while len(self._buffer) and not self._stop.is_set():
                try:
                    self._dispatch_once()
                    backoff = 1.0
                except Exception as e:  # noqa: BLE001
                    log.error("notify: dispatch failed (%s); retrying in %.0fs", e, backoff)
                    if self._stop.wait(backoff):
                        return
                    backoff = min(backoff * 2, 30)

    def _dispatch_once(self) -> None:
        """Dispatcher#run iteration: vocabulary/rules over HTTP; captures over Kafka
        when available (unless the token has the basic profile), otherwise HTTP.
        Unsent records are requeued at the front of the buffer."""
        with self._dispatch_lock:
            records = self._buffer.drain(self.config.buffer_batch_size)
            captures: List[_Record] = []
            for i, rec in enumerate(records):
                try:
                    if rec.type == VOCABULARY:
                        self._with_auth(lambda tok, m=rec.payload: self._acp.post_vocabulary(m, tok))
                    elif rec.type == RULE:
                        self._with_auth(lambda tok, r=rec.payload: self._acp.post_rule(r, tok))
                    else:
                        captures.append(rec)
                except Exception:
                    self._buffer.requeue(captures + records[i:])
                    raise
            if not captures:
                return
            try:
                payloads = [r.payload for r in captures]
                if self._kafka is None or has_basic_profile(self._tokens.access):
                    self._with_auth(lambda tok: self._acp.post_event_captures(payloads, tok))
                else:
                    self._produce(payloads)
            except Exception:
                self._buffer.requeue(captures)
                raise

    def _produce(self, captures: List[Dict[str, Any]]) -> None:
        """Kafka path: one record per subject keyed tenant:event:subjectId on the
        subject's Java-hash partition, or one tenant:event record when there are
        no subjects. Authorization header carries the Kafka header token."""
        token = self._tokens.kafka_header() or ""
        tenant = tenant_id_from_token(token)
        headers = {"Authorization": token if token.startswith("Bearer ") else "Bearer " + token} if token else {}
        records = []
        for capture in captures:
            name = capture["event"]["name"]
            try:
                validate_credential_free(capture.get("payload"))
            except CredentialsInPayloadError:
                log.error("notify: dropping %s capture with credential-like payload fields", name)
                continue
            value = dumps(capture).encode()
            subjects = (capture.get("subjectResult") or {}).get("subjects") or []
            if not subjects:
                records.append(KafkaRecord(EVENTS_TOPIC, java_partition(tenant, self._partitions),
                                           f"{tenant}:{name}", value, headers))
            for s in subjects:
                sid = s.subject_id if isinstance(s, Subject) else str(s.get("address", ""))
                records.append(KafkaRecord(EVENTS_TOPIC, java_partition(sid, self._partitions),
                                           f"{tenant}:{name}:{sid}", value, headers))
        if records:
            self._kafka.produce(records)  # type: ignore[union-attr]

    def _with_auth(self, op: Callable[[Optional[str]], None]) -> None:
        """Refresh first if expired, and once more on HTTP 401."""
        if self._tokens.expired():
            self._refresh()
        try:
            op(self._tokens.access)
        except NotifyHTTPError as e:
            if e.status != 401:
                raise
            self._refresh()
            op(self._tokens.access)

    def _refresh(self) -> None:
        if not self._tokens.refresh:
            return
        try:
            resp = self._acp.refresh_token({"clientId": self._client_id, "refreshToken": self._tokens.refresh})
            self._tokens.set(resp.get("token"), resp.get("expiresInMs") or 0)
            log.info("notify: access token refreshed")
        except Exception as e:  # noqa: BLE001
            log.error("notify: token refresh failed: %s", e)


async def _await(value: Any) -> Any:
    return await value


def _rule_record(hook: _Hook) -> _Record:
    return _Record(RULE, {"eventName": hook.event, "ruleName": hook.name, "ruleDescription": hook.description})


def _register_capture(meta: _EventMeta) -> Dict[str, Any]:
    """REGISTER capture announcing an event's configuration; payload matches the
    Java SDK's flattened EventMetadata."""
    event = {"name": meta.key, "description": meta.description, "priority": meta.priority,
             "scheduleIntent": meta.schedule_intent, "preferredTimeWindow": meta.preferred_time_window,
             "eventType": "REGISTER"}
    now = iso_instant(_dt.datetime.now(_dt.timezone.utc))
    return {
        "timestamp": now, "occuredAt": now, "event": event, "durationMillis": 0, "serviceName": meta.service_name,
        "payload": {"EventMetadata": {
            "event": event, "version": meta.version, "methodName": meta.method_name,
            "declaringClassName": meta.declaring_class_name, "returnTypeName": meta.return_type_name,
            "parameterTypeNames": meta.parameter_type_names,
        }},
    }


def _validate_subjects(key: str, subjects: List[Any]) -> None:
    for i, s in enumerate(subjects):
        if not isinstance(s, Subject):
            raise TypeError(f"SubjectSupplier for event '{key}' returned non-Subject item at index {i}: "
                            f"{type(s).__name__}")
        if not s.address or not str(s.address).strip():
            raise ValueError(f"SubjectSupplier for event '{key}' returned {s.channel.value} subject at index {i} "
                             "without a destination address")


def _caller_frames(limit: int = 5) -> List[Dict[str, Any]]:
    frames = []
    f = sys._getframe(1)
    while f is not None and len(frames) < limit:
        filename = f.f_code.co_filename
        if not filename.startswith(_PACKAGE_DIR) and "functools" not in filename:
            qual = getattr(f.f_code, "co_qualname", f.f_code.co_name)
            parts = [p for p in qual.split(".") if p != "<locals>"]
            module = f.f_globals.get("__name__", "")
            cls = parts[-2] if len(parts) > 1 else ""
            frames.append({"className": f"{module}.{cls}" if cls else module, "methodName": f.f_code.co_name,
                           "lineNumber": f.f_lineno, "fileName": filename})
        f = f.f_back
    return frames
