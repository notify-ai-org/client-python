"""Models and vocabulary — the Python counterpart of @Model / @Vocabulary.

    @model("Payload for order placement events")
    @dataclass
    class OrderPayload:
        order_id: Annotated[str, Vocabulary("orderId", "Unique order identifier")]
        amount: Annotated[float, Vocabulary("amount", "Total order amount in USD")]

When at least one field carries ``Vocabulary`` only those fields are vocabulary;
otherwise every field is, named after the attribute. Dataclasses, pydantic
models and plain annotated classes are supported. Mapping payloads (dicts) are
flattened by their own keys.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import decimal
import threading
import typing
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type

__all__ = ["Vocabulary", "model", "ModelRegistry", "default_registry"]


@dataclass(frozen=True)
class Vocabulary:
    """Field metadata, used inside ``typing.Annotated``."""

    name: str = ""
    description: str = ""


@dataclass
class _Field:
    attr: str
    name: str
    description: str
    type_name: str


@dataclass
class _ModelMeta:
    cls: type
    description: str
    fields: List[_Field]


def _unwrap(tp: Any) -> Tuple[Any, Optional[Vocabulary]]:
    vocab = None
    if typing.get_origin(tp) is typing.Annotated:
        args = typing.get_args(tp)
        tp = args[0]
        vocab = next((a for a in args[1:] if isinstance(a, Vocabulary)), None)
    origin = typing.get_origin(tp)
    if origin is typing.Union or str(origin) == "types.UnionType":
        non_none = [a for a in typing.get_args(tp) if a is not type(None)]
        if len(non_none) == 1:
            tp = non_none[0]
    return tp, vocab


def java_type_name(tp: Any) -> str:
    """Java simple names, so vocabulary looks the same from every SDK."""
    tp, _ = _unwrap(tp)
    origin = typing.get_origin(tp) or tp
    mapping = {str: "String", bool: "boolean", int: "int", float: "double", bytes: "byte[]",
               decimal.Decimal: "BigDecimal", _dt.datetime: "Instant", _dt.date: "LocalDate",
               list: "List", tuple: "List", set: "Set", frozenset: "Set", dict: "Map"}
    if origin in mapping:
        return mapping[origin]
    if tp is Any:
        return "Object"
    return getattr(tp, "__name__", str(tp))


def _hints(cls: type) -> Dict[str, Any]:
    try:
        return typing.get_type_hints(cls, include_extras=True)
    except Exception:
        return dict(getattr(cls, "__annotations__", {}))


def _fields_of(cls: type) -> List[_Field]:
    hints = _hints(cls)
    if dataclasses.is_dataclass(cls):
        names = [f.name for f in dataclasses.fields(cls)]
    elif hasattr(cls, "model_fields"):  # pydantic v2
        names = list(cls.model_fields)
    else:
        names = [n for n in hints if not n.startswith("_")]
    entries = []
    for attr in names:
        tp, vocab = _unwrap(hints.get(attr, Any))
        entries.append((attr, tp, vocab))
    tagged = any(v is not None for _, _, v in entries)
    out = []
    for attr, tp, vocab in entries:
        if tagged and vocab is None:
            continue
        out.append(_Field(attr=attr, name=(vocab.name if vocab and vocab.name else attr),
                          description=vocab.description if vocab else "", type_name=java_type_name(tp)))
    return out


class ModelRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._models: Dict[type, _ModelMeta] = {}
        self._listeners: List[Any] = []

    def register(self, cls: type, description: str = "") -> bool:
        """Register ``cls``; returns True when newly added."""
        if not isinstance(cls, type) or cls is Any or cls.__module__ in ("builtins", "typing", "collections.abc"):
            return False
        with self._lock:
            meta = self._models.get(cls)
            if meta:
                if description and not meta.description:
                    meta.description = description
                return False
            meta = _ModelMeta(cls, description or "", _fields_of(cls))
            self._models[cls] = meta
            listeners = list(self._listeners)
        for listener in listeners:
            listener(meta)
        return True

    def get(self, cls: type) -> Optional[_ModelMeta]:
        with self._lock:
            return self._models.get(cls)

    def all(self) -> List[_ModelMeta]:
        with self._lock:
            return list(self._models.values())

    def subscribe(self, listener: Any) -> None:
        with self._lock:
            self._listeners.append(listener)

    def unsubscribe(self, listener: Any) -> None:
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    # -- flattening (VocabularyManager#toFlattenedMap) -----------------------

    def flatten(self, instance: Any) -> Dict[str, Any]:
        """A model instance becomes ``{vocabName: value}``; nested models are expanded
        into their own leaves; a mapping is flattened by its keys; anything else
        becomes ``{TypeName: value}``."""
        if instance is None:
            return {}
        if isinstance(instance, Mapping):
            return {str(k): v for k, v in instance.items()}
        out: Dict[str, Any] = {}
        if not self._expand(instance, out, set()):
            out[type(instance).__name__] = instance
        return out

    def _expand(self, instance: Any, out: Dict[str, Any], visited: set) -> bool:
        meta = self.get(type(instance))
        if meta is None or not meta.fields or id(instance) in visited:
            return False
        visited.add(id(instance))
        for f in meta.fields:
            value = getattr(instance, f.attr, None)
            if value is None or not self._expand(value, out, visited):
                out[f.name] = value
        return True

    def class_model(self, meta: _ModelMeta) -> Dict[str, Any]:
        """ClassModel DTO (VocabularyManager#toClassModelDtoList)."""
        bases = [b for b in meta.cls.__bases__ if b is not object]
        return {
            "packageName": meta.cls.__module__,
            "className": meta.cls.__name__,
            "classDescription": meta.description,
            "classType": None,
            "superClass": f"{bases[0].__module__}.{bases[0].__qualname__}" if bases else None,
            "interfaces": [],
            "attributes": [{"name": f.name, "type": f.type_name, "description": f.description, "defaultValue": None}
                           for f in meta.fields],
            "methods": [],
        }

    def class_models(self) -> List[Dict[str, Any]]:
        return [self.class_model(m) for m in self.all()]


default_registry = ModelRegistry()


def model(description: str = "") -> Any:
    """Class decorator mirroring @Model. Registers the class in the default registry."""

    def decorate(cls: Type[Any]) -> Type[Any]:
        default_registry.register(cls, description)
        return cls

    return decorate
