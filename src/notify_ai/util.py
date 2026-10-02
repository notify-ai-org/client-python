"""Small helpers shared across the SDK."""

from __future__ import annotations

import base64
import dataclasses
import datetime as _dt
import decimal
import enum
import json
import uuid
from typing import Any, Mapping, Optional


def new_uuid() -> str:
    return str(uuid.uuid4())


def java_string_hash_code(s: str) -> int:
    """Reproduce ``java.lang.String#hashCode`` (over UTF-16 code units, int32 overflow)
    so Kafka partition choice matches the Java SDK."""
    h = 0
    data = s.encode("utf-16-le")
    for i in range(0, len(data), 2):
        unit = data[i] | (data[i + 1] << 8)
        h = (31 * h + unit) & 0xFFFFFFFF
    return h - (1 << 32) if h & 0x80000000 else h


def java_partition(s: str, partitions: int) -> int:
    """``Math.abs(s.hashCode()) % partitions``, without Java's negative result for
    ``Integer.MIN_VALUE``."""
    if partitions <= 0:
        return 0
    return abs(java_string_hash_code(s)) % partitions


def jwt_claims(token: Optional[str]) -> Mapping[str, Any]:
    """Decode a JWT payload without verifying it. The SDK never holds the server's
    signing key; claims are read only to pick a transport and a partition key."""
    if not token:
        return {}
    token = token.strip()
    if token.startswith("Bearer "):
        token = token[7:].strip()
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        return claims if isinstance(claims, dict) else {}
    except Exception:
        return {}


def tenant_id_from_token(token: Optional[str]) -> str:
    value = jwt_claims(token).get("tenantId")
    return str(value) if value is not None else "unknown-tenant"


def has_basic_profile(token: Optional[str]) -> bool:
    return jwt_claims(token).get("profile") == "basic"


def iso_instant(ts: _dt.datetime) -> str:
    """Format like java.time.Instant: UTC ISO-8601 with a trailing Z."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=_dt.timezone.utc)
    return ts.astimezone(_dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_instant(value: Any) -> Optional[_dt.datetime]:
    """Accept the forms Jackson emits for Instant: ISO strings or epoch seconds."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _dt.datetime.fromtimestamp(value, tz=_dt.timezone.utc)
    text = str(value).replace("Z", "+00:00")
    return _dt.datetime.fromisoformat(text)


def to_jsonable(value: Any) -> Any:
    """Convert arbitrary Python values into JSON-compatible structures."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if hasattr(value, "to_json") and callable(value.to_json):
        return value.to_json()
    if isinstance(value, _dt.datetime):
        return iso_instant(value)
    if isinstance(value, (_dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, enum.Enum):
        return to_jsonable(value.value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: to_jsonable(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_jsonable(v) for v in value]
    if hasattr(value, "model_dump"):  # pydantic v2
        return to_jsonable(value.model_dump())
    if hasattr(value, "__dict__"):
        return {k: to_jsonable(v) for k, v in vars(value).items() if not k.startswith("_")}
    return str(value)


def dumps(value: Any) -> str:
    return json.dumps(to_jsonable(value), separators=(",", ":"), ensure_ascii=False)
