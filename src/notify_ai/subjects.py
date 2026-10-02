"""Notification recipients. JSON matches the Java SDK's Subject hierarchy
(discriminated by ``channel``)."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Mapping, Optional

from .util import new_uuid


class Channel(str, Enum):
    SMS = "SMS"
    EMAIL = "EMAIL"
    WHATSAPP = "WHATSAPP"
    PUSH = "PUSH"
    NONE = "NONE"
    WEBHOOK = "WEBHOOK"
    IN_APP = "IN_APP"


class Subject:
    """Base class. ``subject_id`` is the destination address."""

    channel: Channel

    def __init__(self, correlation_id: Optional[str] = None,
                 attributes: Optional[Mapping[str, str]] = None) -> None:
        self.correlation_id = correlation_id or new_uuid()
        self.attributes: Optional[Dict[str, str]] = dict(attributes) if attributes is not None else None

    @property
    def address(self) -> str:
        raise NotImplementedError

    @property
    def subject_id(self) -> str:
        return self.address

    @property
    def address_fingerprint(self) -> str:
        return f"{self.channel.value.lower()}:{self.address}"

    def _fields(self) -> Dict[str, Any]:
        return {}

    def to_json(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "channel": self.channel.value,
            "correlationId": self.correlation_id,
            "attributes": self.attributes,
        }
        out.update(self._fields())
        return out

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.address!r})"


class EmailSubject(Subject):
    channel = Channel.EMAIL

    def __init__(self, email: str, cc: Optional[str] = None, bcc: Optional[str] = None,
                 correlation_id: Optional[str] = None, attributes: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(correlation_id, attributes)
        self.email, self.cc, self.bcc = email, cc, bcc

    @property
    def address(self) -> str:
        return self.email

    @property
    def address_fingerprint(self) -> str:
        return "email:" + (self.email or "").lower()

    def _fields(self) -> Dict[str, Any]:
        return {"email": self.email, "cc": self.cc, "bcc": self.bcc}


class SmsSubject(Subject):
    channel = Channel.SMS

    def __init__(self, phone_number: str, correlation_id: Optional[str] = None,
                 attributes: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(correlation_id, attributes)
        self.phone_number = phone_number

    @property
    def address(self) -> str:
        return self.phone_number

    def _fields(self) -> Dict[str, Any]:
        return {"phoneNumber": self.phone_number}


class WhatsAppSubject(Subject):
    channel = Channel.WHATSAPP

    def __init__(self, phone_number: str, correlation_id: Optional[str] = None,
                 attributes: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(correlation_id, attributes)
        self.phone_number = (phone_number or "").strip()

    @property
    def address(self) -> str:
        return self.phone_number

    def _fields(self) -> Dict[str, Any]:
        return {"phoneNumber": self.phone_number}


class PushSubject(Subject):
    channel = Channel.PUSH

    def __init__(self, device_token: str, correlation_id: Optional[str] = None,
                 attributes: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(correlation_id, attributes)
        self.device_token = device_token

    @property
    def address(self) -> str:
        return self.device_token

    def _fields(self) -> Dict[str, Any]:
        return {"deviceToken": self.device_token}


class WebhookSubject(Subject):
    channel = Channel.WEBHOOK

    def __init__(self, url: str, correlation_id: Optional[str] = None,
                 attributes: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(correlation_id, attributes)
        self.url = url

    @property
    def address(self) -> str:
        return self.url

    def _fields(self) -> Dict[str, Any]:
        return {"url": self.url}


class InAppSubject(Subject):
    channel = Channel.IN_APP

    def __init__(self, url: str, user_id: Optional[str] = None, correlation_id: Optional[str] = None,
                 attributes: Optional[Mapping[str, str]] = None) -> None:
        super().__init__(correlation_id, attributes)
        self.url, self.user_id = (url or "").strip(), user_id

    @property
    def address(self) -> str:
        return self.url

    @property
    def address_fingerprint(self) -> str:
        return f"in_app:{self.url}:{self.user_id or ''}"

    def _fields(self) -> Dict[str, Any]:
        return {"url": self.url, "userId": self.user_id}
