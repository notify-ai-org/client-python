"""Notify.ai client SDK for Python — counterpart of the Java and Go SDKs.

    client = NotifyClient(Config(acp_server_url=..., client_token=...))

    @client.event("ORDER_PLACED", priority=2, schedule_intent="immediate")
    def place_order(payload: OrderPayload) -> OrderPayload: ...

    @client.subject_supplier("ORDER_PLACED")
    def subjects(payload: OrderPayload) -> list[Subject]: ...

    client.start()
"""

from .acp import NotifyHTTPError
from .client import AFTER, BEFORE, Metrics, NotifyClient, current_schedule
from .config import Config
from .policy import CredentialsInPayloadError, validate_credential_free
from .subjects import (Channel, EmailSubject, InAppSubject, PushSubject, SmsSubject, Subject, WebhookSubject,
                       WhatsAppSubject)
from .transport import (EVENTS_TOPIC, SCHEDULED_EVENTS_TOPIC, KafkaCredentials, KafkaFactory, KafkaRecord,
                        KafkaTransport)
from .vocabulary import ModelRegistry, Vocabulary, default_registry, model

__version__ = "1.0.0"

__all__ = [
    "AFTER", "BEFORE", "Channel", "Config", "CredentialsInPayloadError", "EVENTS_TOPIC", "EmailSubject",
    "InAppSubject", "KafkaCredentials", "KafkaFactory", "KafkaRecord", "KafkaTransport", "Metrics",
    "ModelRegistry", "NotifyClient", "NotifyHTTPError", "PushSubject", "SCHEDULED_EVENTS_TOPIC", "SmsSubject",
    "Subject", "Vocabulary", "WebhookSubject", "WhatsAppSubject", "current_schedule", "default_registry", "model",
    "validate_credential_free",
]
