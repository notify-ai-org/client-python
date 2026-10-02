from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    """Mirrors the Java NotifyProperties (prefix ``notify.ai.properties``)."""

    acp_server_url: str = "https://app.notify-ai.dev"
    application_name: str = "notify-client"
    #: Identifies this client to acp-server (sent as clientId at registration).
    client_token: str = ""
    #: Reported at registration; for Python use the top-level package name.
    base_package: str = ""
    #: Maximum records sent per dispatch.
    buffer_batch_size: int = 100
    #: Seconds between dispatcher drains.
    flush_interval: float = 0.1
    #: Cap on buffered records; the oldest capture is dropped when full. <0 = unbounded.
    max_buffer_size: int = 10_000
    #: Deliver captures over Kafka and consume scheduled events (requires a kafka factory).
    kafka_enabled: bool = False
    #: Flush buffered captures at interpreter exit.
    flush_on_exit: bool = True

    def __post_init__(self) -> None:
        self.acp_server_url = (self.acp_server_url or "https://app.notify-ai.dev").rstrip("/")
        self.application_name = self.application_name or "notify-client"
        if self.buffer_batch_size <= 0:
            self.buffer_batch_size = 100
        if self.flush_interval <= 0:
            self.flush_interval = 0.1
        if self.max_buffer_size == 0:
            self.max_buffer_size = 10_000

    @classmethod
    def from_env(cls) -> "Config":
        """NOTIFY_AI_ACP_SERVER_URL, NOTIFY_AI_APPLICATION_NAME, NOTIFY_AI_CLIENT_TOKEN,
        NOTIFY_AI_BASE_PACKAGE, NOTIFY_AI_BUFFER_BATCH_SIZE, NOTIFY_AI_FLUSH_INTERVAL_MS,
        NOTIFY_AI_MAX_BUFFER_SIZE, NOTIFY_AI_KAFKA_ENABLED."""
        env = os.environ.get

        def num(name: str) -> int:
            try:
                return int(env(name, "0"))
            except ValueError:
                return 0

        return cls(
            acp_server_url=env("NOTIFY_AI_ACP_SERVER_URL", ""),
            application_name=env("NOTIFY_AI_APPLICATION_NAME", ""),
            client_token=env("NOTIFY_AI_CLIENT_TOKEN", ""),
            base_package=env("NOTIFY_AI_BASE_PACKAGE", ""),
            buffer_batch_size=num("NOTIFY_AI_BUFFER_BATCH_SIZE"),
            flush_interval=num("NOTIFY_AI_FLUSH_INTERVAL_MS") / 1000,
            max_buffer_size=num("NOTIFY_AI_MAX_BUFFER_SIZE"),
            kafka_enabled=env("NOTIFY_AI_KAFKA_ENABLED", "").lower() in ("1", "true", "yes"),
        )
