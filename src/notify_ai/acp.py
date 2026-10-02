"""HTTP client for acp-server (AcpServerClient)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from .util import dumps


class NotifyHTTPError(Exception):
    def __init__(self, path: str, status: int, body: str) -> None:
        super().__init__(f"POST {path} failed: {status} {body}")
        self.path, self.status, self.body = path, status, body


class AcpClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def _post(self, path: str, body: Any, bearer: Optional[str], timeout: float) -> Any:
        req = urllib.request.Request(self.base_url + path, data=dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
        if bearer:
            req.add_header("Authorization", "Bearer " + bearer)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            # 409 Conflict means the server's idempotency filter saw a duplicate.
            if e.code == 409:
                return None
            raise NotifyHTTPError(path, e.code, e.read().decode(errors="replace")[:2000]) from None
        return json.loads(raw) if raw.strip() else None

    def register(self, request: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("/client/register", request, None, 15) or {}

    def refresh_token(self, request: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("/auth/token/refresh", request, None, 10) or {}

    def post_vocabulary(self, models: List[Dict[str, Any]], bearer: Optional[str]) -> None:
        if models:
            self._post("/api/vocabulary", models, bearer, 30)

    def post_rule(self, rule: Dict[str, Any], bearer: Optional[str]) -> None:
        self._post("/api/vocabulary/rules/process", rule, bearer, 30)

    def post_event_captures(self, captures: List[Dict[str, Any]], bearer: Optional[str]) -> None:
        if captures:
            self._post("/api/event", captures, bearer, 30)
