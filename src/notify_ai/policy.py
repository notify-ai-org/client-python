"""Port of the Java CredentialPayloadPolicy: payloads must not carry credentials."""

from __future__ import annotations

import json
import re
from typing import Any

from .util import to_jsonable

_KEYS = ("credentials", "secret", "signingsecret", "callbacksecret", "password", "apikey", "apisecret",
         "authtoken", "accesstoken", "refreshtoken", "authorization", "privatekey", "serviceaccountjson",
         "secretstring", "secretbinary", "awssecretarn")
_NON_ALNUM = re.compile(r"[^a-z0-9]")


class CredentialsInPayloadError(ValueError):
    def __init__(self) -> None:
        super().__init__("Credentials are not allowed in notification payloads")


def validate_credential_free(data: Any) -> None:
    """Raise CredentialsInPayloadError if keys look like credentials (password, apiKey,
    *_secret, ...) or strings contain private keys. Message prose is not inspected."""
    if not _ok(to_jsonable(data), 0):
        raise CredentialsInPayloadError()


def _ok(node: Any, depth: int) -> bool:
    if depth > 50:
        return False
    if isinstance(node, dict):
        for k, v in node.items():
            compact = _NON_ALNUM.sub("", str(k).lower())
            if any(compact == key or compact.endswith(key) for key in _KEYS):
                return False
            if not _ok(v, depth + 1):
                return False
    elif isinstance(node, list):
        return all(_ok(v, depth + 1) for v in node)
    elif isinstance(node, str):
        text = node.strip()
        if "-----BEGIN PRIVATE KEY-----" in text or "-----BEGIN RSA PRIVATE KEY-----" in text:
            return False
        if text.startswith(("{", "[")):
            try:
                nested = json.loads(text)
            except ValueError:
                return True
            return _ok(nested, depth + 1)
    return True
