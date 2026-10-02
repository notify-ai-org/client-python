import dataclasses
import json
from typing import Annotated, List, Optional

import pytest

from notify_ai import (EmailSubject, InAppSubject, ModelRegistry, SmsSubject, Vocabulary,
                       validate_credential_free, CredentialsInPayloadError)
from notify_ai.util import (dumps, has_basic_profile, java_partition, java_string_hash_code, parse_instant,
                            tenant_id_from_token)
from conftest import fake_jwt

# Reference values produced by java.lang.String#hashCode on a real JVM.
JVM = [("", 0, 0), ("hello", 99162322, 10), ("CUST-1", 1999207223, 11), ("alice@example.com", 2145772861, 1),
       ("tenant-42", -1852830079, 7), ("héllo wörld", 1628148953, 5), ("📦 order", 1563790519, 7),
       ("polygenelubricants", -2147483648, 8)]  # Java partition would be -8 (invalid)


@pytest.mark.parametrize("s,hash_code,partition", JVM)
def test_java_hash_matches_jvm(s, hash_code, partition):
    assert java_string_hash_code(s) == hash_code
    assert java_partition(s, 12) == partition


@dataclasses.dataclass
class Address:
    city: Annotated[str, Vocabulary("city", "Delivery city")]
    zip: Annotated[str, Vocabulary("zip")]


@dataclasses.dataclass
class Order:
    order_id: Annotated[str, Vocabulary("orderId", "Unique order identifier")]
    amount: Annotated[float, Vocabulary("amount")]
    items: Annotated[List[str], Vocabulary("items")]
    shipping: Annotated[Optional[Address], Vocabulary("shipping")] = None
    internal: str = "x"  # untagged: excluded


@dataclasses.dataclass
class Untagged:
    customer_id: str
    count: int


def test_flatten_mirrors_java_vocabulary_manager():
    r = ModelRegistry()
    for cls in (Order, Address, Untagged):
        r.register(cls)
    assert r.flatten(Order("O-1", 12.5, ["a"], Address("Pune", "411001"))) == {
        "orderId": "O-1", "amount": 12.5, "items": ["a"], "city": "Pune", "zip": "411001"}
    assert r.flatten(Order("O-2", 1, []))["shipping"] is None
    assert r.flatten(Untagged("C", 2)) == {"customer_id": "C", "count": 2}
    assert r.flatten({"orderId": "D"}) == {"orderId": "D"}

    class NotAModel:
        pass

    flat = r.flatten(NotAModel())
    assert list(flat) == ["NotAModel"]


def test_class_models():
    r = ModelRegistry()
    r.register(Order, "Order payload")
    (m,) = r.class_models()
    assert m["className"] == "Order" and m["classDescription"] == "Order payload"
    assert {a["name"]: a["type"] for a in m["attributes"]} == {
        "orderId": "String", "amount": "double", "items": "List", "shipping": "Address"}


def test_credential_policy():
    for bad in ({"password": "x"}, {"user": {"api_key": "x"}}, {"stripeSecret": "x"},
                {"list": [{"Authorization": "y"}]}, {"note": "-----BEGIN PRIVATE KEY-----abc"},
                {"json": '{"refresh_token":"x"}'}):
        with pytest.raises(CredentialsInPayloadError):
            validate_credential_free(bad)
    validate_credential_free({"orderId": "1", "message": "your password was changed", "tokens": 3, "json": "{oops"})


def test_subject_json_matches_java_shape():
    attrs = {"firstName": "Alice"}
    assert EmailSubject("a@x.com", bcc="b@x.com", correlation_id="c1", attributes=attrs).to_json() == {
        "channel": "EMAIL", "correlationId": "c1", "attributes": attrs, "email": "a@x.com", "cc": None,
        "bcc": "b@x.com"}
    assert SmsSubject("+1555", "c2").to_json() == {"channel": "SMS", "correlationId": "c2", "attributes": None,
                                                    "phoneNumber": "+1555"}
    assert InAppSubject(" /inbox ", "u1", "c3").to_json()["url"] == "/inbox"
    assert SmsSubject("1").correlation_id
    assert EmailSubject("A@X.com").address_fingerprint == "email:a@x.com"
    assert json.loads(dumps([SmsSubject("1", "c")]))[0]["phoneNumber"] == "1"


def test_jwt_helpers_and_instants():
    tok = fake_jwt({"tenantId": "t-9", "profile": "basic"})
    assert tenant_id_from_token(tok) == "t-9" and has_basic_profile("Bearer " + tok)
    assert tenant_id_from_token("garbage") == "unknown-tenant" and not has_basic_profile(None)
    assert parse_instant(1767225600.5).microsecond == 500000
    assert parse_instant("2026-01-01T00:00:00Z").year == 2026
