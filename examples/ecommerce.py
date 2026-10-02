"""Python port of examples/ecommerce-app's order flow, exercising every SDK
hook type against a running acp-server.

    NOTIFY_AI_ACP_SERVER_URL=http://localhost:8080 NOTIFY_AI_CLIENT_TOKEN=client-... \
        python examples/ecommerce.py

    curl -X POST localhost:8092/api/orders/place \
      -d '{"orderId":"ORD-1","customerId":"CUST-1","amount":120,"items":["Mechanical Keyboard"]}'
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Annotated, Dict, List

from notify_ai import AFTER, BEFORE, Config, EmailSubject, NotifyClient, SmsSubject, Subject, Vocabulary, model

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ecommerce")

# ── Models (@Model / @Vocabulary) ─────────────────────────────────────────────


@model("Payload for order placement and payment events")
@dataclass
class OrderPayload:
    order_id: Annotated[str, Vocabulary("orderId", "Unique order identifier")]
    customer_id: Annotated[str, Vocabulary("customerId", "Customer who placed the order")]
    amount: Annotated[float, Vocabulary("amount", "Total order amount in USD")] = 0.0
    items: Annotated[List[str], Vocabulary("items", "List of item names in the order")] = field(default_factory=list)
    shipping_address: Annotated[str, Vocabulary("shippingAddress", "Delivery address for the order")] = ""


@model("Payload for shipment tracking events")
@dataclass
class ShipmentPayload:
    order_id: Annotated[str, Vocabulary("orderId", "Order being shipped")]
    tracking_number: Annotated[str, Vocabulary("trackingNumber", "Carrier tracking number")]
    carrier: Annotated[str, Vocabulary("carrier", "Shipping carrier name")]
    estimated_delivery: Annotated[str, Vocabulary("estimatedDelivery", "Estimated delivery date (YYYY-MM-DD)")] = ""


@model("Payload for abandoned cart events")
@dataclass
class CartPayload:
    cart_id: Annotated[str, Vocabulary("cartId", "Unique cart identifier")]
    customer_id: Annotated[str, Vocabulary("customerId", "Customer who owns the cart")]
    items: Annotated[List[str], Vocabulary("items", "Items left in the cart")] = field(default_factory=list)
    last_activity_at: Annotated[str, Vocabulary("lastActivityAt", "Timestamp of last cart activity (ISO-8601)")] = ""


@dataclass
class Customer:
    id: str
    name: str
    email: str
    phone: str


CUSTOMERS = {c.id: c for c in [
    Customer("CUST-1", "Alice Johnson", "alice@example.com", "+1-555-0101"),
    Customer("CUST-2", "Bob Smith", "bob@example.com", "+1-555-0102"),
    Customer("CUST-3", "Carol Davis", "carol@example.com", "+1-555-0103"),
]}
ORDERS: Dict[str, OrderPayload] = {}

cfg = Config.from_env()
cfg.application_name = cfg.application_name if cfg.application_name != "notify-client" else "ecommerce-app-py"
if cfg.acp_server_url == "https://app.notify-ai.dev" and not os.environ.get("NOTIFY_AI_ACP_SERVER_URL"):
    cfg.acp_server_url = "http://localhost:8080"
cfg.base_package = "examples.ecommerce"
client = NotifyClient(cfg)

# ── Service with events (@Event) ──────────────────────────────────────────────


class OrderService:
    @client.event("ORDER_PLACED", description="Customer placed an order", event_type="static",
                  schedule_intent="immediate", preferred_time_window="09:00-18:00", priority=2)
    def place_order(self, payload: OrderPayload) -> OrderPayload:
        ORDERS[payload.order_id] = payload
        log.info("📦 Order placed: %s for customer %s — $%.2f", payload.order_id, payload.customer_id, payload.amount)
        return payload

    @client.event("PAYMENT_FAILED", description="Payment processing failed for an order", event_type="static",
                  schedule_intent="immediate", preferred_time_window="00:00-23:59", priority=4)
    def report_payment_failed(self, payload: OrderPayload) -> OrderPayload:
        log.warning("💳 Payment FAILED for order: %s — $%.2f", payload.order_id, payload.amount)
        return payload

    @client.event("ORDER_SHIPPED", description="Order has been shipped to the customer", event_type="static",
                  schedule_intent="immediate", preferred_time_window="09:00-18:00", priority=5)
    def ship_order(self, payload: ShipmentPayload) -> ShipmentPayload:
        log.info("Order shipped: %s via %s — tracking: %s", payload.order_id, payload.carrier, payload.tracking_number)
        return payload

    @client.event("ABANDONED_CART", description="Customer abandoned their shopping cart", event_type="deferred",
                  schedule_intent="deferred", preferred_time_window="09:00-21:00", priority=3)
    def abandon_cart(self, payload: CartPayload) -> CartPayload:
        log.info("🛒 Cart abandoned: %s by customer %s", payload.cart_id, payload.customer_id)
        return payload


# ── Subject suppliers (@SubjectSupplier) ──────────────────────────────────────


def email_to(customer_id: str) -> List[Subject]:
    c = CUSTOMERS.get(customer_id)
    return [EmailSubject(c.email, attributes={"firstName": c.name})] if c else []


@client.subject_supplier("ORDER_PLACED", "Resolves order customer to email recipients")
def order_placed_subjects(p: OrderPayload) -> List[Subject]:
    return email_to(p.customer_id)


@client.subject_supplier("PAYMENT_FAILED", "Resolves customer to SMS for urgent payment alerts")
def payment_failed_subjects(p: OrderPayload) -> List[Subject]:
    c = CUSTOMERS.get(p.customer_id)
    return [SmsSubject(c.phone, attributes={"firstName": c.name})] if c else []


@client.subject_supplier("ORDER_SHIPPED", "Resolves order to email recipients for shipment tracking")
def shipment_subjects(p: ShipmentPayload) -> List[Subject]:
    order = ORDERS.get(p.order_id)
    return email_to(order.customer_id) if order else []


@client.subject_supplier("ABANDONED_CART", "Resolves cart owner to email for re-engagement")
def cart_subjects(p: CartPayload) -> List[Subject]:
    return email_to(p.customer_id)


# ── Rules (@Rule) and callbacks (@Callback) ───────────────────────────────────


@client.rule("fraud-check", event="ORDER_PLACED", description="Blocks orders over $1000 as potential fraud")
def fraud_check(p: OrderPayload) -> bool:
    return p.amount < 1000


@client.rule("inventory-check", event="ORDER_PLACED", description="Checks whether all items are in stock")
def inventory_check(p: OrderPayload) -> bool:
    return True


@client.callback("ORDER_PLACED", BEFORE)
def before_order_placed(p: OrderPayload) -> None:
    log.info("⏳ [BEFORE] About to process ORDER_PLACED for order: %s", p.order_id)


@client.callback("ORDER_PLACED", AFTER)
def after_order_placed(p: OrderPayload) -> None:
    log.info("✅ [AFTER] ORDER_PLACED processing complete for order: %s", p.order_id)


# ── HTTP endpoints ────────────────────────────────────────────────────────────

service = OrderService()


def _snake(d: dict) -> dict:
    import re
    return {re.sub(r"(?<!^)(?=[A-Z])", "_", k).lower(): v for k, v in d.items()}


ROUTES = {
    "/api/orders/place": lambda b: (service.place_order(OrderPayload(**_snake(b))),
                                    {"status": "ORDER_PLACED", "orderId": b["orderId"], "amount": b.get("amount")}),
    "/api/orders/payment-failed": lambda b: (service.report_payment_failed(OrderPayload(**_snake(b))),
                                             {"status": "PAYMENT_FAILED", "orderId": b["orderId"]}),
    "/api/orders/ship": lambda b: (service.ship_order(ShipmentPayload(**_snake(b))),
                                   {"status": "ORDER_SHIPPED", "orderId": b["orderId"],
                                    "trackingNumber": b["trackingNumber"]}),
    "/api/orders/abandon-cart": lambda b: (service.abandon_cart(CartPayload(**_snake(b))),
                                           {"status": "ABANDONED_CART", "cartId": b["cartId"]}),
}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        route = ROUTES.get(self.path)
        if route is None:
            self.send_error(404)
            return
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            _, reply = route(body)
            status = 200
        except (TypeError, KeyError, ValueError) as e:
            reply, status = {"error": str(e)}, 400
        raw = json.dumps(reply).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


if __name__ == "__main__":
    client.start()
    port = int(os.environ.get("PORT", "8092"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    log.info("ecommerce example listening on :%d", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        client.close()
