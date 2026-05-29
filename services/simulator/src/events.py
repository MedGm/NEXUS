import time
import uuid
from dataclasses import dataclass, field
from typing import List


@dataclass
class OrderItem:
    sku: str
    name: str
    quantity: int
    price_usd: float


@dataclass
class OrderPlaced:
    order_id: str
    user_id: str
    items: List[OrderItem]
    total_usd: float
    currency: str
    country: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "items": [
                {"sku": i.sku, "name": i.name, "quantity": i.quantity, "price_usd": i.price_usd}
                for i in self.items
            ],
            "total_usd": self.total_usd,
            "currency": self.currency,
            "country": self.country,
        }


@dataclass
class PaymentProcessed:
    payment_id: str
    order_id: str
    method: str
    status: str
    amount_usd: float
    gateway: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "payment_id": self.payment_id,
            "order_id": self.order_id,
            "method": self.method,
            "status": self.status,
            "amount_usd": self.amount_usd,
            "gateway": self.gateway,
        }


@dataclass
class SessionStarted:
    session_id: str
    user_id: str
    device: str
    os: str
    browser: str
    ip: str
    referrer: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "session_id": self.session_id,
            "user_id": self.user_id,
            "device": self.device,
            "os": self.os,
            "browser": self.browser,
            "ip": self.ip,
            "referrer": self.referrer,
        }


@dataclass
class RecommendationServed:
    rec_id: str
    user_id: str
    model_version: str
    items: List[str]
    score: float
    context: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "rec_id": self.rec_id,
            "user_id": self.user_id,
            "model_version": self.model_version,
            "items": self.items,
            "score": self.score,
            "context": self.context,
        }
