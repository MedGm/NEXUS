import random
from faker import Faker
from .events import OrderItem, OrderPlaced, PaymentProcessed, SessionStarted, RecommendationServed

_EVENT_WEIGHTS = {
    "SessionStarted": 0.40,
    "OrderPlaced": 0.30,
    "PaymentProcessed": 0.20,
    "RecommendationServed": 0.10,
}


class EventSimulator:
    def __init__(self, fake: Faker | None = None):
        self._fake = fake or Faker()
        self._types = list(_EVENT_WEIGHTS.keys())
        self._weights = [_EVENT_WEIGHTS[k] for k in self._types]

    def next(self):
        choice = random.choices(self._types, weights=self._weights, k=1)[0]
        return getattr(self, f"_gen_{choice.lower()}")()

    def _gen_sessionstarted(self) -> SessionStarted:
        return SessionStarted(
            session_id=str(self._fake.uuid4()),
            user_id=str(self._fake.uuid4()),
            device=random.choice(["mobile", "desktop", "tablet"]),
            os=random.choice(["iOS", "Android", "Windows", "macOS", "Linux"]),
            browser=random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
            ip=self._fake.ipv4(),
            referrer=self._fake.uri(),
        )

    def _gen_orderplaced(self) -> OrderPlaced:
        items = [
            OrderItem(
                sku=self._fake.bothify("PROD-####"),
                name=self._fake.catch_phrase(),
                quantity=random.randint(1, 5),
                price_usd=round(random.uniform(5.0, 500.0), 2),
            )
            for _ in range(random.randint(1, 4))
        ]
        return OrderPlaced(
            order_id=str(self._fake.uuid4()),
            user_id=str(self._fake.uuid4()),
            items=items,
            total_usd=round(sum(i.price_usd * i.quantity for i in items), 2),
            currency=random.choice(["USD", "EUR", "GBP"]),
            country=self._fake.country_code(),
        )

    def _gen_paymentprocessed(self) -> PaymentProcessed:
        return PaymentProcessed(
            payment_id=str(self._fake.uuid4()),
            order_id=str(self._fake.uuid4()),
            method=random.choice(["CARD", "PAYPAL", "CRYPTO", "BANK_TRANSFER"]),
            status=random.choices(
                ["APPROVED", "DECLINED", "PENDING"], weights=[0.85, 0.10, 0.05]
            )[0],
            amount_usd=round(random.uniform(5.0, 1000.0), 2),
            gateway=random.choice(["stripe", "paypal", "adyen", "square"]),
        )

    def _gen_recommendationserved(self) -> RecommendationServed:
        return RecommendationServed(
            rec_id=str(self._fake.uuid4()),
            user_id=str(self._fake.uuid4()),
            model_version=f"v{random.randint(1,5)}.{random.randint(0,9)}.0",
            items=[self._fake.bothify("PROD-####") for _ in range(random.randint(3, 10))],
            score=round(random.uniform(0.5, 1.0), 4),
            context=random.choice(["homepage", "product_page", "cart", "email"]),
        )
