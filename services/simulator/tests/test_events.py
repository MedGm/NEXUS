import time
from src.events import OrderItem, OrderPlaced, PaymentProcessed, SessionStarted, RecommendationServed


def test_order_placed_to_dict_has_all_fields():
    item = OrderItem(sku="PROD-1234", name="Widget", quantity=2, price_usd=19.99)
    event = OrderPlaced(
        order_id="ord-1", user_id="usr-1", items=[item],
        total_usd=39.98, currency="USD", country="US",
    )
    d = event.to_dict()
    assert d["order_id"] == "ord-1"
    assert d["items"][0]["sku"] == "PROD-1234"
    assert "event_id" in d
    assert "timestamp" in d
    assert d["timestamp"] <= int(time.time() * 1000) + 100


def test_payment_processed_to_dict():
    event = PaymentProcessed(
        payment_id="pay-1", order_id="ord-1", method="CARD",
        status="APPROVED", amount_usd=39.98, gateway="stripe",
    )
    d = event.to_dict()
    assert d["status"] == "APPROVED"
    assert "event_id" in d


def test_session_started_to_dict():
    event = SessionStarted(
        session_id="ses-1", user_id="usr-1", device="mobile",
        os="iOS", browser="Safari", ip="1.2.3.4", referrer="https://example.com",
    )
    d = event.to_dict()
    assert d["device"] == "mobile"


def test_recommendation_served_to_dict():
    event = RecommendationServed(
        rec_id="rec-1", user_id="usr-1", model_version="v2.0.0",
        items=["PROD-1", "PROD-2"], score=0.92, context="homepage",
    )
    d = event.to_dict()
    assert d["items"] == ["PROD-1", "PROD-2"]
    assert d["score"] == 0.92
