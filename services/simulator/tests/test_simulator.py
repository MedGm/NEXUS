from src.events import OrderPlaced, PaymentProcessed, SessionStarted, RecommendationServed
from src.simulator import EventSimulator

EVENT_TYPES = (OrderPlaced, PaymentProcessed, SessionStarted, RecommendationServed)


def test_next_returns_known_event_type():
    sim = EventSimulator()
    event = sim.next()
    assert isinstance(event, EVENT_TYPES)


def test_next_returns_varied_types_over_many_calls():
    sim = EventSimulator()
    types_seen = {type(sim.next()).__name__ for _ in range(200)}
    assert len(types_seen) == 4, f"Expected all 4 types, got: {types_seen}"


def test_order_placed_has_valid_items():
    sim = EventSimulator()
    orders = []
    for _ in range(500):
        e = sim.next()
        if isinstance(e, OrderPlaced):
            orders.append(e)
        if len(orders) >= 5:
            break
    assert orders, "No OrderPlaced events generated in 500 calls"
    for order in orders:
        assert len(order.items) >= 1
        assert order.total_usd > 0
        assert order.currency in ("USD", "EUR", "GBP")


def test_payment_processed_status_is_valid():
    sim = EventSimulator()
    payments = []
    for _ in range(500):
        e = sim.next()
        if isinstance(e, PaymentProcessed):
            payments.append(e)
        if len(payments) >= 5:
            break
    assert payments
    for p in payments:
        assert p.status in ("APPROVED", "DECLINED", "PENDING")


def test_session_started_device_is_valid():
    sim = EventSimulator()
    sessions = []
    for _ in range(500):
        e = sim.next()
        if isinstance(e, SessionStarted):
            sessions.append(e)
        if len(sessions) >= 3:
            break
    assert sessions
    for s in sessions:
        assert s.device in ("mobile", "desktop", "tablet")
