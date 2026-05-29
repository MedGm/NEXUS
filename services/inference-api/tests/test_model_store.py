from src.model_store import InferenceModelStore


def _make_store_no_mlflow():
    """Bypass __init__ to avoid MLflow connection."""
    store = InferenceModelStore.__new__(InferenceModelStore)
    store._models = {t: None for t in ["OrderPlaced", "PaymentProcessed",
                                        "SessionStarted", "RecommendationServed",
                                        "PriceSnapshot"]}
    store._versions = {t: None for t in store._models}
    store._shadow_models = {t: None for t in store._models}
    store._shadow_versions = {t: None for t in store._models}
    return store


def test_get_returns_none_when_no_model():
    store = _make_store_no_mlflow()
    model, version = store.get("OrderPlaced")
    assert model is None
    assert version is None


def test_get_shadow_returns_none_when_no_shadow():
    store = _make_store_no_mlflow()
    model, version = store.get_shadow("OrderPlaced")
    assert model is None
    assert version is None


def test_get_returns_model_when_set():
    store = _make_store_no_mlflow()
    fake_model = object()
    store._models["OrderPlaced"] = fake_model
    store._versions["OrderPlaced"] = 3
    model, version = store.get("OrderPlaced")
    assert model is fake_model
    assert version == 3


def test_get_shadow_returns_shadow_model_when_set():
    store = _make_store_no_mlflow()
    fake_shadow = object()
    store._shadow_models["PaymentProcessed"] = fake_shadow
    store._shadow_versions["PaymentProcessed"] = 7
    model, version = store.get_shadow("PaymentProcessed")
    assert model is fake_shadow
    assert version == 7


def test_price_snapshot_always_returns_none():
    store = _make_store_no_mlflow()
    model, version = store.get("PriceSnapshot")
    assert model is None


def test_get_unknown_type_returns_none():
    store = _make_store_no_mlflow()
    model, version = store.get("Unknown")
    assert model is None
    assert version is None
