import os

from river.anomaly import HalfSpaceTrees
from river.drift import ADWIN

THRESHOLDS: dict[str, float | None] = {
    "OrderPlaced":          float(os.environ.get("THRESHOLD_ORDER", "0.75")),
    "PaymentProcessed":     float(os.environ.get("THRESHOLD_PAYMENT", "0.80")),
    "SessionStarted":       float(os.environ.get("THRESHOLD_SESSION", "0.75")),
    "RecommendationServed": float(os.environ.get("THRESHOLD_REC", "0.70")),
    "PriceSnapshot":        None,   # ADWIN uses drift_detected, not a numeric threshold
}

_HST_PARAMS = {"n_trees": 25, "height": 8, "window_size": 250}


def make_model(event_type: str):
    """Return a fresh River model for the given event type."""
    if event_type == "PriceSnapshot":
        return ADWIN()
    return HalfSpaceTrees(**_HST_PARAMS)


class AnomalyDetector:
    """
    Wraps one River model for a single event type.
    score_one() calls model.score_one() THEN model.learn_one() — this order ensures
    the current sample does not influence its own anomaly score.
    PriceSnapshot uses ADWIN as a change-point detector on pct_change_24h.
    """

    def __init__(self, event_type: str, model=None, threshold: float | None = None):
        self.event_type = event_type
        self._model = model if model is not None else make_model(event_type)
        # threshold override for testing; otherwise use env-configured default
        self._threshold = threshold if threshold is not None else THRESHOLDS.get(event_type)

    def score_one(self, features: dict[str, float]) -> tuple[float, bool]:
        """Returns (anomaly_score, is_anomaly). Score-then-learn order."""
        if self.event_type == "PriceSnapshot":
            pct = features.get("pct_change_24h", 0.0)
            self._model.update(pct)
            is_anomaly = bool(self._model.drift_detected)
            return (1.0 if is_anomaly else 0.0, is_anomaly)

        score = float(self._model.score_one(features))
        self._model.learn_one(features)
        threshold = self._threshold if self._threshold is not None else 0.75
        return (score, score >= threshold)

    @property
    def model(self):
        return self._model
