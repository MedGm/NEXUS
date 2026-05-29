import os
import time
from collections import OrderedDict

import scipy.stats

KS_THRESHOLD = float(os.environ.get("KS_THRESHOLD", "0.15"))
MIN_SAMPLES = int(os.environ.get("KS_MIN_SAMPLES", "50"))
SCORE_BUFFER_SIZE = 250
HST_PARAMS = {"n_trees": 25, "height": 8, "window_size": 250}


def compute_ks(
    reference_scores: list[float],
    recent_scores: list[float],
) -> tuple[float, float]:
    """KS 2-sample test. Returns (ks_stat, ks_pvalue)."""
    stat, pval = scipy.stats.ks_2samp(reference_scores, recent_scores)
    return float(stat), float(pval)


def detect_event_type(record: dict) -> str | None:
    """Identify event type from record fields. Returns None if unknown."""
    if "order_id" in record and "total_usd" in record:
        return "OrderPlaced"
    if "payment_id" in record and "amount_usd" in record:
        return "PaymentProcessed"
    if "session_id" in record:
        return "SessionStarted"
    if "rec_id" in record:
        return "RecommendationServed"
    if "symbol" in record and "price_usd" in record:
        return "PriceSnapshot"
    return None


class _LRUSet:
    def __init__(self, maxsize: int):
        self._cache: OrderedDict = OrderedDict()
        self._maxsize = maxsize
    def __contains__(self, key) -> bool:
        return key in self._cache
    def add(self, key) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)
            return
        if len(self._cache) >= self._maxsize:
            self._cache.popitem(last=False)
        self._cache[key] = True


def extract_features_for_type(
    event_type: str,
    records: list[dict],
) -> list[dict[str, float]]:
    """
    Extract feature vectors for the given event_type.
    Records that don't match event_type are skipped.
    Mirrors FeatureExtractor logic from services/anomaly-detector.
    """
    seen_users = _LRUSet(maxsize=10_000)
    last_ts: dict[str, int] = {}
    result = []

    for record in records:
        if detect_event_type(record) != event_type:
            continue
        now_ms = int(time.time() * 1000)
        event_ts = record.get("timestamp", now_ms)
        consumer_lag_ms = float(max(0, now_ms - event_ts))
        last = last_ts.get(event_type, event_ts)
        inter_arrival_ms = float(max(0, event_ts - last))
        last_ts[event_type] = event_ts

        if event_type == "OrderPlaced":
            features = {
                "total_usd": float(record.get("total_usd", 0.0)),
                "item_count": float(len(record.get("items", []))),
                "inter_arrival_ms": inter_arrival_ms,
            }
        elif event_type == "PaymentProcessed":
            features = {
                "amount_usd": float(record.get("amount_usd", 0.0)),
                "is_failure": 0.0 if record.get("status") == "success" else 1.0,
                "consumer_lag_ms": consumer_lag_ms,
            }
        elif event_type == "SessionStarted":
            user_id = record.get("user_id", "")
            is_new = 0.0 if user_id in seen_users else 1.0
            seen_users.add(user_id)
            features = {"inter_arrival_ms": inter_arrival_ms, "is_new_user": is_new}
        elif event_type == "RecommendationServed":
            features = {
                "confidence_score": float(record.get("score", 0.0)),
                "consumer_lag_ms": consumer_lag_ms,
                "inter_arrival_ms": inter_arrival_ms,
            }
        elif event_type == "PriceSnapshot":
            features = {
                "pct_change_24h": float(record.get("pct_change_24h", 0.0)),
                "volume_24h_usd": float(record.get("volume_24h_usd", 0.0)),
            }
        else:
            continue
        result.append(features)

    return result
