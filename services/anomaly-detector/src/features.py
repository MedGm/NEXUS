import time
from collections import OrderedDict


class _LRUSet:
    """Bounded set — evicts oldest entry when full."""
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


class FeatureExtractor:
    """
    Extracts per-type feature vectors from raw Avro record dicts.
    All features are derived at consume time — no schema changes required.

    Derived features:
      inter_arrival_ms: gap between consecutive event timestamps per type (in-memory).
                        First event of a type returns 0.
      consumer_lag_ms:  int(time.time() * 1000) - event["timestamp"]. Measures Kafka
                        processing lag, NOT business latency. Catches consumer group lag
                        and replay events arriving out of time.
      is_new_user:      1.0 if user_id not seen in last 10k sessions, else 0.0.
    """

    def __init__(self):
        self._last_ts: dict[str, int] = {}
        self._seen_users = _LRUSet(maxsize=10_000)

    def extract(self, event_type: str, record: dict) -> dict[str, float]:
        now_ms = int(time.time() * 1000)
        event_ts = record.get("timestamp", now_ms)

        consumer_lag_ms = float(max(0, now_ms - event_ts))

        last_ts = self._last_ts.get(event_type, event_ts)
        inter_arrival_ms = float(max(0, event_ts - last_ts))
        self._last_ts[event_type] = event_ts

        if event_type == "OrderPlaced":
            return {
                "total_usd": float(record.get("total_usd", 0.0)),
                "item_count": float(len(record.get("items", []))),
                "inter_arrival_ms": inter_arrival_ms,
            }
        elif event_type == "PaymentProcessed":
            return {
                "amount_usd": float(record.get("amount_usd", 0.0)),
                "is_failure": 0.0 if record.get("status") == "success" else 1.0,
                "consumer_lag_ms": consumer_lag_ms,
            }
        elif event_type == "SessionStarted":
            user_id = record.get("user_id", "")
            is_new = 0.0 if user_id in self._seen_users else 1.0
            self._seen_users.add(user_id)
            return {
                "inter_arrival_ms": inter_arrival_ms,
                "is_new_user": is_new,
            }
        elif event_type == "RecommendationServed":
            return {
                "confidence_score": float(record.get("score", 0.0)),
                "consumer_lag_ms": consumer_lag_ms,
                "inter_arrival_ms": inter_arrival_ms,
            }
        elif event_type == "PriceSnapshot":
            return {
                "pct_change_24h": float(record.get("pct_change_24h", 0.0)),
                "volume_24h_usd": float(record.get("volume_24h_usd", 0.0)),
            }
        else:
            raise ValueError(f"Unknown event_type: {event_type!r}")
