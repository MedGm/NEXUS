import os

TRUST_GATE_THRESHOLD = float(os.environ.get("TRUST_GATE_THRESHOLD", "0.6"))
LAG_THRESHOLD_MS = int(os.environ.get("INFERENCE_LAG_THRESHOLD_MS", "5000"))


class TwoTierRouter:
    """
    Routes inference requests to one of two tiers:
      'accurate': River model (trust >= gate AND lag < threshold)
      'fallback':  StatisticalFallback (no trust data, OR high lag)
      'reject':    trust below gate — caller raises HTTP 422

    Returns (tier, rejection_reason). rejection_reason is None unless tier == 'reject'.
    """

    def __init__(
        self,
        trust_gate: float = TRUST_GATE_THRESHOLD,
        lag_threshold_ms: int = LAG_THRESHOLD_MS,
    ):
        self._trust_gate = trust_gate
        self._lag_threshold_ms = lag_threshold_ms

    def route(
        self,
        dataset_id: str,
        features: dict[str, float],
        trust_score: float | None,
    ) -> tuple[str, str | None]:
        if trust_score is None:
            return "fallback", None

        if trust_score < self._trust_gate:
            return (
                "reject",
                f"Trust score {trust_score:.2f} below threshold {self._trust_gate} "
                f"for dataset {dataset_id}",
            )

        lag = features.get("consumer_lag_ms", 0.0)
        if lag >= self._lag_threshold_ms:
            return "fallback", None

        return "accurate", None
