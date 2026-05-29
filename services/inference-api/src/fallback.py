import logging
import os

log = logging.getLogger(__name__)

WARMUP_EVENTS = int(os.environ.get("FALLBACK_WARMUP_EVENTS", "500"))
IQR_MULTIPLIER = 1.5


class StatisticalFallback:
    """
    Per-feature IQR anomaly detector. Fits bounds on the first `warmup_events`
    events per dataset then switches to scoring. Resets on restart — re-warms
    in ~1 minute of traffic at normal load. Not persisted.
    """

    def __init__(self, warmup_events: int = WARMUP_EVENTS):
        self._warmup_events = warmup_events
        self._samples: dict[str, dict[str, list[float]]] = {}
        self._counts: dict[str, int] = {}
        self._bounds: dict[str, dict[str, tuple[float, float]]] = {}

    def _is_warmed(self, dataset_id: str) -> bool:
        return dataset_id in self._bounds

    def _record(self, dataset_id: str, features: dict[str, float]) -> None:
        if dataset_id not in self._samples:
            self._samples[dataset_id] = {}
            self._counts[dataset_id] = 0
        for k, v in features.items():
            self._samples[dataset_id].setdefault(k, []).append(v)
        self._counts[dataset_id] += 1
        if self._counts[dataset_id] >= self._warmup_events:
            self._compute_bounds(dataset_id)

    def _compute_bounds(self, dataset_id: str) -> None:
        bounds: dict[str, tuple[float, float]] = {}
        for feature, values in self._samples[dataset_id].items():
            s = sorted(values)
            n = len(s)
            q1 = s[n // 4]
            q3 = s[(3 * n) // 4]
            iqr = q3 - q1
            bounds[feature] = (q1 - IQR_MULTIPLIER * iqr, q3 + IQR_MULTIPLIER * iqr)
        self._bounds[dataset_id] = bounds
        del self._samples[dataset_id]
        log.info("StatisticalFallback warmed up for dataset=%s", dataset_id)

    def score(self, dataset_id: str, features: dict[str, float]) -> float:
        """
        Returns 1.0 if any feature is outside IQR bounds, 0.0 otherwise.
        Returns 0.0 and records sample during warm-up period.
        """
        if not self._is_warmed(dataset_id):
            self._record(dataset_id, features)
            return 0.0
        bounds = self._bounds[dataset_id]
        for feature, val in features.items():
            if feature in bounds:
                lower, upper = bounds[feature]
                if val < lower or val > upper:
                    return 1.0
        return 0.0
