import os

PSI_FLAG_THRESHOLD = 0.20   # log warning + Prometheus counter when PSI exceeds this
PSI_ZERO_THRESHOLD = 0.25   # score hits 0.0 at this PSI value

MAX_AGE_SECONDS = int(os.environ.get("TRUST_MAX_AGE_SECONDS", "300"))

WEIGHTS = {
    "with_psi": {
        "freshness": 0.30,
        "completeness": 0.30,
        "drift_psi": 0.30,
        "lineage": 0.10,
    },
    "without_psi": {
        "freshness": 0.375,
        "completeness": 0.375,
        "lineage": 0.25,
    },
}


def compute_freshness(age_seconds: float, max_age: int = MAX_AGE_SECONDS) -> float:
    if age_seconds <= max_age:
        return 1.0
    return max(0.0, 1.0 - (age_seconds - max_age) / max_age)
    # age=0     → 1.0
    # age=MAX   → 1.0
    # age=2×MAX → 0.0
    # age>2×MAX → 0.0 (clamped)


def score_from_psi(psi: float | None) -> float | None:
    if psi is None:
        return None
    return max(0.0, 1.0 - psi / PSI_ZERO_THRESHOLD)


class TrustScoreCalculator:
    def compute(
        self,
        freshness: float,
        completeness: float,
        drift_psi: float | None,
        lineage_depth: int,
    ) -> dict:
        psi_score = score_from_psi(drift_psi)
        lineage_score = 1.0 / lineage_depth

        if psi_score is not None:
            w = WEIGHTS["with_psi"]
            composite = (
                freshness * w["freshness"]
                + completeness * w["completeness"]
                + psi_score * w["drift_psi"]
                + lineage_score * w["lineage"]
            )
        else:
            w = WEIGHTS["without_psi"]
            composite = (
                freshness * w["freshness"]
                + completeness * w["completeness"]
                + lineage_score * w["lineage"]
            )

        return {
            "freshness": freshness,
            "completeness": completeness,
            "drift_psi": psi_score,
            "lineage_depth": lineage_depth,
            "composite_score": max(0.0, min(1.0, composite)),
        }
