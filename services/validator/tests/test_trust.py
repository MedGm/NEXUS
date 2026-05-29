import pytest
from src.trust import (
    compute_freshness,
    score_from_psi,
    TrustScoreCalculator,
    WEIGHTS,
    PSI_FLAG_THRESHOLD,
    PSI_ZERO_THRESHOLD,
)


# ── compute_freshness ─────────────────────────────────────────────────────────

def test_freshness_age_zero():
    assert compute_freshness(0, max_age=300) == 1.0


def test_freshness_at_max_age():
    assert compute_freshness(300, max_age=300) == 1.0


def test_freshness_at_double_max_age():
    assert compute_freshness(600, max_age=300) == 0.0


def test_freshness_beyond_double_max_age():
    assert compute_freshness(900, max_age=300) == 0.0


def test_freshness_midpoint():
    # age=450, max=300 → 1.0 - (450-300)/300 = 1.0 - 0.5 = 0.5
    result = compute_freshness(450, max_age=300)
    assert abs(result - 0.5) < 1e-9


# ── score_from_psi ────────────────────────────────────────────────────────────

def test_score_from_psi_none():
    assert score_from_psi(None) is None


def test_score_from_psi_zero():
    assert score_from_psi(0.0) == 1.0


def test_score_from_psi_at_zero_threshold():
    # PSI = PSI_ZERO_THRESHOLD → score = 0.0
    result = score_from_psi(PSI_ZERO_THRESHOLD)
    assert result == 0.0


def test_score_from_psi_beyond_threshold_clamped():
    result = score_from_psi(PSI_ZERO_THRESHOLD * 2)
    assert result == 0.0


def test_psi_flag_threshold_below_zero_threshold():
    # Flag fires before score reaches zero
    assert PSI_FLAG_THRESHOLD < PSI_ZERO_THRESHOLD


# ── TrustScoreCalculator ──────────────────────────────────────────────────────

def test_composite_perfect_scores_without_psi():
    calc = TrustScoreCalculator()
    result = calc.compute(
        freshness=1.0, completeness=1.0, drift_psi=None, lineage_depth=1
    )
    assert result["composite_score"] == pytest.approx(1.0)
    assert result["drift_psi"] is None


def test_composite_perfect_scores_with_psi():
    calc = TrustScoreCalculator()
    result = calc.compute(
        freshness=1.0, completeness=1.0, drift_psi=0.0, lineage_depth=1
    )
    assert result["composite_score"] == pytest.approx(1.0)


def test_composite_uses_correct_without_psi_weights():
    # freshness=1, completeness=0, psi=None, lineage=1 (depth=1 → score=1)
    # composite = 1*0.375 + 0*0.375 + 1*0.25 = 0.625
    calc = TrustScoreCalculator()
    result = calc.compute(
        freshness=1.0, completeness=0.0, drift_psi=None, lineage_depth=1
    )
    assert result["composite_score"] == pytest.approx(0.625)


def test_composite_uses_correct_with_psi_weights():
    # freshness=1, completeness=0, psi_score=1 (psi=0), lineage=1
    # composite = 1*0.30 + 0*0.30 + 1*0.30 + 1*0.10 = 0.70
    calc = TrustScoreCalculator()
    result = calc.compute(
        freshness=1.0, completeness=0.0, drift_psi=0.0, lineage_depth=1
    )
    assert result["composite_score"] == pytest.approx(0.70)


def test_lineage_depth_2_reduces_score():
    calc = TrustScoreCalculator()
    result1 = calc.compute(freshness=1.0, completeness=1.0, drift_psi=None, lineage_depth=1)
    result2 = calc.compute(freshness=1.0, completeness=1.0, drift_psi=None, lineage_depth=2)
    assert result2["composite_score"] < result1["composite_score"]


def test_weights_dict_structure():
    assert "with_psi" in WEIGHTS
    assert "without_psi" in WEIGHTS
    assert abs(sum(WEIGHTS["with_psi"].values()) - 1.0) < 1e-9
    assert abs(sum(WEIGHTS["without_psi"].values()) - 1.0) < 1e-9
