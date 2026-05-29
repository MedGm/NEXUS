import pytest
from src.psi import compute_psi


def test_returns_none_when_reference_too_small():
    observed = [float(i) for i in range(50)]
    reference = [float(i) for i in range(50)]   # < min_reference_samples=100
    assert compute_psi(observed, reference, min_reference_samples=100) is None


def test_returns_none_when_observed_empty():
    reference = [float(i) for i in range(200)]
    assert compute_psi([], reference, min_reference_samples=100) is None


def test_identical_distributions_near_zero():
    # Same distribution split in half → PSI should be close to 0
    data = [float(i % 20) for i in range(400)]
    result = compute_psi(data[:200], data[200:], min_reference_samples=100)
    assert result is not None
    assert result < 0.05


def test_very_different_distributions_high_psi():
    # Observed all low values, reference all high values
    observed = [1.0] * 200
    reference = [100.0] * 200
    result = compute_psi(observed, reference, min_reference_samples=100)
    assert result is not None
    assert result > 0.5


def test_epsilon_prevents_division_by_zero():
    # All observed in one bin that has zero reference count
    observed = [0.0] * 200
    reference = [float(i + 50) for i in range(200)]
    result = compute_psi(observed, reference, min_reference_samples=100)
    assert result is not None   # Must not raise ZeroDivisionError or return inf


def test_result_is_non_negative():
    observed = [float(i % 10) for i in range(200)]
    reference = [float(i % 15) for i in range(200)]
    result = compute_psi(observed, reference, min_reference_samples=100)
    assert result is not None
    assert result >= 0.0
