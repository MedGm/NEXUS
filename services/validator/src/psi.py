import math


def compute_psi(
    observed: list[float],
    reference: list[float],
    bins: int = 10,
    min_reference_samples: int = 100,
) -> float | None:
    """
    Population Stability Index between observed and reference distributions.
    Returns None if reference has fewer than min_reference_samples values.
    """
    if len(reference) < min_reference_samples:
        return None
    if len(observed) == 0:
        return None

    epsilon = 1e-4

    # Build bin edges from reference distribution
    ref_min = min(reference)
    ref_max = max(reference)
    if ref_min == ref_max:
        # Degenerate: all reference values identical
        ref_max = ref_min + 1.0

    bin_edges = [ref_min + (ref_max - ref_min) * i / bins for i in range(bins + 1)]
    bin_edges[-1] += 1e-9  # ensure max value falls in last bin

    def _bin_counts(values: list[float]) -> list[int]:
        counts = [0] * bins
        for v in values:
            for j in range(bins):
                if bin_edges[j] <= v < bin_edges[j + 1]:
                    counts[j] += 1
                    break
            else:
                counts[-1] += 1  # catch exact max
        return counts

    ref_counts = _bin_counts(reference)
    obs_counts = _bin_counts(observed)

    ref_total = len(reference)
    obs_total = len(observed)

    psi = 0.0
    for ref_c, obs_c in zip(ref_counts, obs_counts):
        ref_pct = max(ref_c / ref_total, epsilon)
        obs_pct = max(obs_c / obs_total, epsilon)
        psi += (obs_pct - ref_pct) * math.log(obs_pct / ref_pct)

    return psi
