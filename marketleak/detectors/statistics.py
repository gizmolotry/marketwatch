"""Small deterministic statistical helpers used by the detector."""

from __future__ import annotations

from math import erfc, log10, sqrt
from statistics import median
from typing import Sequence

from .models import ComponentDiagnostic


def median_absolute_deviation(values: Sequence[float]) -> tuple[float, float]:
    center = float(median(values))
    mad = float(median(abs(value - center) for value in values))
    return center, mad


def causal_tail_diagnostic(
    value: float,
    history: Sequence[float],
    *,
    min_observations: int,
    empirical_tail_min_observations: int,
) -> ComponentDiagnostic | None:
    """Score a value against trailing history without an invented variance.

    Non-zero MAD uses a robust z diagnostic. A zero MAD never receives an
    epsilon or magic fallback scale: only an adequately populated smoothed
    empirical tail is allowed.
    """

    finite_history = [float(item) for item in history]
    if len(finite_history) < min_observations:
        return None
    center, mad = median_absolute_deviation(finite_history)
    if mad > 0:
        robust_z = 0.6744897501960817 * (value - center) / mad
        p_value = min(1.0, max(0.0, erfc(abs(robust_z) / sqrt(2.0))))
        return ComponentDiagnostic(
            value=value,
            baseline_count=len(finite_history),
            baseline_median=center,
            baseline_mad=mad,
            method="median_mad",
            score=abs(robust_z),
            p_value=p_value,
        )
    if len(finite_history) < empirical_tail_min_observations:
        return None
    distance = abs(value - center)
    tail_count = sum(abs(item - center) >= distance for item in finite_history)
    p_value = (tail_count + 1.0) / (len(finite_history) + 1.0)
    return ComponentDiagnostic(
        value=value,
        baseline_count=len(finite_history),
        baseline_median=center,
        baseline_mad=0.0,
        method="smoothed_empirical_tail",
        score=-log10(max(p_value, 1e-300)),
        p_value=p_value,
    )


def benjamini_hochberg(p_values: Sequence[float]) -> list[tuple[float, int]]:
    """Return BH-adjusted q-values and deterministic ranks in input order."""

    count = len(p_values)
    if not count:
        return []
    ordered = sorted(enumerate(p_values), key=lambda item: (item[1], item[0]))
    adjusted = [1.0] * count
    running_min = 1.0
    for reverse_index in range(count - 1, -1, -1):
        original_index, p_value = ordered[reverse_index]
        rank = reverse_index + 1
        running_min = min(running_min, p_value * count / rank)
        adjusted[original_index] = min(1.0, running_min)
    ranks = [0] * count
    for rank, (original_index, _) in enumerate(ordered, start=1):
        ranks[original_index] = rank
    return list(zip(adjusted, ranks))
