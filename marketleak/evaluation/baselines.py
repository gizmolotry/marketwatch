"""Transparent naive baselines and feature-ablation helpers."""

from __future__ import annotations

from decimal import Decimal
from typing import Callable, Iterable, Mapping

from .schemas import EvaluationRow


def price_change_score(features: Mapping[str, Decimal]) -> Decimal:
    value = abs(features.get("price_change", Decimal(0)))
    return min(Decimal(1), value)


def volume_score(features: Mapping[str, Decimal]) -> Decimal:
    value = max(Decimal(0), features.get("volume", Decimal(0)))
    return value / (Decimal(1) + value)


def score_baselines(
    rows: Iterable[tuple[EvaluationRow, Mapping[str, Decimal]]],
) -> dict[str, list[EvaluationRow]]:
    result = {"naive_price_change": [], "naive_volume": []}
    for row, features in rows:
        result["naive_price_change"].append(row.model_copy(update={"score": price_change_score(features)}))
        result["naive_volume"].append(row.model_copy(update={"score": volume_score(features)}))
    return result


def feature_ablation_scores(
    features: Mapping[str, Decimal],
    scorer: Callable[[Mapping[str, Decimal]], Decimal],
) -> dict[str, Decimal]:
    scores = {"all_features": scorer(features)}
    for feature in sorted(features):
        scores[f"without:{feature}"] = scorer({key: value for key, value in features.items() if key != feature})
    return scores
