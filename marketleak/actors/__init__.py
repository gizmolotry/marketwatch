"""Actor-level surveillance features derived from attributable fills."""

from .features import (
    ActorFeatureBuilder,
    ActorFeatures,
    ActorPosition,
    build_actor_features,
    build_actor_positions,
)
from .wallet_cohort import (
    CohortQueryFilter,
    CohortMarketScopeBinding,
    CohortRankingReport,
    FeatureSpec,
    FeatureSurprise,
    NuisanceVector,
    PopulationCoverage,
    PopulationCoverageSlice,
    RankedWallet,
    WalletCohortPolicy,
    WalletEventFeatureVector,
    rank_wallet_cohort,
)

_PRIORITY_EXPORTS = frozenset(
    {
        "LongitudinalPriorityInput",
        "WalletPriorityPolicy",
        "WalletPriorityQueue",
        "WalletPriorityRow",
        "build_wallet_priority_queue",
    }
)


def __getattr__(name: str):
    """Load priority exports lazily so wallet tracking can import actor features."""

    if name not in _PRIORITY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from . import wallet_priority

    value = getattr(wallet_priority, name)
    globals()[name] = value
    return value

__all__ = [
    "ActorFeatureBuilder",
    "ActorFeatures",
    "ActorPosition",
    "build_actor_features",
    "build_actor_positions",
    "CohortQueryFilter",
    "CohortMarketScopeBinding",
    "CohortRankingReport",
    "FeatureSpec",
    "FeatureSurprise",
    "NuisanceVector",
    "PopulationCoverage",
    "PopulationCoverageSlice",
    "RankedWallet",
    "WalletCohortPolicy",
    "WalletEventFeatureVector",
    "rank_wallet_cohort",
    "WalletPriorityPolicy",
    "LongitudinalPriorityInput",
    "WalletPriorityQueue",
    "WalletPriorityRow",
    "build_wallet_priority_queue",
]
