"""Closed vocabularies for the three independent integrity questions."""

from enum import Enum


class ActivityStatus(str, Enum):
    """A: whether market activity is statistically scorable and abnormal."""

    NOT_SCORABLE = "not_scorable"
    NORMAL = "normal"
    ABNORMAL = "abnormal"


class PublicExplanationStatus(str, Enum):
    """B: point-in-time public-information relationship to the activity."""

    OBSERVED_PRE_SHOCK = "observed_pre_shock"
    NO_MATCH_OBSERVED_PRE_SHOCK = "no_match_observed_pre_shock"
    TIMING_AMBIGUOUS = "timing_ambiguous"
    UNKNOWN_COVERAGE = "unknown_coverage"


class ActorEvidenceStatus(str, Enum):
    """C: strength of actor-specific context; never a finding of fraud."""

    NO_ACTOR_DATA = "no_actor_data"
    CONTEXT_ONLY = "context_only"
    CORROBORATED_ACCESS_SIGNAL = "corroborated_access_signal"
    INSUFFICIENT = "insufficient"


class ActorVisibility(str, Enum):
    PUBLIC_WALLET = "public_wallet"
    OWN_ACCOUNT_ONLY = "own_account_only"
    NOT_AVAILABLE = "not_available"
    UNKNOWN = "unknown"


class AnalysisTarget(str, Enum):
    ACTIVITY = "activity"
    PUBLIC_EXPLANATION = "public_explanation"
    ACTOR_EVIDENCE = "actor_evidence"


class ObservationKind(str, Enum):
    LAST_TRADE = "last_trade"
    MIDPOINT = "midpoint"
    BEST_BID = "best_bid"
    BEST_ASK = "best_ask"
    PLATFORM_SNAPSHOT = "platform_snapshot"


class TradeSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class EvidenceType(str, Enum):
    PUBLIC_INFORMATION = "public_information"
    MARKET_DATA = "market_data"
    ACTOR_ACTIVITY = "actor_activity"
    ACCESS_CONTEXT = "access_context"
    RESOLUTION = "resolution"
    OTHER = "other"


class CoverageStatus(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class EvidenceRelation(str, Enum):
    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"
    CONTEXT_FOR = "context_for"
    DERIVED_FROM = "derived_from"
    DUPLICATE_OF = "duplicate_of"


class ShadowRunStatus(str, Enum):
    PLANNED = "planned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
