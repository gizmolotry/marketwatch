"""Validation-first causal activity detection."""

from .causal import CausalActivityDetector, detect_activity, detect_legacy_ticks
from .models import (
    BucketFeatures,
    ComponentDiagnostic,
    DetectionBatch,
    DetectorConfig,
    DetectorSignal,
    Incident,
    MarketControl,
)
from .statistics import benjamini_hochberg, causal_tail_diagnostic, median_absolute_deviation

__all__ = [
    "BucketFeatures",
    "CausalActivityDetector",
    "ComponentDiagnostic",
    "DetectionBatch",
    "DetectorConfig",
    "DetectorSignal",
    "Incident",
    "MarketControl",
    "benjamini_hochberg",
    "causal_tail_diagnostic",
    "detect_activity",
    "detect_legacy_ticks",
    "median_absolute_deviation",
]
