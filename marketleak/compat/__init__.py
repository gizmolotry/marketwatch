"""Compatibility adapters between legacy v1 payloads and canonical v2 DTOs."""

from .legacy import (
    attach_assessment_v2,
    legacy_alert_payload,
    market_tick_to_price_observation,
    with_v2_assessment,
)

__all__ = [
    "attach_assessment_v2",
    "legacy_alert_payload",
    "market_tick_to_price_observation",
    "with_v2_assessment",
]
