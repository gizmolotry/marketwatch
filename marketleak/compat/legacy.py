"""Explicit adapters at the v1/v2 API boundary."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

from marketleak.domain import (
    ActorVisibility,
    IntegrityAssessment,
    ObservationKind,
    PriceObservation,
    platform_qualified_uid,
)
from marketleak.schemas import MarketTick


def attach_assessment_v2(
    legacy_payload: Mapping[str, Any],
    assessment: IntegrityAssessment | Mapping[str, Any],
) -> dict[str, Any]:
    """Copy a legacy response and append the non-accusatory v2 assessment.

    Existing keys and nested values are preserved.  A caller cannot override
    the disclaimer through either input.
    """

    payload = deepcopy(dict(legacy_payload))
    if isinstance(assessment, IntegrityAssessment):
        assessment_payload = assessment.model_dump(mode="json")
    else:
        assessment_payload = deepcopy(dict(assessment))
        assessment_payload["not_proof_of_fraud"] = True
    payload["assessment_v2"] = assessment_payload
    payload["not_proof_of_fraud"] = True
    return payload


def legacy_alert_payload(
    legacy_payload: Mapping[str, Any],
    assessment: IntegrityAssessment | Mapping[str, Any],
) -> dict[str, Any]:
    """Backwards-compatible name for :func:`attach_assessment_v2`."""

    return attach_assessment_v2(legacy_payload, assessment)


def market_tick_to_price_observation(
    tick: MarketTick,
    *,
    outcome_uid: str,
    source_uid: str,
    raw_artifact_uid: str,
    parser_version: str,
    ingested_at: datetime,
    kind: ObservationKind = ObservationKind.PLATFORM_SNAPSHOT,
) -> PriceObservation:
    """Map a legacy tick without inventing size or actor identity."""

    event_time = datetime.fromtimestamp(tick.timestamp, tz=timezone.utc)
    visibility = (
        ActorVisibility.PUBLIC_WALLET
        if tick.maker is not None and tick.taker is not None
        else ActorVisibility.NOT_AVAILABLE
    )
    observation_uid = platform_qualified_uid(tick.platform, f"observation-{tick.tick_uid}")
    return PriceObservation(
        observation_uid=observation_uid,
        market_uid=tick.market_uid,
        outcome_uid=outcome_uid,
        platform=tick.platform,
        price=Decimal(str(tick.price)),
        kind=kind,
        actor_visibility=visibility,
        event_time=event_time,
        ingested_at=ingested_at,
        source_uid=source_uid,
        raw_artifact_uid=raw_artifact_uid,
        parser_version=parser_version,
    )


# Alternate verb form used by some downstream integrations.
with_v2_assessment = attach_assessment_v2
