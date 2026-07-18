from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from marketleak.compat import attach_assessment_v2, market_tick_to_price_observation
from marketleak.domain import ActorVisibility
from marketleak.schemas import MarketTick


def test_snapshot_tick_does_not_fabricate_trade_fields() -> None:
    tick = MarketTick(
        tick_uid="tick-1",
        market_uid="polymarket:m-1",
        timestamp=1_700_000_000,
        price=0.52,
        platform="polymarket",
    )
    assert tick.size is None
    assert tick.maker is None
    assert tick.taker is None


def test_partial_actor_attribution_is_rejected() -> None:
    with pytest.raises(ValidationError, match="maker and taker"):
        MarketTick(
            tick_uid="tick-1",
            market_uid="polymarket:m-1",
            timestamp=1_700_000_000,
            price=0.52,
            maker="0x1234567890123456789012345678901234567890",
            platform="polymarket",
        )


def test_tick_adapter_marks_actor_unavailable_and_uses_decimal() -> None:
    tick = MarketTick(
        tick_uid="tick-1",
        market_uid="polymarket:m-1",
        timestamp=1_700_000_000,
        price=0.52,
        platform="polymarket",
    )
    observation = market_tick_to_price_observation(
        tick,
        outcome_uid="polymarket:yes",
        source_uid="polymarket:prices-api",
        raw_artifact_uid="raw:abc",
        parser_version="2.0.0",
        ingested_at=datetime(2026, 7, 12, tzinfo=timezone.utc),
    )
    assert observation.price == Decimal("0.52")
    assert observation.actor_visibility == ActorVisibility.NOT_AVAILABLE


def test_compat_payload_preserves_legacy_keys_without_mutation() -> None:
    legacy = {"market_uid": "polymarket:m-1", "leak_risk": 0.91, "nested": {"x": 1}}
    v2 = {
        "activity": {"status": "abnormal"},
        "public_explanation": {"status": "unknown_coverage"},
        "actor_evidence": {"status": "no_actor_data"},
        "not_proof_of_fraud": False,
    }
    payload = attach_assessment_v2(legacy, v2)
    assert payload["market_uid"] == legacy["market_uid"]
    assert payload["leak_risk"] == legacy["leak_risk"]
    assert payload["nested"] == legacy["nested"]
    assert payload["assessment_v2"]["not_proof_of_fraud"] is True
    assert payload["not_proof_of_fraud"] is True
    assert "assessment_v2" not in legacy
    assert v2["not_proof_of_fraud"] is False
