from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from marketleak.domain import (
    ActorVisibility,
    ObservationKind,
    OrderBookLevel,
    OrderBookSnapshot,
    PriceObservation,
    TradeFill,
    TradeSide,
)
from marketleak.multimodal.assembly import (
    AssemblyError,
    RawOrderFilledFact,
    ReferencePriceFact,
    adapt_order_filled,
    assemble_multimodal_event,
)
from marketleak.multimodal.features import build_market_feature_snapshot
from marketleak.multimodal.schemas import (
    MissingnessStatus,
    Modality,
    Provenance,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
MARKET_UID = "polymarket:btc-15m"
OUTCOME_UID = "polymarket:btc-up"


def provenance(*, source_uid: str = "polymarket:trades", minute: int = 1) -> Provenance:
    return Provenance(
        source_uid=source_uid,
        raw_artifact_uid=f"raw:{source_uid.split(':', 1)[1]}-{minute}",
        parser_version="phase15-test",
        content_hash="a" * 64,
        retrieved_at=T0 + timedelta(minutes=minute + 1),
        source_url="https://example.test/raw",
    )


def reliability(*, source_uid: str = "polygon:rpc") -> SourceReliability:
    return SourceReliability(
        source_uid=source_uid,
        source_class=SourceClass.BLOCKCHAIN_RPC,
        tier=ReliabilityTier.HIGH,
        score=Decimal("0.9"),
        assessed_at=T0,
        rationale="raw source contract",
    )


def observation(*, minute: int = 1, ingested_minute: int | None = None, uid: str = "obs-1") -> PriceObservation:
    return PriceObservation(
        observation_uid=f"polymarket:{uid}",
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        platform="polymarket",
        price=Decimal("0.51"),
        kind=ObservationKind.LAST_TRADE,
        actor_visibility=ActorVisibility.UNKNOWN,
        event_time=T0 + timedelta(minutes=minute),
        ingested_at=T0 + timedelta(minutes=ingested_minute if ingested_minute is not None else minute + 1),
        source_uid="polymarket:trades",
        raw_artifact_uid=f"raw:obs-{uid}",
        parser_version="test",
    )


def fill(*, minute: int = 2, uid: str = "fill-1", size: str = "0.10") -> TradeFill:
    return TradeFill(
        fill_uid=f"polymarket:{uid}",
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        platform="polymarket",
        price=Decimal("0.52"),
        size=Decimal(size),
        side=TradeSide.BUY,
        actor_visibility=ActorVisibility.PUBLIC_WALLET,
        actor_uid="polymarket:wallet-1",
        transaction_uid="polymarket:tx-1",
        event_time=T0 + timedelta(minutes=minute),
        ingested_at=T0 + timedelta(minutes=minute + 1),
        source_uid="polymarket:trades",
        raw_artifact_uid=f"raw:{uid}",
        parser_version="test",
    )


def book(*, minute: int = 3) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        snapshot_uid="polymarket:book-1",
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        platform="polymarket",
        bids=(OrderBookLevel(price=Decimal("0.49"), size=Decimal("2")),),
        asks=(OrderBookLevel(price=Decimal("0.53"), size=Decimal("3")),),
        event_time=T0 + timedelta(minutes=minute),
        ingested_at=T0 + timedelta(minutes=minute + 1),
        source_uid="polymarket:book",
        raw_artifact_uid="raw:book-1",
        parser_version="test",
    )


def reference_payload() -> dict[str, object]:
    source_uid = "reference:settlement-source"
    return {
        "reference_uid": "reference:btc-usd-1",
        "market_uid": MARKET_UID,
        "outcome_uid": OUTCOME_UID,
        "asset_symbol": "BTC",
        "quote_currency": "USD",
        "price": Decimal("100000"),
        "observed_at": T0 + timedelta(minutes=3),
        "ingested_at": T0 + timedelta(minutes=4),
        "settlement_source_uid": source_uid,
        "documented_settlement_source_url": "https://example.test/settlement-rule",
        "provenance": provenance(source_uid=source_uid, minute=3),
        "reliability": reliability(source_uid=source_uid),
    }


def test_future_event_and_later_ingested_record_are_excluded():
    snapshot = build_market_feature_snapshot(
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        as_of=T0 + timedelta(minutes=7),
        observations=[observation(minute=1), observation(minute=6, uid="future")],
        fills=[fill(minute=2)],
        books=[book()],
    )

    assert snapshot.window_starts_at == T0
    assert snapshot.window_ends_at == T0 + timedelta(minutes=5)
    assert snapshot.price_observation_count == 1
    assert snapshot.observation_uids == ("polymarket:obs-1",)

    later_received = observation(minute=4, ingested_minute=8, uid="late-receipt")
    event = assemble_multimodal_event(
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        as_of=T0 + timedelta(minutes=7),
        observations=[later_received],
    )
    assert event.market_features is None
    status = {item.modality: item.status for item in event.modality_availability}
    assert status[Modality.MARKET_STATE] == MissingnessStatus.MISSING


def test_missing_book_and_trade_streams_are_explicit_not_imputed():
    event = assemble_multimodal_event(
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        as_of=T0 + timedelta(minutes=7),
        observations=[observation()],
    )

    assert event.market_features is not None
    assert event.market_features.fill_count == 0
    assert event.market_features.orderbook_snapshot_count == 0
    assert event.market_features.trade_notional is None
    assert event.market_features.best_bid is None
    assert event.market_features.bid_depth is None


def test_liquidity_is_a_feature_not_a_hard_decision_rule():
    event = assemble_multimodal_event(
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        as_of=T0 + timedelta(minutes=7),
        fills=[fill(size="0.01")],
    )

    assert event.market_features is not None
    assert event.market_features.trade_notional == Decimal("0.0052")
    assert not hasattr(event.market_features, "is_thin_market")
    assert not hasattr(event, "is_alert")
    assert event.not_proof_of_fraud is True


def test_reference_price_requires_a_documented_market_settlement_source():
    payload = reference_payload()
    payload["documented_settlement_source_url"] = ""
    with pytest.raises(ValidationError):
        ReferencePriceFact.model_validate(payload)

    reference = ReferencePriceFact.model_validate(reference_payload())
    event = assemble_multimodal_event(
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        as_of=T0 + timedelta(minutes=7),
        reference_price=reference,
    )
    assert event.reference_price == reference


def test_legacy_transfer_single_placeholder_and_bitcoin_claims_are_rejected():
    canonical_fill = fill()
    legacy = {
        "event_type": "TransferSingle",
        "chain": "polygon",
        "chain_id": 137,
        "price": Decimal("0.5"),
    }
    with pytest.raises(AssemblyError, match="TransferSingle"):
        adapt_order_filled(legacy, fill=canonical_fill, as_of=T0 + timedelta(minutes=7))

    bitcoin = {"event_type": "OrderFilled", "chain": "bitcoin", "chain_id": 0}
    with pytest.raises(AssemblyError, match="Bitcoin"):
        adapt_order_filled(bitcoin, fill=canonical_fill, as_of=T0 + timedelta(minutes=7))


def test_raw_polygon_order_filled_requires_an_exact_canonical_join():
    canonical_fill = fill()
    raw = RawOrderFilledFact(
        event_type="OrderFilled",
        chain="polygon",
        chain_id=137,
        transaction_hash="0xabc",
        log_index=1,
        block_number=123,
        block_timestamp=T0 + timedelta(minutes=4),
        contract_address="0xexchange",
        order_hash="0xorder",
        maker_uid="polymarket:wallet-1",
        token_uid="polymarket:token-btc-up",
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        venue_transaction_uid="polymarket:tx-1",
        fill_uid=canonical_fill.fill_uid,
        maker_amount=Decimal("100"),
        taker_amount=Decimal("2"),
        provenance=provenance(source_uid="polygon:rpc", minute=4),
        reliability=reliability(source_uid="polygon:rpc"),
    )

    settlement = adapt_order_filled(raw, fill=canonical_fill, as_of=T0 + timedelta(minutes=7))

    assert settlement is not None
    assert settlement.market_uid == MARKET_UID
    assert settlement.outcome_uid == OUTCOME_UID
    assert settlement.quantity is None  # Maker/taker amounts have different asset units.
    mismatched = raw.model_copy(update={"fill_uid": "polymarket:other-fill"})
    with pytest.raises(AssemblyError, match="fill_uid"):
        adapt_order_filled(mismatched, fill=canonical_fill, as_of=T0 + timedelta(minutes=7))


def test_assembly_is_deterministic_with_continuous_microstructure_features():
    first = assemble_multimodal_event(
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        as_of=T0 + timedelta(minutes=7),
        observations=[observation(uid="obs-b", minute=4), observation(uid="obs-a", minute=1)],
        fills=[fill(uid="fill-b", minute=4), fill(uid="fill-a", minute=2)],
        books=[book()],
    )
    second = assemble_multimodal_event(
        market_uid=MARKET_UID,
        outcome_uid=OUTCOME_UID,
        as_of=T0 + timedelta(minutes=7),
        observations=[observation(uid="obs-a", minute=1), observation(uid="obs-b", minute=4)],
        fills=[fill(uid="fill-a", minute=2), fill(uid="fill-b", minute=4)],
        books=[book()],
    )

    assert first == second
    assert first.market_features is not None
    assert first.market_features.quoted_spread == Decimal("0.04")
    assert first.market_features.depth_imbalance == Decimal("-0.2")
    assert first.market_features.source_high_watermarks["polymarket:trades"] == T0 + timedelta(minutes=4)
