from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest

from marketleak.onchain.polymarket_orderfilled import (
    CTF_EXCHANGE_V2,
    CanonicalVenueFill,
    ORDER_FILLED_V2_TOPIC,
    OrderFilledRejection,
    RawLogLineage,
    parse_and_reconcile_order_filled,
    parse_order_filled_v2,
)


T0 = datetime(2026, 7, 13, 12, tzinfo=UTC)
TX_HASH = "0x" + "1" * 64
ORDER_HASH = "0x" + "2" * 64
MAKER = "0x" + "3" * 40
TAKER = "0x" + "4" * 40


def _topic_address(address: str) -> str:
    return "0x" + "0" * 24 + address.removeprefix("0x")


def _word(value: int) -> str:
    return f"{value:064x}"


def _lineage() -> RawLogLineage:
    return RawLogLineage(
        raw_artifact_uid="raw:polygon-orderfilled-fixture",
        raw_sha256="a" * 64,
        source_uid="polygon:rpc/eth_getLogs",
        parser_version="polymarket-orderfilled-v2-test",
        retrieved_at=T0,
    )


def _raw_log() -> dict[str, object]:
    # V2 data words: side, tokenId, makerAmountFilled, takerAmountFilled,
    # fee, builder, metadata.  order hash/maker/taker are indexed topics.
    return {
        "event": "OrderFilled",
        "chain": "polygon",
        "chain_id": 137,
        "address": CTF_EXCHANGE_V2,
        "transaction_hash": TX_HASH,
        "log_index": 7,
        "block_hash": "0x" + "5" * 64,
        "block_number": 123456,
        "block_timestamp": T0,
        "topics": [ORDER_FILLED_V2_TOPIC, ORDER_HASH, _topic_address(MAKER), _topic_address(TAKER)],
        "data": "0x" + "".join((_word(0), _word(42), _word(100), _word(200), _word(3), _word(6), _word(7))),
    }


def _fill(*, uid: str = "polymarket:fill/exact", outcome_uid: str = "polymarket:outcome/42") -> CanonicalVenueFill:
    return CanonicalVenueFill(
        fill_uid=uid,
        transaction_hash=TX_HASH,
        market_uid="polymarket:market/condition-1",
        outcome_uid=outcome_uid,
    )


def test_valid_v2_log_has_one_exact_canonical_fill_join() -> None:
    result = parse_and_reconcile_order_filled(_raw_log(), lineage=_lineage(), canonical_fills=[_fill()])
    assert result.status == "corroborated"
    assert result.fill_uid == "polymarket:fill/exact"
    assert result.market_uid == "polymarket:market/condition-1"
    assert result.outcome_uid == "polymarket:outcome/42"
    assert result.log.side == "buy"
    assert result.log.token_id == 42
    assert result.log.maker_amount_filled == 100
    assert result.log.taker_amount_filled == 200
    assert result.log.fee == 3
    assert result.identity_inferred is False
    assert result.intent_inferred is False


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda log: log.update({"chain_id": 1}), "Polygon"),
        (lambda log: log.update({"chain": "bitcoin"}), "Bitcoin"),
        (lambda log: log.update({"address": "0x" + "9" * 40}), "official Polymarket"),
        (lambda log: log.update({"event": "TransferSingle"}), "TransferSingle"),
        (lambda log: log.update({"price": "0.5"}), "price"),
        (lambda log: log.update({"bitcoin_address": "bc1example"}), "Bitcoin"),
        (lambda log: log.update({"identity_claim": "someone"}), "identity"),
        (lambda log: log.update({"intent_claim": "something"}), "intent"),
        (lambda log: log.update({"topics": ["0x" + "0" * 64, ORDER_HASH, _topic_address(MAKER), _topic_address(TAKER)]}), "topic"),
        (lambda log: log.update({"data": "0x" + "00" * 32}), "seven ABI words"),
    ],
)
def test_rejects_non_contract_or_non_orderfilled_claim_paths(mutate, match: str) -> None:
    raw = deepcopy(_raw_log())
    mutate(raw)
    with pytest.raises(OrderFilledRejection, match=match):
        parse_order_filled_v2(raw, lineage=_lineage())


def test_raw_lineage_is_required_before_parse() -> None:
    with pytest.raises(OrderFilledRejection, match="raw artifact UID"):
        RawLogLineage(
            raw_artifact_uid="",
            raw_sha256="a" * 64,
            source_uid="polygon:rpc",
            parser_version="test",
            retrieved_at=T0,
        )
    with pytest.raises(OrderFilledRejection, match="SHA-256"):
        RawLogLineage(
            raw_artifact_uid="raw:one",
            raw_sha256="wrong",
            source_uid="polygon:rpc",
            parser_version="test",
            retrieved_at=T0,
        )


def test_unmatched_and_multi_event_ambiguity_never_name_market_or_outcome() -> None:
    unmatched = parse_and_reconcile_order_filled(_raw_log(), lineage=_lineage(), canonical_fills=[])
    assert unmatched.status == "unmatched"
    assert unmatched.fill_uid is None
    assert unmatched.market_uid is None
    assert unmatched.outcome_uid is None

    ambiguous = parse_and_reconcile_order_filled(
        _raw_log(),
        lineage=_lineage(),
        canonical_fills=[_fill(uid="polymarket:fill/one"), _fill(uid="polymarket:fill/two")],
    )
    assert ambiguous.status == "ambiguous"
    assert ambiguous.fill_uid is None
    assert ambiguous.market_uid is None
    assert ambiguous.outcome_uid is None


def test_outcome_token_mismatch_is_unmatched_not_inferred() -> None:
    result = parse_and_reconcile_order_filled(
        _raw_log(),
        lineage=_lineage(),
        canonical_fills=[_fill(outcome_uid="polymarket:outcome/999")],
    )
    assert result.status == "unmatched"
    assert result.reason == "no_exact_canonical_transaction_and_outcome_join"
