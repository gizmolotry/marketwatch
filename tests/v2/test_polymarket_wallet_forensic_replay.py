"""Mechanics tests for the post-hoc wallet replay; fixtures are not evidence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketleak.domain import ActorVisibility, CoverageStatus, TradeFill, TradeSide
from marketleak.forensics import (
    ExternalCaseAuditContext,
    PolymarketWalletCase,
    ReplayQueryFilter,
    WalletReplayCoverage,
    build_polymarket_wallet_forensic_replay,
)
from marketleak.ingestion.connectors.polymarket import PolymarketConnector


# Public identifiers/times keep the fixture shape anchored to the official case,
# but these minimal rows test mechanics only and are not an effectiveness corpus.
WALLET = "0x31a56e9e690c621ed21de08cb559e9524cdb8ed9"
ACTOR_UID = f"polymarket:wallet/{WALLET}"
MARKET = "0x580adc1327de9bf7c179ef5aaffa3377bb5cb252b7d6390b027172d43fd6f993"
CASE_CUTOFF = datetime(2026, 1, 3, 2, 58, 25, tzinfo=UTC)
RECONSTRUCTED_AT = datetime(2026, 7, 19, 20, 0, tzinfo=UTC)
RAW_HASH = "a" * 64


def _case() -> PolymarketWalletCase:
    return PolymarketWalletCase(
        case_uid="public-case:polymarket-wallet-replay-2026-01",
        actor_uid=ACTOR_UID,
        historical_cutoff=CASE_CUTOFF,
    )


def _coverage(
    *,
    record_count: int,
    retrieved_at: datetime = RECONSTRUCTED_AT - timedelta(minutes=1),
    status: CoverageStatus = CoverageStatus.COMPLETE,
    interval_start: datetime = datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC),
    interval_end: datetime = RECONSTRUCTED_AT - timedelta(minutes=2),
    query_start: int | None = None,
    query_end: int | None = None,
) -> WalletReplayCoverage:
    return WalletReplayCoverage(
        platform="polymarket",
        dataset="public_wallet_trades",
        source_uid=PolymarketConnector.TRADE_SOURCE_UID,
        queried_actor_uid=ACTOR_UID,
        interval_start=interval_start,
        interval_end=interval_end,
        retrieved_at=retrieved_at,
        status=status,
        record_count=record_count,
        raw_sha256=(RAW_HASH,),
        query_filters=(
            ReplayQueryFilter.from_value("user", WALLET),
            ReplayQueryFilter.from_value("start", int(interval_start.timestamp()) if query_start is None else query_start),
            ReplayQueryFilter.from_value("end", int(interval_end.timestamp()) if query_end is None else query_end),
            ReplayQueryFilter.from_value("takerOnly", False),
        ),
        continuation=None if status == CoverageStatus.COMPLETE else "bounded-continuation-fixture",
        missing_reasons=() if status == CoverageStatus.COMPLETE else ("collector_did_not_claim_complete",),
    )


def _fill(
    uid: str,
    *,
    event_time: datetime,
    ingested_at: datetime,
    market_suffix: str = MARKET,
    price: str = "0.07",
    size: str = "1000",
    side: TradeSide = TradeSide.BUY,
) -> TradeFill:
    return TradeFill(
        event_time=event_time,
        ingested_at=ingested_at,
        source_uid=PolymarketConnector.TRADE_SOURCE_UID,
        raw_artifact_uid=f"polymarket:raw/{RAW_HASH}",
        parser_version=PolymarketConnector.PARSER_VERSION,
        fill_uid=f"polymarket:fill/{uid}",
        market_uid=f"polymarket:market/{market_suffix}",
        outcome_uid=f"polymarket:outcome/{market_suffix}-yes",
        platform="polymarket",
        price=Decimal(price),
        size=Decimal(size),
        side=side,
        actor_visibility=ActorVisibility.PUBLIC_WALLET,
        actor_uid=ACTOR_UID,
        transaction_uid=f"polymarket:tx/0x{uid}",
    )


def _pre_cutoff_fill(uid: str = "01") -> TradeFill:
    return _fill(
        uid,
        event_time=CASE_CUTOFF - timedelta(minutes=1),
        ingested_at=RECONSTRUCTED_AT - timedelta(minutes=3),
    )


def test_late_ingestion_is_descriptive_but_explicitly_prospective_ineligible() -> None:
    fill = _pre_cutoff_fill()
    report = build_polymarket_wallet_forensic_replay(
        _case(),
        reconstructed_at=RECONSTRUCTED_AT,
        coverage=_coverage(record_count=1),
        fills=(fill,),
    )

    assert report.reconstruction_status == "descriptive_complete"
    assert report.replay_mode == "hindsight_reconstructed"
    assert report.to_payload()["replay_mode"] == "hindsight_reconstructed"
    assert report.metrics is not None and report.metrics.observed_fill_count == 1
    assert report.fill_lineage[0].ingested_at > CASE_CUTOFF
    assert report.prospective.eligible is False
    assert report.prospective.source_was_available_at_cutoff is False
    assert report.prospective.prospectively_available_fill_count == 0
    assert "source_retrieved_after_historical_cutoff" in report.prospective.reason_codes
    assert "pre_cutoff_events_ingested_after_historical_cutoff" in report.prospective.reason_codes
    assert report.prospective.historical_alert_claim == "not_made"
    assert report.prospective.effectiveness_claim == "not_made"
    assert report.prospective.training_eligible is False


def test_event_cutoff_and_reconstruction_clock_exclude_ineligible_rows() -> None:
    included = _pre_cutoff_fill("01")
    post_cutoff = _fill(
        "02",
        event_time=CASE_CUTOFF + timedelta(seconds=1),
        ingested_at=RECONSTRUCTED_AT - timedelta(minutes=3),
    )
    post_reconstruction = _fill(
        "03",
        event_time=CASE_CUTOFF - timedelta(minutes=2),
        ingested_at=RECONSTRUCTED_AT + timedelta(seconds=1),
    )
    report = build_polymarket_wallet_forensic_replay(
        _case(),
        reconstructed_at=RECONSTRUCTED_AT,
        coverage=_coverage(record_count=3),
        fills=(post_reconstruction, post_cutoff, included),
    )

    assert report.reconstruction_status == "descriptive_complete"
    assert report.metrics is not None and report.metrics.observed_fill_count == 1
    assert [item.fill_uid for item in report.fill_lineage] == [included.fill_uid]
    assert report.excluded_counts.post_cutoff_event == 1
    assert report.excluded_counts.post_reconstruction_ingestion == 1


def test_incomplete_coverage_abstains_without_discarding_observed_lineage() -> None:
    fill = _pre_cutoff_fill()
    report = build_polymarket_wallet_forensic_replay(
        _case(),
        reconstructed_at=RECONSTRUCTED_AT,
        coverage=_coverage(record_count=1, status=CoverageStatus.PARTIAL),
        fills=(fill,),
    )

    assert report.reconstruction_status == "abstain"
    assert report.metrics is None
    assert report.abstention_reasons == ("incomplete_source_coverage",)
    assert report.fill_lineage[0].fill_uid == fill.fill_uid
    assert report.coverage.status == CoverageStatus.PARTIAL
    assert report.coverage.continuation == "bounded-continuation-fixture"


def test_report_serialization_and_hash_are_deterministic_across_input_order() -> None:
    first = _pre_cutoff_fill("01")
    second = _fill(
        "02",
        event_time=CASE_CUTOFF - timedelta(minutes=2),
        ingested_at=RECONSTRUCTED_AT - timedelta(minutes=3),
        market_suffix=f"{MARKET}-related",
        price="0.10",
        size="200",
    )
    kwargs = {
        "reconstructed_at": RECONSTRUCTED_AT,
        "coverage": _coverage(record_count=2),
    }
    ordered = build_polymarket_wallet_forensic_replay(_case(), fills=(first, second), **kwargs)
    reversed_input = build_polymarket_wallet_forensic_replay(_case(), fills=(second, first), **kwargs)

    assert ordered.analysis_input_sha256 == reversed_input.analysis_input_sha256
    assert ordered.report_sha256 == reversed_input.report_sha256
    assert ordered.canonical_bytes() == reversed_input.canonical_bytes()
    assert ordered.to_payload()["report_sha256"] == ordered.report_sha256


def test_analysis_hash_binds_full_canonical_fill_values() -> None:
    original = _pre_cutoff_fill("01")
    changed_price = _fill(
        "01",
        event_time=original.event_time,
        ingested_at=original.ingested_at,
        price="0.08",
    )
    common = {
        "reconstructed_at": RECONSTRUCTED_AT,
        "coverage": _coverage(record_count=1),
    }
    original_report = build_polymarket_wallet_forensic_replay(_case(), fills=(original,), **common)
    changed_report = build_polymarket_wallet_forensic_replay(_case(), fills=(changed_price,), **common)

    assert original_report.fill_lineage[0].fill_uid == changed_report.fill_lineage[0].fill_uid
    assert (
        original_report.fill_lineage[0].canonical_record_sha256
        != changed_report.fill_lineage[0].canonical_record_sha256
    )
    assert original_report.analysis_input_sha256 != changed_report.analysis_input_sha256
    assert original_report.report_uid != changed_report.report_uid


def test_later_legal_context_cannot_change_metrics_or_analysis_input() -> None:
    fill = _pre_cutoff_fill()

    def context(uid: str, summary: str) -> ExternalCaseAuditContext:
        return ExternalCaseAuditContext(
            context_uid=uid,
            mapped_actor_uid=ACTOR_UID,
            title="Official public case material",
            source_url=f"https://example.gov/{uid}",
            available_at=datetime(2026, 4, 23, 16, 0, tzinfo=UTC),
            legal_stage="allegation_pending",
            mapping_basis="official document identifies the public wallet",
            mapping_strength="officially_named_wallet",
            summary=summary,
        )

    common = {
        "reconstructed_at": RECONSTRUCTED_AT,
        "coverage": _coverage(record_count=1),
        "fills": (fill,),
    }
    first = build_polymarket_wallet_forensic_replay(
        _case(), external_audit_context=(context("audit-a", "Later allegation A."),), **common
    )
    second = build_polymarket_wallet_forensic_replay(
        _case(), external_audit_context=(context("audit-b", "Later allegation B."),), **common
    )

    assert first.metrics == second.metrics
    assert first.prospective == second.prospective
    assert first.analysis_input_sha256 == second.analysis_input_sha256
    assert first.report_uid == second.report_uid
    assert first.report_sha256 != second.report_sha256
    metric_keys = set(first.to_payload()["metrics"])
    assert "realized_performance" not in metric_keys
    assert "legal_stage" not in metric_keys
    assert "fraud_probability" not in first.to_payload()
    assert first.to_payload()["external_audit_context"][0]["analysis_use"] == "excluded_later_audit_context"


def test_source_retrieved_after_reconstruction_forces_abstention() -> None:
    fill = _pre_cutoff_fill()
    report = build_polymarket_wallet_forensic_replay(
        _case(),
        reconstructed_at=RECONSTRUCTED_AT,
        coverage=_coverage(
            record_count=1,
            retrieved_at=RECONSTRUCTED_AT + timedelta(seconds=1),
        ),
        fills=(fill,),
    )

    assert report.reconstruction_status == "abstain"
    assert report.metrics is None
    assert report.fill_lineage == ()
    assert report.excluded_counts.source_unavailable_at_reconstruction == 1
    assert "source_retrieved_after_reconstruction" in report.abstention_reasons


def test_query_filter_epochs_must_exactly_match_coverage_interval() -> None:
    with pytest.raises(ValueError, match="start must exactly match"):
        _coverage(record_count=1, query_start=2)
    with pytest.raises(ValueError, match="end must exactly match"):
        _coverage(
            record_count=1,
            query_end=int((RECONSTRUCTED_AT - timedelta(minutes=2)).timestamp()) - 1,
        )


def test_row_outside_declared_query_interval_forces_abstention() -> None:
    interval_start = CASE_CUTOFF - timedelta(minutes=2)
    inside = _fill(
        "01",
        event_time=CASE_CUTOFF - timedelta(minutes=1),
        ingested_at=RECONSTRUCTED_AT - timedelta(minutes=3),
    )
    outside = _fill(
        "02",
        event_time=CASE_CUTOFF - timedelta(minutes=3),
        ingested_at=RECONSTRUCTED_AT - timedelta(minutes=3),
    )
    report = build_polymarket_wallet_forensic_replay(
        _case(),
        reconstructed_at=RECONSTRUCTED_AT,
        coverage=_coverage(
            record_count=2,
            interval_start=interval_start,
        ),
        fills=(inside, outside),
    )

    assert report.reconstruction_status == "abstain"
    assert report.metrics is None
    assert report.excluded_counts.outside_query_interval == 1
    assert "row_outside_declared_query_interval" in report.abstention_reasons
