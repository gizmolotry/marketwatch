from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from marketleak.cli_v3 import main as cli_main
from marketleak.domain.enums import CoverageStatus
from marketleak.labels.schemas import LabelTarget
from marketleak.multimodal.event_store import EventMemoryStore
from marketleak.multimodal.orchestration import (
    BaselinePlan,
    FeatureAssemblyInput,
    assess_readiness,
    build_as_of_assembly,
    train_baseline_candidate,
)
from marketleak.multimodal.schemas import (
    MarketStateSlice,
    MissingnessStatus,
    Modality,
    ModalityMissingness,
    Provenance,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


def fact(*, uid: str, event_minutes: int, available_minutes: int) -> MarketStateSlice:
    source = "source:venue"
    return MarketStateSlice(
        event_uid=uid,
        event_time=T0 + timedelta(minutes=event_minutes),
        ingested_at=T0 + timedelta(minutes=available_minutes),
        provenance=Provenance(
            source_uid=source,
            raw_artifact_uid=f"raw:event-{event_minutes}",
            parser_version="phase15-test",
            content_hash="a" * 64,
            retrieved_at=T0,
            source_url="https://example.test/raw",
        ),
        reliability=SourceReliability(
            source_uid=source,
            source_class=SourceClass.OFFICIAL_VENUE,
            tier=ReliabilityTier.HIGH,
            score=Decimal("1"),
            assessed_at=T0,
            rationale="official source",
        ),
        missingness=(ModalityMissingness(modality=Modality.MARKET_STATE, status=MissingnessStatus.OBSERVED),),
        market_uid="market:btc",
        outcome_uid="outcome:btc-up",
        window_starts_at=T0 + timedelta(minutes=event_minutes - 1),
        window_ends_at=T0 + timedelta(minutes=event_minutes),
        last_trade_price=Decimal("0.55"),
        trade_notional=Decimal("10"),
        fill_count=1,
    )


def feature(*, uid: str, event_uid: str, coverage: CoverageStatus = CoverageStatus.COMPLETE, context: bool = True):
    return FeatureAssemblyInput(
        feature_uid=uid,
        event_uid=event_uid,
        event_cluster_uid="cluster:btc",
        market_uid="market:btc",
        platform="polymarket",
        category="crypto",
        features={"price_change": Decimal("0.2"), "volume": Decimal("10")},
        coverage_status=coverage,
        context_complete=context,
    )


def plan() -> BaselinePlan:
    return BaselinePlan(
        target=LabelTarget.ACTIVITY_A,
        train_end=T0 + timedelta(days=1),
        validation_end=T0 + timedelta(days=2),
        analyst_capacity=5,
    )


def test_as_of_assembly_excludes_future_event_and_its_features():
    past = fact(uid="event:past", event_minutes=1, available_minutes=2)
    future = fact(uid="event:future", event_minutes=30, available_minutes=31)
    assembly = build_as_of_assembly(
        EventMemoryStore([future, past]),
        [feature(uid="feature:past", event_uid="event:past"), feature(uid="feature:future", event_uid="event:future")],
        as_of=T0 + timedelta(minutes=10),
    )

    assert [row.feature.feature_uid for row in assembly.rows] == ["feature:past"]
    assert [row.feature_uid for row in assembly.excluded] == ["feature:future"]
    assert assembly.event_snapshot.manifest.event_uids == ("event:past",)


def test_label_context_and_coverage_gates_block_training():
    observed = fact(uid="event:past", event_minutes=1, available_minutes=2)
    assembly = build_as_of_assembly(
        EventMemoryStore([observed]),
        [feature(uid="feature:past", event_uid="event:past", coverage=CoverageStatus.PARTIAL, context=False)],
        as_of=T0 + timedelta(minutes=10),
    )

    readiness = assess_readiness(assembly, plan=plan())

    assert readiness.status == "not_ready"
    assert readiness.training_allowed is False
    assert readiness.serving_allowed is False
    assert {"coverage_incomplete", "context_incomplete", "human_labels_missing"} <= set(readiness.reason_codes)


def test_candidate_remains_unapproved_when_not_ready():
    observed = fact(uid="event:past", event_minutes=1, available_minutes=2)
    assembly = build_as_of_assembly(
        EventMemoryStore([observed]),
        [feature(uid="feature:past", event_uid="event:past")],
        as_of=T0 + timedelta(minutes=10),
    )

    result = train_baseline_candidate(assembly, plan=plan())

    assert result.status == "not_ready"
    assert result.candidate.status == "not_ready"
    assert result.candidate.approved is False
    assert result.candidate.published is False
    assert result.evaluation == ()


def test_cli_emits_stable_json_not_ready_status(tmp_path, capsys):
    observed = fact(uid="event:past", event_minutes=1, available_minutes=2)
    input_path = tmp_path / "phase15.json"
    input_path.write_text(
        json.dumps(
            {
                "events": [observed.model_dump(mode="json")],
                "features": [feature(uid="feature:past", event_uid="event:past").model_dump(mode="json")],
                "plan": plan().model_dump(mode="json"),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    argv = ["train-baseline", "--input", str(input_path), "--as-of", "2026-07-13T12:10:00Z"]

    assert cli_main(argv) == 0
    first = capsys.readouterr().out
    assert cli_main(argv) == 0
    second = capsys.readouterr().out
    payload = json.loads(first)

    assert first == second
    assert payload["status"] == "not_ready"
    assert payload["candidate"]["approved"] is False
    assert payload["candidate"]["published"] is False
    assert "fraud" not in first.lower()
    assert "insider" not in first.lower()
