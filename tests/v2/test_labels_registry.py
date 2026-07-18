from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from marketleak.labels import (
    Adjudication,
    Case,
    CaseOrigin,
    HardNegativeType,
    LabelRegistry,
    LabelTarget,
    LabelValue,
    MechanicEvent,
    generate_hard_negatives,
)


def internal_case() -> Case:
    return Case(
        case_uid="labels:test-case",
        title="Reviewed case",
        origin=CaseOrigin.INTERNAL_ADJUDICATION,
        description="A test adjudication with an exact market mapping.",
        source_urls=("https://example.test/case",),
        platform="kalshi",
        category="test",
        market_uids=("kalshi:market/test",),
        exact_market_mapping_available=True,
        exact_actor_mapping_available=False,
        training_eligible=True,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_registry_is_append_only_and_supersession_is_target_specific(tmp_path):
    registry = LabelRegistry(tmp_path / "labels.jsonl")
    registry.append(internal_case())
    first = Adjudication(
        adjudication_uid="labels:test-A-v1",
        case_uid="labels:test-case",
        target=LabelTarget.ACTIVITY_A,
        value=LabelValue.UNKNOWN,
        rationale="Initial evidence was incomplete.",
        adjudicator="reviewer-1",
        adjudicated_at=datetime(2026, 1, 2, tzinfo=UTC),
        training_eligible=False,
    )
    second = Adjudication(
        adjudication_uid="labels:test-A-v2",
        case_uid="labels:test-case",
        target=LabelTarget.ACTIVITY_A,
        value=LabelValue.NEGATIVE,
        rationale="A completed review found normal activity.",
        adjudicator="reviewer-2",
        adjudicated_at=datetime(2026, 1, 3, tzinfo=UTC),
        training_eligible=True,
        supersedes_uid=first.adjudication_uid,
    )
    registry.append(first)
    registry.append(second)

    assert len(registry.adjudications()) == 2
    assert registry.current_adjudication("labels:test-case", LabelTarget.ACTIVITY_A) == second
    with pytest.raises(ValueError, match="already exists"):
        registry.append(second)


def test_unknown_and_unmapped_cannot_be_training_labels():
    with pytest.raises(ValueError, match="cannot be training eligible"):
        Adjudication(
            adjudication_uid="labels:bad",
            case_uid="labels:test-case",
            target=LabelTarget.ACTOR_EVIDENCE_C,
            value=LabelValue.UNMAPPED,
            rationale="No exact mapping.",
            adjudicator="reviewer",
            adjudicated_at=datetime(2026, 1, 1, tzinfo=UTC),
            training_eligible=True,
        )
    assert LabelValue.UNKNOWN != LabelValue.NEGATIVE
    assert LabelValue.UNMAPPED.is_binary is False


def test_all_eight_hard_negative_generators_are_covered_and_non_trainable():
    event = MechanicEvent(
        event_uid="mechanic-1",
        event_cluster_uid="cluster-1",
        market_uid="kalshi:market/test",
        starts_at=datetime(2026, 1, 1, tzinfo=UTC),
        ends_at=datetime(2026, 1, 1, 1, tzinfo=UTC),
        evidence_refs=("fixture:mechanic",),
    )
    windows = generate_hard_negatives(event)

    assert {window.hard_negative_type for window in windows} == set(HardNegativeType)
    assert all(window.synthetic and not window.training_eligible for window in windows)
    assert all(window.sampling_weight > Decimal(0) for window in windows)
    assert all(
        window.labels[LabelTarget.ACTOR_EVIDENCE_C] == LabelValue.UNKNOWN
        for window in windows
    )


def test_public_cftc_registry_is_transparent_and_not_trainable():
    root = Path(__file__).resolve().parents[2]
    registry = LabelRegistry(root / "datasets" / "labels" / "public_cases_v1.jsonl")

    cases = registry.cases()
    adjudications = registry.adjudications()

    assert len(cases) == 2
    assert all(case.origin == CaseOrigin.PUBLIC_ADJUDICATION for case in cases)
    assert all(not case.training_eligible for case in cases)
    assert all(not case.market_uids and not case.actor_uids for case in cases)
    assert all(not adjudication.training_eligible for adjudication in adjudications)
    assert {item.target for item in adjudications} == set(LabelTarget)

