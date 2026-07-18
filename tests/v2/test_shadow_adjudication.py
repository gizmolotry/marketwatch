from datetime import datetime, timedelta, timezone

import pytest

from marketleak.shadow import AdjudicationStore, FrozenRunMutationError


UTC = timezone.utc
NOW = datetime(2026, 6, 1, tzinfo=UTC)


def test_adjudications_are_append_only_and_supersede_explicitly(tmp_path):
    store = AdjudicationStore(tmp_path)
    first = store.append(
        record_uid="window:1",
        label="unknown",
        rationale="Awaiting source verification.",
        adjudicator_uid="analyst:one",
        adjudicated_at=NOW,
    )
    second = store.append(
        record_uid="window:1",
        label="negative",
        rationale="Public announcement was prospectively observed before the move.",
        evidence_uids=("evidence:announcement",),
        adjudicator_uid="analyst:one",
        adjudicated_at=NOW + timedelta(hours=1),
        supersedes_uid=first.adjudication_uid,
    )

    records = store.verify()
    assert records == (first, second)
    assert second.supersedes_uid == first.adjudication_uid
    assert records[0].label == "unknown"


def test_supersession_must_reference_same_record_and_can_happen_once(tmp_path):
    store = AdjudicationStore(tmp_path)
    first = store.append(
        record_uid="window:1",
        label="unknown",
        rationale="Initial review.",
        adjudicator_uid="analyst:one",
        adjudicated_at=NOW,
    )

    with pytest.raises(FrozenRunMutationError, match="same shadow record"):
        store.append(
            record_uid="window:2",
            label="negative",
            rationale="Wrong target.",
            adjudicator_uid="analyst:one",
            adjudicated_at=NOW,
            supersedes_uid=first.adjudication_uid,
        )

    replacement = store.append(
        record_uid="window:1",
        label="insufficient",
        rationale="Coverage was insufficient.",
        adjudicator_uid="analyst:one",
        adjudicated_at=NOW + timedelta(minutes=1),
        supersedes_uid=first.adjudication_uid,
    )
    assert replacement.supersedes_uid == first.adjudication_uid

    with pytest.raises(FrozenRunMutationError, match="already been superseded"):
        store.append(
            record_uid="window:1",
            label="negative",
            rationale="Second replacement is forbidden.",
            adjudicator_uid="analyst:two",
            adjudicated_at=NOW + timedelta(minutes=2),
            supersedes_uid=first.adjudication_uid,
        )

