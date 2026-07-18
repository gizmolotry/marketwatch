from datetime import datetime, timedelta, timezone

import networkx as nx
import pytest

from marketleak.domain import ActorEvidenceStatus
from marketleak.graph.claims import (
    ClaimAdjudicationStatus,
    EvidenceBand,
    GraphClaim,
    ObservedFillActor,
    add_claim,
)
from marketleak.graph.scoring import score_actor_access


UTC = timezone.utc
AS_OF = datetime(2026, 3, 1, tzinfo=UTC)
ACTOR = "wallet:actor"
CONTROLLER = "person:controller"
ACCESS_GROUP = "access:deal-team"
EVENT = "event:acme-merger"


def _claim(
    uid,
    subject,
    object_,
    relationship,
    *,
    source="source:one",
    observed_at=AS_OF - timedelta(days=1),
    status=ClaimAdjudicationStatus.UNREVIEWED,
    contradiction_count=0,
    independent=True,
    alert_uid=None,
):
    return GraphClaim(
        claim_uid=uid,
        subject_uid=subject,
        object_uid=object_,
        relationship=relationship,
        evidence_uids=(f"evidence:{uid.split(':')[-1]}",),
        source_uid=source,
        source_span="lines 1-2",
        observed_at=observed_at,
        confidence=0.9,
        evidence_band=EvidenceBand.A,
        source_revision="rev-1",
        parser_version="claim-parser-v1",
        adjudication_status=status,
        contradiction_count=contradiction_count,
        independent=independent,
        created_from_alert_uid=alert_uid,
    )


def _actor():
    return ObservedFillActor(
        actor_uid=ACTOR,
        fill_uid="fill:1",
        market_uid="venue:market-1",
        observed_at=AS_OF - timedelta(hours=2),
        evidence_uid="evidence:fill-1",
        raw_artifact_uid="raw:fill-1",
    )


def _valid_graph():
    graph = nx.MultiDiGraph()
    add_claim(graph, _claim("claim:controller", ACTOR, CONTROLLER, "CONTROLLED_BY", source="source:identity"))
    add_claim(graph, _claim("claim:role", CONTROLLER, ACCESS_GROUP, "HOLDS_ROLE", source="source:employment"))
    add_claim(
        graph,
        _claim(
            "claim:access",
            ACCESS_GROUP,
            EVENT,
            "HAS_PLAUSIBLE_ACCESS_TO",
            source="source:event-record",
        ),
    )
    return graph


def test_valid_independent_controller_role_access_path_corroborates():
    result = score_actor_access(_valid_graph(), observed_actor=_actor(), event_uid=EVENT, as_of=AS_OF)

    assert result.status == ActorEvidenceStatus.CORROBORATED_ACCESS_SIGNAL
    assert result.best_score > 0.2
    assert len(result.evidence_uids) == 3
    path = result.paths[0]
    assert path.accepted is True
    assert path.relationships == ("CONTROLLED_BY", "HOLDS_ROLE", "HAS_PLAUSIBLE_ACCESS_TO")
    assert len(path.edge_contributions) == 3
    assert all(item.accepted for item in path.edge_contributions)


def test_removing_required_access_edge_breaks_corroboration():
    graph = _valid_graph()
    graph.remove_edge(ACCESS_GROUP, EVENT, "claim:access")

    result = score_actor_access(graph, observed_actor=_actor(), event_uid=EVENT, as_of=AS_OF)

    assert result.status == ActorEvidenceStatus.INSUFFICIENT
    assert result.best_score == 0.0


def test_current_alert_self_edge_is_not_score_bearing():
    graph = nx.MultiDiGraph()
    add_claim(
        graph,
        _claim(
            "claim:self-alert",
            ACTOR,
            EVENT,
            "ASSOCIATED_WITH_ANOMALY",
            independent=False,
            alert_uid="alert:current",
        ),
    )

    result = score_actor_access(
        graph,
        observed_actor=_actor(),
        event_uid=EVENT,
        as_of=AS_OF,
        current_alert_uid="alert:current",
    )

    assert result.status == ActorEvidenceStatus.INSUFFICIENT
    assert result.best_score == 0.0
    contribution = result.paths[0].edge_contributions[0]
    assert contribution.accepted is False
    assert contribution.reason == "relationship_not_score_bearing"


def test_unprovenanced_edge_is_excluded():
    graph = _valid_graph()
    graph.remove_edge(ACTOR, CONTROLLER, "claim:controller")
    graph.add_edge(ACTOR, CONTROLLER, key="raw", type="CONTROLLED_BY")

    result = score_actor_access(graph, observed_actor=_actor(), event_uid=EVENT, as_of=AS_OF)

    assert result.status == ActorEvidenceStatus.INSUFFICIENT
    assert result.best_score == 0.0
    assert result.paths[0].edge_contributions[0].reason == "missing_or_invalid_provenance"


@pytest.mark.parametrize(
    "claim_kwargs,expected_reason",
    [
        ({"observed_at": AS_OF + timedelta(minutes=1)}, "observed_after_as_of"),
        ({"observed_at": AS_OF - timedelta(days=366)}, "stale_claim"),
        ({"status": ClaimAdjudicationStatus.CONTRADICTED}, "contradicted_or_rejected"),
        ({"contradiction_count": 1}, "contradicted_or_rejected"),
    ],
)
def test_post_cutoff_stale_and_contradicted_claims_are_excluded(claim_kwargs, expected_reason):
    graph = _valid_graph()
    graph.remove_edge(ACTOR, CONTROLLER, "claim:controller")
    add_claim(
        graph,
        _claim(
            "claim:controller-rejected",
            ACTOR,
            CONTROLLER,
            "CONTROLLED_BY",
            source="source:identity",
            **claim_kwargs,
        ),
    )

    result = score_actor_access(graph, observed_actor=_actor(), event_uid=EVENT, as_of=AS_OF)

    assert result.status == ActorEvidenceStatus.INSUFFICIENT
    assert result.best_score == 0.0
    assert result.paths[0].edge_contributions[0].reason == expected_reason


def test_actor_context_cannot_start_without_an_observed_fill_actor():
    result = score_actor_access(_valid_graph(), observed_actor=None, event_uid=EVENT, as_of=AS_OF)

    assert result.status == ActorEvidenceStatus.NO_ACTOR_DATA
    assert result.reason_codes == ("observed_fill_actor_required",)

