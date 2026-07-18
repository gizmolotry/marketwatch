"""Time-aware, provenance-aware scoring of independent access paths."""

from __future__ import annotations

import math
from datetime import datetime
from enum import Enum
from typing import Any

import networkx as nx
from pydantic import BaseModel, ConfigDict, Field

from marketleak.domain.enums import ActorEvidenceStatus
from marketleak.evidence.normalize import utc_datetime
from marketleak.graph.claims import GraphClaim, ObservedFillActor
from marketleak.graph.policy import EvidenceGraphPolicy


class EdgeContribution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    claim_uid: str
    subject_uid: str
    object_uid: str
    relationship: str
    source_uid: str | None = None
    evidence_uids: tuple[str, ...] = ()
    confidence: float = 0.0
    band_weight: float = 0.0
    recency_factor: float = 0.0
    effective_score: float = 0.0
    accepted: bool
    reason: str


class ScoredAccessPath(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    nodes: tuple[str, ...]
    relationships: tuple[str, ...]
    edge_contributions: tuple[EdgeContribution, ...]
    source_diversity: int = Field(ge=0)
    source_diversity_factor: float = Field(ge=0.0, le=1.0)
    hub_penalty_factor: float = Field(ge=0.0, le=1.0)
    score: float = Field(ge=0.0, le=1.0)
    accepted: bool
    rejection_reason: str | None = None


class ActorAccessAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: ActorEvidenceStatus
    actor_uid: str | None
    event_uid: str
    as_of: datetime
    observed_fill_uid: str | None
    best_score: float = Field(ge=0.0, le=1.0)
    evidence_uids: tuple[str, ...] = ()
    paths: tuple[ScoredAccessPath, ...] = ()
    reason_codes: tuple[str, ...] = ()
    not_proof_of_fraud: bool = True


def _edge_triples(graph: nx.MultiDiGraph, edge_path: list[Any]) -> list[tuple[str, str, Any]]:
    triples: list[tuple[str, str, Any]] = []
    for edge in edge_path:
        if len(edge) == 3:
            triples.append((edge[0], edge[1], edge[2]))
        else:
            triples.append((edge[0], edge[1], 0))
    return triples


def _rejected_unprovenanced(subject_uid: str, object_uid: str, data: dict[str, Any], reason: str) -> EdgeContribution:
    return EdgeContribution(
        claim_uid=str(data.get("claim_uid") or f"unprovenanced:{subject_uid}->{object_uid}"),
        subject_uid=subject_uid,
        object_uid=object_uid,
        relationship=str(data.get("relationship") or data.get("type") or "UNKNOWN").upper(),
        accepted=False,
        reason=reason,
    )


def _score_path(
    graph: nx.MultiDiGraph,
    edge_path: list[Any],
    *,
    as_of: datetime,
    policy: EvidenceGraphPolicy,
    current_alert_uid: str | None,
) -> ScoredAccessPath:
    triples = _edge_triples(graph, edge_path)
    nodes = tuple([triples[0][0], *[edge[1] for edge in triples]])
    contributions: list[EdgeContribution] = []
    claims: list[GraphClaim] = []

    for subject_uid, object_uid, key in triples:
        data = graph.get_edge_data(subject_uid, object_uid, key) or {}
        try:
            claim = GraphClaim.from_edge_data(subject_uid, object_uid, data)
        except Exception:
            contributions.append(_rejected_unprovenanced(subject_uid, object_uid, data, "missing_or_invalid_provenance"))
            continue
        claims.append(claim)
        decision = policy.evaluate_claim(claim, as_of=as_of, current_alert_uid=current_alert_uid)
        effective = claim.confidence * decision.band_weight * decision.recency_factor if decision.accepted else 0.0
        contributions.append(
            EdgeContribution(
                claim_uid=claim.claim_uid,
                subject_uid=subject_uid,
                object_uid=object_uid,
                relationship=claim.relationship,
                source_uid=claim.source_uid,
                evidence_uids=claim.evidence_uids,
                confidence=claim.confidence,
                band_weight=decision.band_weight,
                recency_factor=decision.recency_factor,
                effective_score=effective,
                accepted=decision.accepted,
                reason=decision.reason,
            )
        )

    relationships = tuple(item.relationship for item in contributions)
    invalid_edge = next((item for item in contributions if not item.accepted), None)
    sequence_valid = policy.relationship_sequence_valid(relationships)
    sources = {claim.source_uid for claim in claims if claim.source_uid}
    diversity_factor = min(1.0, len(sources) / max(1, policy.minimum_source_diversity))
    intermediate_nodes = nodes[1:-1]
    excess_hub_degree = sum(
        max(0, graph.degree(node) - policy.hub_degree_threshold) for node in intermediate_nodes
    )
    hub_factor = 1.0 / (1.0 + excess_hub_degree * policy.hub_penalty_per_edge)

    if invalid_edge is not None:
        accepted = False
        rejection_reason = invalid_edge.reason
        score = 0.0
    elif not sequence_valid:
        accepted = False
        rejection_reason = "required_controller_role_access_sequence_missing"
        score = 0.0
    elif len(sources) < policy.minimum_source_diversity:
        accepted = False
        rejection_reason = "insufficient_source_diversity"
        score = 0.0
    else:
        accepted = True
        rejection_reason = None
        edge_scores = [max(item.effective_score, 1e-12) for item in contributions]
        geometric_mean = math.prod(edge_scores) ** (1.0 / len(edge_scores))
        score = min(1.0, geometric_mean * diversity_factor * hub_factor)

    return ScoredAccessPath(
        nodes=nodes,
        relationships=relationships,
        edge_contributions=tuple(contributions),
        source_diversity=len(sources),
        source_diversity_factor=diversity_factor,
        hub_penalty_factor=hub_factor,
        score=score,
        accepted=accepted,
        rejection_reason=rejection_reason,
    )


def score_actor_access(
    graph: nx.MultiDiGraph,
    *,
    observed_actor: ObservedFillActor | None,
    event_uid: str,
    as_of: datetime,
    policy: EvidenceGraphPolicy | None = None,
    current_alert_uid: str | None = None,
) -> ActorAccessAssessment:
    """Score corroborated access starting only from a fill-observed actor."""
    cutoff = utc_datetime(as_of, field_name="as_of")
    policy = policy or EvidenceGraphPolicy()
    if observed_actor is None:
        return ActorAccessAssessment(
            status=ActorEvidenceStatus.NO_ACTOR_DATA,
            actor_uid=None,
            event_uid=event_uid,
            as_of=cutoff,
            observed_fill_uid=None,
            best_score=0.0,
            reason_codes=("observed_fill_actor_required",),
        )
    if observed_actor.observed_at > cutoff:
        return ActorAccessAssessment(
            status=ActorEvidenceStatus.INSUFFICIENT,
            actor_uid=observed_actor.actor_uid,
            event_uid=event_uid,
            as_of=cutoff,
            observed_fill_uid=observed_actor.fill_uid,
            best_score=0.0,
            reason_codes=("fill_observed_after_as_of",),
        )
    if observed_actor.actor_uid not in graph or event_uid not in graph:
        return ActorAccessAssessment(
            status=ActorEvidenceStatus.INSUFFICIENT,
            actor_uid=observed_actor.actor_uid,
            event_uid=event_uid,
            as_of=cutoff,
            observed_fill_uid=observed_actor.fill_uid,
            best_score=0.0,
            reason_codes=("actor_or_event_missing_from_independent_graph",),
        )

    scored_paths: list[ScoredAccessPath] = []
    try:
        edge_paths = nx.all_simple_edge_paths(
            graph,
            observed_actor.actor_uid,
            event_uid,
            cutoff=policy.max_path_edges,
        )
        for edge_path in edge_paths:
            scored_paths.append(
                _score_path(
                    graph,
                    edge_path,
                    as_of=cutoff,
                    policy=policy,
                    current_alert_uid=current_alert_uid,
                )
            )
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        scored_paths = []

    scored_paths.sort(key=lambda item: (-item.score, item.nodes, item.relationships))
    accepted = [path for path in scored_paths if path.accepted]
    best_score = max((path.score for path in accepted), default=0.0)
    corroborated = [path for path in accepted if path.score >= policy.corroboration_threshold]
    if corroborated:
        status = ActorEvidenceStatus.CORROBORATED_ACCESS_SIGNAL
        reasons = ("independent_controller_role_access_path",)
    elif accepted:
        status = ActorEvidenceStatus.CONTEXT_ONLY
        reasons = ("independent_path_below_corroboration_threshold",)
    else:
        status = ActorEvidenceStatus.INSUFFICIENT
        reasons = ("no_complete_independent_access_path",)

    evidence_uids = tuple(
        sorted(
            {
                evidence_uid
                for path in accepted
                for contribution in path.edge_contributions
                for evidence_uid in contribution.evidence_uids
            }
        )
    )
    return ActorAccessAssessment(
        status=status,
        actor_uid=observed_actor.actor_uid,
        event_uid=event_uid,
        as_of=cutoff,
        observed_fill_uid=observed_actor.fill_uid,
        best_score=best_score,
        evidence_uids=evidence_uids,
        paths=tuple(scored_paths),
        reason_codes=reasons,
    )


__all__ = [
    "ActorAccessAssessment",
    "EdgeContribution",
    "ScoredAccessPath",
    "score_actor_access",
]

