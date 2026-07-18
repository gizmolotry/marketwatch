"""Policy gates for independent actor-to-event graph evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from marketleak.evidence.normalize import utc_datetime
from marketleak.graph.claims import ClaimAdjudicationStatus, EvidenceBand, GraphClaim


DISALLOWED_SCORE_RELATIONSHIPS = frozenset(
    {
        "ASSOCIATED_WITH_ANOMALY",
        "FLAGGED_IN",
        "TRADED_IN",
        "OBSERVED_ON_CHAIN",
        "TRANSFERRED_TO",
    }
)

CONTROLLER_RELATIONSHIPS = frozenset({"CONTROLLED_BY", "VERIFIED_CONTROLLER"})
ROLE_RELATIONSHIPS = frozenset(
    {
        "HOLDS_ROLE",
        "MEMBER_OF_ACCESS_GROUP",
        "EMPLOYED_BY",
        "CONTRACTED_BY",
    }
)
EVENT_ACCESS_RELATIONSHIPS = frozenset(
    {"HAS_PLAUSIBLE_ACCESS_TO", "HAS_VERIFIED_ACCESS_TO"}
)


@dataclass(frozen=True)
class ClaimPolicyDecision:
    accepted: bool
    reason: str
    recency_factor: float
    band_weight: float


@dataclass(frozen=True)
class EvidenceGraphPolicy:
    max_path_edges: int = 4
    stale_after: timedelta = timedelta(days=365)
    hub_degree_threshold: int = 10
    hub_penalty_per_edge: float = 0.1
    minimum_source_diversity: int = 2
    corroboration_threshold: float = 0.20
    disallowed_relationships: frozenset[str] = DISALLOWED_SCORE_RELATIONSHIPS
    band_weights: dict[EvidenceBand, float] = field(
        default_factory=lambda: {
            EvidenceBand.A: 1.0,
            EvidenceBand.B: 0.85,
            EvidenceBand.C: 0.60,
            EvidenceBand.D: 0.30,
            EvidenceBand.E: 0.0,
        }
    )

    def relationship_sequence_valid(self, relationships: tuple[str, ...]) -> bool:
        """Require actor -> controller -> role/access -> event."""
        if len(relationships) != 3:
            return False
        return (
            relationships[0] in CONTROLLER_RELATIONSHIPS
            and relationships[1] in ROLE_RELATIONSHIPS
            and relationships[2] in EVENT_ACCESS_RELATIONSHIPS
        )

    def evaluate_claim(
        self,
        claim: GraphClaim,
        *,
        as_of: datetime,
        current_alert_uid: str | None = None,
    ) -> ClaimPolicyDecision:
        cutoff = utc_datetime(as_of, field_name="as_of")
        band_weight = self.band_weights.get(claim.evidence_band, 0.0)
        if claim.relationship in self.disallowed_relationships:
            return ClaimPolicyDecision(False, "relationship_not_score_bearing", 0.0, band_weight)
        if not claim.independent:
            return ClaimPolicyDecision(False, "claim_not_independent", 0.0, band_weight)
        if current_alert_uid is not None and claim.created_from_alert_uid == current_alert_uid:
            return ClaimPolicyDecision(False, "claim_created_by_current_alert", 0.0, band_weight)
        if not claim.evidence_uids or not claim.source_uid or not claim.source_span:
            return ClaimPolicyDecision(False, "missing_provenance", 0.0, band_weight)
        if claim.observed_at > cutoff:
            return ClaimPolicyDecision(False, "observed_after_as_of", 0.0, band_weight)
        if claim.valid_from is not None and claim.valid_from > cutoff:
            return ClaimPolicyDecision(False, "not_yet_valid", 0.0, band_weight)
        if claim.valid_to is not None and claim.valid_to < cutoff:
            return ClaimPolicyDecision(False, "validity_expired", 0.0, band_weight)
        if cutoff - claim.observed_at > self.stale_after:
            return ClaimPolicyDecision(False, "stale_claim", 0.0, band_weight)
        if claim.adjudication_status in {
            ClaimAdjudicationStatus.CONTRADICTED,
            ClaimAdjudicationStatus.REJECTED,
        } or claim.contradiction_count > 0:
            return ClaimPolicyDecision(False, "contradicted_or_rejected", 0.0, band_weight)
        if band_weight <= 0.0:
            return ClaimPolicyDecision(False, "evidence_band_not_score_bearing", 0.0, band_weight)

        age_seconds = max(0.0, (cutoff - claim.observed_at).total_seconds())
        horizon_seconds = max(1.0, self.stale_after.total_seconds())
        recency_factor = max(0.10, 1.0 - (age_seconds / horizon_seconds))
        return ClaimPolicyDecision(True, "accepted", recency_factor, band_weight)


__all__ = [
    "CONTROLLER_RELATIONSHIPS",
    "DISALLOWED_SCORE_RELATIONSHIPS",
    "EVENT_ACCESS_RELATIONSHIPS",
    "EvidenceGraphPolicy",
    "ROLE_RELATIONSHIPS",
]

