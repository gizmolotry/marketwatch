"""Deterministic evidence matching with explicit audit output."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable, Mapping

from pydantic import BaseModel, ConfigDict, Field

from marketleak.evidence.normalize import NormalizedEvidence, utc_datetime


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]*", re.IGNORECASE)
_STOPWORDS = frozenset(
    {
        "a", "an", "and", "are", "at", "be", "by", "did", "do", "does",
        "for", "from", "happen", "in", "is", "market", "no", "of", "on",
        "or", "the", "to", "was", "were", "what", "when", "where", "which",
        "who", "will", "win", "wins", "with", "yes",
    }
)


def _terms(text: str) -> frozenset[str]:
    return frozenset(
        token.casefold()
        for token in _TOKEN_RE.findall(text or "")
        if len(token) >= 3 and token.casefold() not in _STOPWORDS
    )


class EvidenceMatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence: NormalizedEvidence
    lexical_score: float = Field(ge=0.0, le=1.0)
    upstream_score: float | None = None
    relevance_score: float = Field(ge=0.0, le=1.0)
    overlapping_terms: tuple[str, ...] = ()
    relevant: bool
    explicit_verified: bool
    eligible_as_of: bool
    reasons: tuple[str, ...]


class MatchResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    market_uid: str
    market_terms: tuple[str, ...]
    as_of: datetime
    candidates: tuple[EvidenceMatch, ...]

    @property
    def relevant_candidates(self) -> tuple[EvidenceMatch, ...]:
        return tuple(item for item in self.candidates if item.relevant and item.eligible_as_of)


class EvidenceMatcher:
    """Match evidence using content/entity overlap or explicit verification.

    Search strategy names and query text are deliberately ignored.  An exact
    query can retrieve a document, but it cannot prove that document is about
    the market.
    """

    def __init__(self, *, minimum_overlap_terms: int = 2, minimum_overlap_ratio: float = 0.25):
        if minimum_overlap_terms < 1:
            raise ValueError("minimum_overlap_terms must be positive")
        if not 0.0 <= minimum_overlap_ratio <= 1.0:
            raise ValueError("minimum_overlap_ratio must be in [0, 1]")
        self.minimum_overlap_terms = minimum_overlap_terms
        self.minimum_overlap_ratio = minimum_overlap_ratio

    def match(
        self,
        *,
        market_uid: str,
        market_text: str,
        candidates: Iterable[NormalizedEvidence],
        as_of: datetime,
        explicit_verified_evidence_uids: Iterable[str] = (),
        upstream_scores: Mapping[str, float] | None = None,
    ) -> MatchResult:
        cutoff = utc_datetime(as_of, field_name="as_of")
        market_terms = _terms(market_text)
        verified_uids = set(explicit_verified_evidence_uids)
        scores = dict(upstream_scores or {})
        matches: list[EvidenceMatch] = []

        for raw_candidate in candidates:
            evidence = (
                raw_candidate
                if isinstance(raw_candidate, NormalizedEvidence)
                else NormalizedEvidence.model_validate(raw_candidate)
            )
            evidence_terms = _terms(evidence.searchable_text)
            overlap = tuple(sorted(market_terms.intersection(evidence_terms)))
            denominator = max(1, len(market_terms))
            lexical_score = min(1.0, len(overlap) / denominator)
            metadata_market_uids = {
                str(value)
                for value in evidence.metadata.get("verified_market_uids", ())
            }
            explicit_verified = evidence.evidence_uid in verified_uids or market_uid in metadata_market_uids
            required_count = min(self.minimum_overlap_terms, max(1, len(market_terms)))
            deterministic_overlap = (
                len(overlap) >= required_count
                and lexical_score >= self.minimum_overlap_ratio
            )
            relevant = explicit_verified or deterministic_overlap
            eligible_as_of = evidence.first_seen_at <= cutoff and evidence.retrieved_at <= cutoff

            reasons: list[str] = []
            if explicit_verified:
                reasons.append("explicit_market_verification")
            if deterministic_overlap:
                reasons.append("deterministic_entity_overlap")
            if not relevant:
                reasons.append("insufficient_entity_overlap")
            if not eligible_as_of:
                reasons.append("not_observed_by_as_of")
            # A query strategy can be retained in metadata for audit, but it is
            # never added as a relevance reason.

            upstream_score = scores.get(evidence.evidence_uid)
            matches.append(
                EvidenceMatch(
                    evidence=evidence,
                    lexical_score=lexical_score,
                    upstream_score=upstream_score,
                    relevance_score=1.0 if explicit_verified else lexical_score,
                    overlapping_terms=overlap,
                    relevant=relevant,
                    explicit_verified=explicit_verified,
                    eligible_as_of=eligible_as_of,
                    reasons=tuple(reasons),
                )
            )

        matches.sort(
            key=lambda item: (
                not item.relevant,
                not item.eligible_as_of,
                -item.relevance_score,
                item.evidence.first_seen_at,
                item.evidence.evidence_uid,
            )
        )
        return MatchResult(
            market_uid=market_uid,
            market_terms=tuple(sorted(market_terms)),
            as_of=cutoff,
            candidates=tuple(matches),
        )

