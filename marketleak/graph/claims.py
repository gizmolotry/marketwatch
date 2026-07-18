"""Provenance-bearing graph claims for actor-access analysis."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

import networkx as nx
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from marketleak.evidence.normalize import utc_datetime


class EvidenceBand(str, Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"


class ClaimAdjudicationStatus(str, Enum):
    UNREVIEWED = "UNREVIEWED"
    CONFIRMED = "CONFIRMED"
    CONTRADICTED = "CONTRADICTED"
    REJECTED = "REJECTED"


class GraphClaim(BaseModel):
    """One independently sourced relationship, never an inferred graph fact."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    claim_uid: str = Field(min_length=1)
    subject_uid: str = Field(min_length=1)
    object_uid: str = Field(min_length=1)
    relationship: str = Field(min_length=1)
    evidence_uids: tuple[str, ...] = Field(min_length=1)
    source_uid: str = Field(min_length=1)
    source_span: str = Field(min_length=1)
    observed_at: datetime
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_band: EvidenceBand
    source_revision: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    adjudication_status: ClaimAdjudicationStatus = ClaimAdjudicationStatus.UNREVIEWED
    contradiction_count: int = Field(default=0, ge=0)
    independent: bool = True
    created_from_alert_uid: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("relationship")
    @classmethod
    def _normalize_relationship(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("observed_at", "valid_from", "valid_to", mode="before")
    @classmethod
    def _timestamps_are_utc(cls, value, info):
        return utc_datetime(value, field_name=info.field_name, allow_none=info.field_name in {"valid_from", "valid_to"})

    @model_validator(mode="after")
    def _valid_claim(self):
        if self.subject_uid == self.object_uid:
            raise ValueError("graph claims cannot be self-referential")
        if self.valid_from is not None and self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be after valid_from")
        if not self.independent and self.created_from_alert_uid is None:
            raise ValueError("non-independent claims must identify the creating alert")
        return self

    def edge_attributes(self) -> dict[str, Any]:
        data = self.model_dump()
        data["type"] = self.relationship
        data["claim"] = self
        return data

    @classmethod
    def from_edge_data(cls, subject_uid: str, object_uid: str, data: dict[str, Any]) -> "GraphClaim":
        embedded = data.get("claim")
        if isinstance(embedded, cls):
            return embedded
        values = dict(data)
        values.pop("claim", None)
        values.pop("type", None)
        values.setdefault("subject_uid", subject_uid)
        values.setdefault("object_uid", object_uid)
        values.setdefault("relationship", data.get("relationship") or data.get("type"))
        return cls.model_validate(values)


class ObservedFillActor(BaseModel):
    """Actor entry point proven by a venue fill, not market metadata or env vars."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    actor_uid: str = Field(min_length=1)
    fill_uid: str = Field(min_length=1)
    market_uid: str = Field(min_length=1)
    observed_at: datetime
    evidence_uid: str = Field(min_length=1)
    raw_artifact_uid: str = Field(min_length=1)

    @field_validator("observed_at", mode="before")
    @classmethod
    def _timestamp_is_utc(cls, value):
        return utc_datetime(value, field_name="observed_at")


def add_claim(graph: nx.MultiDiGraph, claim: GraphClaim) -> None:
    """Add a claim while preserving parallel, revision-specific edges."""
    graph.add_node(claim.subject_uid)
    graph.add_node(claim.object_uid)
    graph.add_edge(
        claim.subject_uid,
        claim.object_uid,
        key=claim.claim_uid,
        **claim.edge_attributes(),
    )


__all__ = [
    "ClaimAdjudicationStatus",
    "EvidenceBand",
    "GraphClaim",
    "ObservedFillActor",
    "add_claim",
]

