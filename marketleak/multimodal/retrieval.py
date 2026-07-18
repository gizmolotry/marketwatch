"""Deterministic, exact retrieval for Phase 15 multimodal event memory.

This module is intentionally a *local* retrieval primitive.  It accepts
already-produced embeddings, never calls a network service or an LLM, and uses
one ``faiss.IndexFlatIP`` shard per modality.  Approximate nearest-neighbour
indexes are deliberately unavailable until their recall and safety behaviour
has been demonstrated against this exact reference implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
import hashlib
from typing import Annotated, Any, Iterable, Literal

import faiss
import numpy as np
from pydantic import Field, field_validator, model_validator

from marketleak.domain.common import NonEmptyStr, StableUID
from marketleak.multimodal.schemas import (
    Modality,
    Phase15Model,
    Provenance,
    SourceReliability,
    canonical_hash,
)


RETRIEVAL_SCHEMA_VERSION = "15.0.0-retrieval-flat-v1"
Sha256 = str

# An explicit policy constant makes it impossible to mistake an ANN experiment
# for the production/default retrieval path.
ANN_STATUS = "unavailable_pending"
ANN_UNAVAILABLE_REASON = (
    "ANN is intentionally unavailable: promote HNSW/IVF-PQ only after it meets "
    "the exact Flat Recall@K, temporal, provenance, calibration, and missing-modality gates."
)


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _require_sha256(value: str, *, field_name: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 hex digest")
    return normalized


def _normalized_embedding(value: Iterable[float], *, expected_dimension: int | None = None) -> np.ndarray:
    """Validate and L2-normalize one vector using a deterministic float32 path."""

    vector = np.asarray(tuple(value), dtype=np.float32)
    if vector.ndim != 1 or vector.shape[0] == 0:
        raise ValueError("embedding must be a non-empty one-dimensional vector")
    if expected_dimension is not None and vector.shape[0] != expected_dimension:
        raise ValueError(f"embedding dimension {vector.shape[0]} does not match expected {expected_dimension}")
    if not np.isfinite(vector).all():
        raise ValueError("embedding must contain only finite values")
    norm = float(np.linalg.norm(vector))
    if norm <= 0.0 or not np.isfinite(norm):
        raise ValueError("embedding must have a finite, non-zero norm")
    return np.ascontiguousarray(vector / norm, dtype=np.float32)


def embedding_hash(value: Iterable[float]) -> str:
    """Hash the normalized float32 vector, not a formatter-dependent JSON list."""

    vector = _normalized_embedding(value)
    dimension = np.asarray([vector.shape[0]], dtype="<u4").tobytes()
    return hashlib.sha256(dimension + vector.astype("<f4", copy=False).tobytes()).hexdigest()


class CoverageState(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class ANNState(str, Enum):
    UNAVAILABLE_PENDING = ANN_STATUS


class EncoderProvenance(Phase15Model):
    """Identity and immutable artifact lineage for an embedding encoder."""

    encoder_uid: StableUID
    name: NonEmptyStr
    version: NonEmptyStr
    artifact_hash: str

    @field_validator("artifact_hash")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="artifact_hash")


class RetrievalMetadata(Phase15Model):
    """Stable sidecar row; vectors are intentionally stored separately."""

    record_uid: StableUID
    modality: Modality
    content_hash: str
    encoder: EncoderProvenance
    event_time: datetime
    available_at: datetime
    coverage: CoverageState
    entity_uids: tuple[StableUID, ...] = ()
    provenance: Provenance
    reliability: SourceReliability
    relevance_hint: Annotated[Decimal, Field(strict=True, ge=Decimal("0"), le=Decimal("1"))] = Decimal("1")
    embedding_hash: str

    @field_validator("content_hash", "embedding_hash")
    @classmethod
    def validate_hashes(cls, value: str, info) -> str:
        return _require_sha256(value, field_name=info.field_name)

    @field_validator("event_time", "available_at")
    @classmethod
    def validate_times(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_lineage(self) -> "RetrievalMetadata":
        if self.available_at < self.provenance.retrieved_at:
            raise ValueError("record cannot become available before raw provenance is retrieved")
        if self.reliability.source_uid != self.provenance.source_uid:
            raise ValueError("reliability source must match provenance source")
        if len(set(self.entity_uids)) != len(self.entity_uids):
            raise ValueError("entity_uids must be unique")
        return self


class RetrievalRecord(Phase15Model):
    """A raw catalog input.  The mutable numerical array never reaches the sidecar."""

    metadata: RetrievalMetadata
    embedding: tuple[float, ...]

    @field_validator("embedding")
    @classmethod
    def validate_embedding(cls, value: tuple[float, ...]) -> tuple[float, ...]:
        _normalized_embedding(value)
        return value

    @model_validator(mode="after")
    def validate_embedding_fingerprint(self) -> "RetrievalRecord":
        if embedding_hash(self.embedding) != self.metadata.embedding_hash:
            raise ValueError("embedding_hash does not match the normalized float32 embedding")
        return self


class RerankWeights(Phase15Model):
    """Explicit, fixed late-reranking weights; no model-inferred decision boundary."""

    relevance: Annotated[Decimal, Field(strict=True, ge=Decimal("0"))] = Decimal("0.55")
    entity: Annotated[Decimal, Field(strict=True, ge=Decimal("0"))] = Decimal("0.15")
    time: Annotated[Decimal, Field(strict=True, ge=Decimal("0"))] = Decimal("0.15")
    provenance: Annotated[Decimal, Field(strict=True, ge=Decimal("0"))] = Decimal("0.075")
    reliability: Annotated[Decimal, Field(strict=True, ge=Decimal("0"))] = Decimal("0.075")

    @model_validator(mode="after")
    def require_nonzero_total(self) -> "RerankWeights":
        if self.total <= Decimal("0"):
            raise ValueError("at least one reranking weight must be positive")
        return self

    @property
    def total(self) -> Decimal:
        return self.relevance + self.entity + self.time + self.provenance + self.reliability


class RetrievalQuery(Phase15Model):
    """A pure vector query with all admissibility filters declared up front."""

    query_uid: StableUID
    modality: Modality
    embedding: tuple[float, ...]
    encoder: EncoderProvenance
    as_of: datetime
    top_k: int = Field(default=10, strict=True, ge=1)
    allowed_coverage: tuple[CoverageState, ...] = (CoverageState.COMPLETE,)
    entity_uids: tuple[StableUID, ...] = ()
    required_entity_uids: tuple[StableUID, ...] = ()
    require_all_entities: bool = False
    source_uids: tuple[StableUID, ...] = ()
    include_record_uids: tuple[StableUID, ...] = ()
    event_time_start: datetime | None = None
    event_time_end: datetime | None = None
    anchor_time: datetime | None = None
    time_horizon_seconds: int = Field(default=86_400, strict=True, ge=1)
    rerank_weights: RerankWeights = RerankWeights()

    @field_validator("embedding")
    @classmethod
    def validate_embedding(cls, value: tuple[float, ...]) -> tuple[float, ...]:
        _normalized_embedding(value)
        return value

    @field_validator("as_of", "event_time_start", "event_time_end", "anchor_time")
    @classmethod
    def validate_times(cls, value: datetime | None, info) -> datetime | None:
        return None if value is None else _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_filters(self) -> "RetrievalQuery":
        if not self.allowed_coverage:
            raise ValueError("allowed_coverage cannot be empty")
        if self.event_time_start is not None and self.event_time_end is not None:
            if self.event_time_end < self.event_time_start:
                raise ValueError("event_time_end cannot precede event_time_start")
        for name, values in (
            ("entity_uids", self.entity_uids),
            ("required_entity_uids", self.required_entity_uids),
            ("source_uids", self.source_uids),
            ("include_record_uids", self.include_record_uids),
        ):
            if len(set(values)) != len(values):
                raise ValueError(f"{name} must be unique")
        return self


class RerankBreakdown(Phase15Model):
    relevance: float = Field(ge=0.0, le=1.0)
    entity: float = Field(ge=0.0, le=1.0)
    time: float = Field(ge=0.0, le=1.0)
    provenance: float = Field(ge=0.0, le=1.0)
    reliability: float = Field(ge=0.0, le=1.0)


class RetrievalResult(Phase15Model):
    metadata: RetrievalMetadata
    cosine_similarity: float = Field(ge=-1.000001, le=1.000001)
    rerank: RerankBreakdown
    rerank_score: float = Field(ge=0.0, le=1.0)


class RetrievalResponse(Phase15Model):
    catalog_uid: StableUID
    query_uid: StableUID
    as_of: datetime
    eligible_count: int = Field(ge=0)
    results: tuple[RetrievalResult, ...]
    index_type: Literal["faiss.IndexFlatIP"] = "faiss.IndexFlatIP"
    ann_state: ANNState = ANNState.UNAVAILABLE_PENDING
    ann_reason: NonEmptyStr = ANN_UNAVAILABLE_REASON

    @field_validator("as_of")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _utc(value, field_name="as_of")


class RetrievalCatalogManifest(Phase15Model):
    """Frozen catalog fingerprint and sidecar provenance."""

    schema_version: NonEmptyStr = RETRIEVAL_SCHEMA_VERSION
    catalog_uid: StableUID
    built_at: datetime
    metadata_hash: str
    record_count: int = Field(ge=0)
    modality_dimensions: tuple[tuple[Modality, int], ...]
    index_type: Literal["faiss.IndexFlatIP"] = "faiss.IndexFlatIP"
    ann_state: ANNState = ANNState.UNAVAILABLE_PENDING
    ann_reason: NonEmptyStr = ANN_UNAVAILABLE_REASON
    frozen: Literal[True] = True

    @field_validator("built_at")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return _utc(value, field_name="built_at")

    @field_validator("metadata_hash")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _require_sha256(value, field_name="metadata_hash")

    @model_validator(mode="after")
    def validate_dimensions(self) -> "RetrievalCatalogManifest":
        modalities = [modality for modality, _dimension in self.modality_dimensions]
        if len(set(modalities)) != len(modalities):
            raise ValueError("modality_dimensions must contain each modality once")
        if any(dimension <= 0 for _modality, dimension in self.modality_dimensions):
            raise ValueError("all FAISS shard dimensions must be positive")
        return self


class MetadataCollisionError(ValueError):
    """Raised when an input attempts to reuse a stable UID in one catalog snapshot."""


class EncoderMismatchError(ValueError):
    """Raised when a query vector's encoder lineage differs from its target shard."""


@dataclass(frozen=True)
class _FlatShard:
    metadata: tuple[RetrievalMetadata, ...]
    vectors: np.ndarray
    index: Any
    encoder: EncoderProvenance


class RetrievalCatalog:
    """Frozen modality-specific exact cosine catalogs and immutable metadata sidecars."""

    __slots__ = ("_manifest", "_shards", "_metadata")

    def __init__(
        self,
        *,
        manifest: RetrievalCatalogManifest,
        shards: dict[Modality, _FlatShard],
        metadata: tuple[RetrievalMetadata, ...],
    ) -> None:
        self._manifest = manifest
        self._shards = dict(shards)
        self._metadata = metadata

    @property
    def manifest(self) -> RetrievalCatalogManifest:
        return self._manifest

    @property
    def metadata_sidecar(self) -> tuple[RetrievalMetadata, ...]:
        return self._metadata

    @classmethod
    def build(cls, records: Iterable[RetrievalRecord], *, built_at: datetime) -> "RetrievalCatalog":
        """Create an immutable exact catalog from precomputed, provenance-bound vectors."""

        normalized_built_at = _utc(built_at, field_name="built_at")
        ordered_records = tuple(
            sorted(records, key=lambda record: (record.metadata.modality.value, record.metadata.record_uid))
        )
        seen_uids: set[str] = set()
        for record in ordered_records:
            uid = record.metadata.record_uid
            if uid in seen_uids:
                raise MetadataCollisionError(f"duplicate stable metadata UID: {uid}")
            seen_uids.add(uid)

        grouped: dict[Modality, list[RetrievalRecord]] = {}
        for record in ordered_records:
            grouped.setdefault(record.metadata.modality, []).append(record)

        shards: dict[Modality, _FlatShard] = {}
        dimensions: list[tuple[Modality, int]] = []
        for modality in sorted(grouped, key=lambda value: value.value):
            rows = grouped[modality]
            encoder = rows[0].metadata.encoder
            if any(row.metadata.encoder != encoder for row in rows):
                raise MetadataCollisionError(
                    f"modality {modality.value} mixes encoder provenance; build separate catalog snapshots"
                )
            vectors = [_normalized_embedding(row.embedding) for row in rows]
            dimension = int(vectors[0].shape[0])
            if any(vector.shape[0] != dimension for vector in vectors):
                raise MetadataCollisionError(f"modality {modality.value} has inconsistent embedding dimensions")
            matrix = np.ascontiguousarray(np.vstack(vectors), dtype=np.float32)
            index = faiss.IndexFlatIP(dimension)
            index.add(matrix)
            matrix.setflags(write=False)
            shards[modality] = _FlatShard(
                metadata=tuple(row.metadata for row in rows),
                vectors=matrix,
                index=index,
                encoder=encoder,
            )
            dimensions.append((modality, dimension))

        sidecar = tuple(record.metadata for record in ordered_records)
        metadata_payload = [item.model_dump(mode="json") for item in sidecar]
        metadata_hash = canonical_hash(metadata_payload)
        fingerprint = {
            "schema_version": RETRIEVAL_SCHEMA_VERSION,
            "built_at": normalized_built_at,
            "metadata_hash": metadata_hash,
            "record_count": len(sidecar),
            "modality_dimensions": tuple(dimensions),
            "index_type": "faiss.IndexFlatIP",
            "ann_state": ANNState.UNAVAILABLE_PENDING,
            "ann_reason": ANN_UNAVAILABLE_REASON,
            "frozen": True,
        }
        manifest = RetrievalCatalogManifest(
            catalog_uid=f"catalog:{canonical_hash(fingerprint)}",
            **fingerprint,
        )
        return cls(manifest=manifest, shards=shards, metadata=sidecar)

    def _eligible_positions(self, shard: _FlatShard, query: RetrievalQuery) -> tuple[int, ...]:
        allowed_coverage = set(query.allowed_coverage)
        entity_filter = set(query.required_entity_uids)
        source_filter = set(query.source_uids)
        include_filter = set(query.include_record_uids)
        positions: list[int] = []
        for position, metadata in enumerate(shard.metadata):
            # Every filter happens before any similarity ranking.
            if metadata.event_time > query.as_of or metadata.available_at > query.as_of:
                continue
            if metadata.coverage not in allowed_coverage:
                continue
            if include_filter and metadata.record_uid not in include_filter:
                continue
            if source_filter and metadata.provenance.source_uid not in source_filter:
                continue
            if query.event_time_start is not None and metadata.event_time < query.event_time_start:
                continue
            if query.event_time_end is not None and metadata.event_time > query.event_time_end:
                continue
            if entity_filter:
                candidate_entities = set(metadata.entity_uids)
                if query.require_all_entities:
                    if not entity_filter.issubset(candidate_entities):
                        continue
                elif not candidate_entities.intersection(entity_filter):
                    continue
            positions.append(position)
        return tuple(positions)

    @staticmethod
    def _rerank_breakdown(
        *,
        metadata: RetrievalMetadata,
        cosine: float,
        query: RetrievalQuery,
    ) -> RerankBreakdown:
        relevance = max(0.0, min(1.0, (float(cosine) + 1.0) / 2.0)) * float(metadata.relevance_hint)
        if query.entity_uids:
            requested = set(query.entity_uids)
            entity = len(requested.intersection(metadata.entity_uids)) / len(requested)
        else:
            entity = 0.5
        if query.anchor_time is None:
            time_score = 0.5
        else:
            seconds = abs((metadata.event_time - query.anchor_time).total_seconds())
            time_score = max(0.0, 1.0 - seconds / float(query.time_horizon_seconds))
        provenance = 1.0 if (
            metadata.provenance.source_uid
            and metadata.provenance.raw_artifact_uid
            and metadata.provenance.content_hash
            and metadata.content_hash
        ) else 0.0
        return RerankBreakdown(
            relevance=relevance,
            entity=entity,
            time=time_score,
            provenance=provenance,
            reliability=float(metadata.reliability.score),
        )

    @staticmethod
    def _rerank_score(breakdown: RerankBreakdown, weights: RerankWeights) -> float:
        numerator = (
            Decimal(str(breakdown.relevance)) * weights.relevance
            + Decimal(str(breakdown.entity)) * weights.entity
            + Decimal(str(breakdown.time)) * weights.time
            + Decimal(str(breakdown.provenance)) * weights.provenance
            + Decimal(str(breakdown.reliability)) * weights.reliability
        )
        return float(numerator / weights.total)

    def retrieve(self, query: RetrievalQuery) -> RetrievalResponse:
        """Apply all admissibility filters before exact cosine ranking and reranking."""

        shard = self._shards.get(query.modality)
        if shard is None:
            return RetrievalResponse(
                catalog_uid=self.manifest.catalog_uid,
                query_uid=query.query_uid,
                as_of=query.as_of,
                eligible_count=0,
                results=(),
            )
        if shard.encoder != query.encoder:
            raise EncoderMismatchError(
                f"query encoder {query.encoder.encoder_uid} does not match {query.modality.value} shard encoder "
                f"{shard.encoder.encoder_uid}"
            )
        query_vector = _normalized_embedding(query.embedding, expected_dimension=shard.vectors.shape[1])
        positions = self._eligible_positions(shard, query)
        if not positions:
            return RetrievalResponse(
                catalog_uid=self.manifest.catalog_uid,
                query_uid=query.query_uid,
                as_of=query.as_of,
                eligible_count=0,
                results=(),
            )

        # Build a temporary Flat shard over *eligible* rows.  Querying the full
        # catalog then filtering would leak future/inadmissible rows into the
        # ranking and distort the requested top-k.
        filtered_vectors = np.ascontiguousarray(shard.vectors[list(positions)], dtype=np.float32)
        filtered_index = faiss.IndexFlatIP(filtered_vectors.shape[1])
        filtered_index.add(filtered_vectors)
        scores, local_positions = filtered_index.search(
            query_vector.reshape(1, -1),
            len(positions),
        )
        ranked: list[RetrievalResult] = []
        for cosine, local_position in zip(scores[0].tolist(), local_positions[0].tolist(), strict=True):
            if local_position < 0:
                continue
            metadata = shard.metadata[positions[int(local_position)]]
            breakdown = self._rerank_breakdown(metadata=metadata, cosine=float(cosine), query=query)
            ranked.append(
                RetrievalResult(
                    metadata=metadata,
                    cosine_similarity=float(cosine),
                    rerank=breakdown,
                    rerank_score=self._rerank_score(breakdown, query.rerank_weights),
                )
            )
        ranked.sort(
            key=lambda result: (
                -result.rerank_score,
                -result.cosine_similarity,
                result.metadata.record_uid,
            )
        )
        return RetrievalResponse(
            catalog_uid=self.manifest.catalog_uid,
            query_uid=query.query_uid,
            as_of=query.as_of,
            eligible_count=len(positions),
            results=tuple(ranked[: query.top_k]),
        )


class RecallBenchmarkCase(Phase15Model):
    case_uid: StableUID
    query: RetrievalQuery
    relevant_record_uids: tuple[StableUID, ...]

    @model_validator(mode="after")
    def validate_relevant_records(self) -> "RecallBenchmarkCase":
        if not self.relevant_record_uids:
            raise ValueError("Recall@K case needs at least one relevant record UID")
        if len(set(self.relevant_record_uids)) != len(self.relevant_record_uids):
            raise ValueError("relevant_record_uids must be unique")
        return self


class FlatRecallAtKReport(Phase15Model):
    catalog_uid: StableUID
    k: int = Field(ge=1)
    case_count: int = Field(ge=0)
    hit_count: int = Field(ge=0)
    recall_at_k: float = Field(ge=0.0, le=1.0)


class RetrievalSafetyViolation(Phase15Model):
    query_uid: StableUID
    record_uid: StableUID
    reason: NonEmptyStr


class TemporalProvenanceReport(Phase15Model):
    catalog_uid: StableUID
    query_count: int = Field(ge=0)
    result_count: int = Field(ge=0)
    violations: tuple[RetrievalSafetyViolation, ...]

    @property
    def violation_count(self) -> int:
        return len(self.violations)


def benchmark_flat_recall_at_k(
    catalog: RetrievalCatalog,
    cases: Iterable[RecallBenchmarkCase],
    *,
    k: int,
) -> FlatRecallAtKReport:
    """Evaluate exact Flat recall against supplied temporal, filtered queries."""

    if isinstance(k, bool) or k < 1:
        raise ValueError("k must be a positive integer")
    rows = tuple(cases)
    hits = 0
    for case in rows:
        response = catalog.retrieve(case.query.model_copy(update={"top_k": k}))
        retrieved = {result.metadata.record_uid for result in response.results}
        if retrieved.intersection(case.relevant_record_uids):
            hits += 1
    return FlatRecallAtKReport(
        catalog_uid=catalog.manifest.catalog_uid,
        k=k,
        case_count=len(rows),
        hit_count=hits,
        recall_at_k=(hits / len(rows)) if rows else 0.0,
    )


def benchmark_temporal_provenance(
    catalog: RetrievalCatalog,
    queries: Iterable[RetrievalQuery],
) -> TemporalProvenanceReport:
    """Verify retrieval never returns future, incomplete-lineage, or uncovered rows."""

    violations: list[RetrievalSafetyViolation] = []
    query_count = 0
    result_count = 0
    for query in queries:
        query_count += 1
        response = catalog.retrieve(query)
        for result in response.results:
            result_count += 1
            metadata = result.metadata
            reasons: list[str] = []
            if metadata.event_time > query.as_of:
                reasons.append("future_event_time")
            if metadata.available_at > query.as_of:
                reasons.append("future_availability")
            if metadata.coverage not in query.allowed_coverage:
                reasons.append("coverage_not_allowed")
            if not (
                metadata.provenance.source_uid
                and metadata.provenance.raw_artifact_uid
                and metadata.provenance.content_hash
                and metadata.reliability.source_uid == metadata.provenance.source_uid
            ):
                reasons.append("incomplete_or_mismatched_provenance")
            for reason in reasons:
                violations.append(
                    RetrievalSafetyViolation(
                        query_uid=query.query_uid,
                        record_uid=metadata.record_uid,
                        reason=reason,
                    )
                )
    return TemporalProvenanceReport(
        catalog_uid=catalog.manifest.catalog_uid,
        query_count=query_count,
        result_count=result_count,
        violations=tuple(violations),
    )


__all__ = [
    "ANN_STATUS",
    "ANN_UNAVAILABLE_REASON",
    "ANNState",
    "CoverageState",
    "EncoderMismatchError",
    "EncoderProvenance",
    "FlatRecallAtKReport",
    "MetadataCollisionError",
    "RecallBenchmarkCase",
    "RetrievalCatalog",
    "RetrievalCatalogManifest",
    "RetrievalMetadata",
    "RetrievalQuery",
    "RetrievalRecord",
    "RetrievalResponse",
    "RetrievalResult",
    "RerankBreakdown",
    "RerankWeights",
    "TemporalProvenanceReport",
    "benchmark_flat_recall_at_k",
    "benchmark_temporal_provenance",
    "embedding_hash",
]
