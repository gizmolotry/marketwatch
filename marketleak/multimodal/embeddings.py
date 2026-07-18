"""Local-only, provenance-bound embedding preparation.

This module prepares deterministic vector records for a separately frozen
retrieval catalog.  It never builds an index, reaches a network, invokes a
language model, or derives an outcome score.  An encoder is always injected;
the optional SentenceTransformer adapter accepts an already-local artifact
only and disables remote code and downloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
import hashlib
from pathlib import Path
from typing import Iterable, Mapping, Protocol, Sequence

import numpy as np

from .retrieval import (
    CoverageState,
    EncoderProvenance,
    RetrievalMetadata,
    RetrievalQuery,
    RetrievalRecord,
    embedding_hash,
)
from .schemas import Modality, Provenance, SourceReliability


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _require_hash(value: str, *, field: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return normalized


class EmbeddingMode(str, Enum):
    DOCUMENT = "document"
    QUERY = "query"


class LocalEncoderProtocol(Protocol):
    """Injected, offline encoder contract used by the preparation boundary."""

    def encode(self, texts: Sequence[str], *, mode: EmbeddingMode) -> Sequence[Sequence[float]]:
        """Return one finite vector per input text without side effects."""


@dataclass(frozen=True)
class LocalEncoderDescriptor:
    """Immutable identity and expected output dimension for a local encoder."""

    encoder_uid: str
    name: str
    version: str
    artifact_hash: str
    vector_dimension: int

    def __post_init__(self) -> None:
        if not all(str(value).strip() for value in (self.encoder_uid, self.name, self.version)):
            raise ValueError("encoder UID, name, and version are required")
        object.__setattr__(self, "artifact_hash", _require_hash(self.artifact_hash, field="artifact_hash"))
        if isinstance(self.vector_dimension, bool) or int(self.vector_dimension) < 1:
            raise ValueError("vector_dimension must be a positive integer")
        object.__setattr__(self, "vector_dimension", int(self.vector_dimension))

    def to_provenance(self) -> EncoderProvenance:
        return EncoderProvenance(
            encoder_uid=self.encoder_uid,
            name=self.name,
            version=self.version,
            artifact_hash=self.artifact_hash,
        )


@dataclass(frozen=True)
class EmbeddingCandidate:
    """Causally admissible source text for one local embedding operation."""

    record_uid: str
    modality: Modality
    content: str
    event_time: datetime
    available_at: datetime
    as_of: datetime
    coverage: CoverageState
    provenance: Provenance
    reliability: SourceReliability
    entity_uids: tuple[str, ...] = ()
    relevance_hint: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if not str(self.record_uid).strip() or not str(self.content):
            raise ValueError("record_uid and non-empty content are required")
        modality = self.modality if isinstance(self.modality, Modality) else Modality(self.modality)
        coverage = self.coverage if isinstance(self.coverage, CoverageState) else CoverageState(self.coverage)
        object.__setattr__(self, "modality", modality)
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(self, "event_time", _utc(self.event_time))
        object.__setattr__(self, "available_at", _utc(self.available_at))
        object.__setattr__(self, "as_of", _utc(self.as_of))
        if self.event_time > self.as_of or self.available_at > self.as_of:
            raise ValueError("future or later-available content cannot be embedded at this as_of")
        if self.available_at < self.provenance.retrieved_at:
            raise ValueError("content cannot become available before raw provenance retrieval")
        if self.reliability.source_uid != self.provenance.source_uid:
            raise ValueError("reliability source must match raw provenance source")
        if coverage != CoverageState.COMPLETE:
            raise ValueError("uncovered or partial content cannot be embedded for the frozen catalog")
        if len(set(self.entity_uids)) != len(self.entity_uids):
            raise ValueError("entity_uids must be unique")
        hint = Decimal(self.relevance_hint)
        if hint < Decimal("0") or hint > Decimal("1"):
            raise ValueError("relevance_hint must be in [0, 1]")
        object.__setattr__(self, "relevance_hint", hint)

    @property
    def content_hash(self) -> str:
        return _sha256_text(self.content)


@dataclass(frozen=True)
class PreparedEmbedding:
    """A local vector plus immutable source/encoder/timing metadata."""

    candidate: EmbeddingCandidate
    mode: EmbeddingMode
    encoder: LocalEncoderDescriptor
    vector: tuple[float, ...]

    def __post_init__(self) -> None:
        mode = self.mode if isinstance(self.mode, EmbeddingMode) else EmbeddingMode(self.mode)
        object.__setattr__(self, "mode", mode)
        vector = tuple(float(item) for item in self.vector)
        if len(vector) != self.encoder.vector_dimension:
            raise ValueError("encoder output dimension does not match descriptor")
        array = np.asarray(vector, dtype=np.float32)
        if not np.isfinite(array).all() or float(np.linalg.norm(array)) <= 0.0:
            raise ValueError("encoder output must be finite and non-zero")
        object.__setattr__(self, "vector", vector)

    @property
    def content_hash(self) -> str:
        return self.candidate.content_hash

    @property
    def vector_hash(self) -> str:
        return embedding_hash(self.vector)

    def to_retrieval_record(self) -> RetrievalRecord:
        """Materialize a document-side catalog row; queries cannot enter it."""

        if self.mode != EmbeddingMode.DOCUMENT:
            raise ValueError("query-mode embeddings cannot be inserted into a frozen retrieval catalog")
        metadata = RetrievalMetadata(
            record_uid=self.candidate.record_uid,
            modality=self.candidate.modality,
            content_hash=self.content_hash,
            encoder=self.encoder.to_provenance(),
            event_time=self.candidate.event_time,
            available_at=self.candidate.available_at,
            coverage=self.candidate.coverage,
            entity_uids=self.candidate.entity_uids,
            provenance=self.candidate.provenance,
            reliability=self.candidate.reliability,
            relevance_hint=self.candidate.relevance_hint,
            embedding_hash=self.vector_hash,
        )
        return RetrievalRecord(metadata=metadata, embedding=self.vector)

    def to_retrieval_query(self, *, query_uid: str | None = None, top_k: int = 10) -> RetrievalQuery:
        """Materialize a query-side vector with the original causal cutoff."""

        if self.mode != EmbeddingMode.QUERY:
            raise ValueError("document-mode embeddings cannot be represented as retrieval queries")
        return RetrievalQuery(
            query_uid=query_uid or f"query:{self.candidate.record_uid}",
            modality=self.candidate.modality,
            embedding=self.vector,
            encoder=self.encoder.to_provenance(),
            as_of=self.candidate.as_of,
            top_k=top_k,
            allowed_coverage=(CoverageState.COMPLETE,),
            entity_uids=self.candidate.entity_uids,
        )


def prepare_embedding_record(
    candidate: EmbeddingCandidate,
    *,
    mode: EmbeddingMode,
    encoder: LocalEncoderProtocol,
    descriptor: LocalEncoderDescriptor,
) -> PreparedEmbedding:
    """Encode exactly one causally admissible item through an injected local encoder."""

    selected_mode = mode if isinstance(mode, EmbeddingMode) else EmbeddingMode(mode)
    vectors = encoder.encode((candidate.content,), mode=selected_mode)
    if len(vectors) != 1:
        raise ValueError("encoder must return exactly one vector for one candidate")
    return PreparedEmbedding(candidate=candidate, mode=selected_mode, encoder=descriptor, vector=tuple(vectors[0]))


def hash_local_model_tree(path: str | Path) -> str:
    """Hash a local model directory/file without resolving any remote identifier."""

    root = Path(path)
    if not root.exists() or root.is_symlink():
        raise ValueError("encoder model path must exist locally and cannot be a symlink")
    files = [root] if root.is_file() else sorted(item for item in root.rglob("*") if item.is_file())
    if not files:
        raise ValueError("local encoder path must contain at least one regular file")
    digest = hashlib.sha256()
    for item in files:
        if item.is_symlink():
            raise ValueError("local encoder path cannot contain symlinked files")
        relative = item.name if root.is_file() else item.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\x00")
        with item.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    return digest.hexdigest()


class SentenceTransformerLocalAdapter:
    """Optional adapter whose constructor guards local-only model provenance."""

    def __init__(self, model_path: str | Path, *, descriptor: LocalEncoderDescriptor) -> None:
        raw_path = str(model_path).strip()
        if "://" in raw_path or raw_path.startswith("hf:"):
            raise ValueError("SentenceTransformer adapter accepts an already-local model path only")
        path = Path(raw_path).expanduser()
        if not path.exists() or not path.is_dir() or path.is_symlink():
            raise ValueError("SentenceTransformer adapter requires an existing local model directory")
        resolved = path.resolve()
        if hash_local_model_tree(resolved) != descriptor.artifact_hash:
            raise ValueError("local model artifact hash does not match encoder descriptor")
        self.model_path = resolved
        self.descriptor = descriptor
        self._model: object | None = None

    def encode(self, texts: Sequence[str], *, mode: EmbeddingMode) -> Sequence[Sequence[float]]:
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:  # pragma: no cover - optional runtime guard.
                raise RuntimeError("sentence-transformers is not installed for the optional local adapter") from exc
            # These flags are part of the boundary: a model identifier cannot
            # trigger a download or execute custom remote code.
            self._model = SentenceTransformer(
                str(self.model_path),
                local_files_only=True,
                trust_remote_code=False,
            )
        model = self._model
        if mode == EmbeddingMode.QUERY and hasattr(model, "encode_query"):
            vectors = model.encode_query(list(texts), convert_to_numpy=True, show_progress_bar=False)
        elif mode == EmbeddingMode.DOCUMENT and hasattr(model, "encode_document"):
            vectors = model.encode_document(list(texts), convert_to_numpy=True, show_progress_bar=False)
        else:
            vectors = model.encode(list(texts), convert_to_numpy=True, show_progress_bar=False)
        array = np.asarray(vectors, dtype=np.float32)
        if array.ndim != 2 or array.shape[0] != len(texts):
            raise ValueError("local encoder returned an invalid batch shape")
        if array.shape[1] != self.descriptor.vector_dimension:
            raise ValueError("local encoder dimension does not match descriptor")
        return tuple(tuple(float(value) for value in row) for row in array)


__all__ = [
    "EmbeddingCandidate",
    "EmbeddingMode",
    "LocalEncoderDescriptor",
    "LocalEncoderProtocol",
    "PreparedEmbedding",
    "SentenceTransformerLocalAdapter",
    "hash_local_model_tree",
    "prepare_embedding_record",
]
