from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketleak.multimodal.embeddings import (
    EmbeddingCandidate,
    EmbeddingMode,
    LocalEncoderDescriptor,
    SentenceTransformerLocalAdapter,
    hash_local_model_tree,
    prepare_embedding_record,
)
from marketleak.multimodal.retrieval import CoverageState
from marketleak.multimodal.schemas import (
    Modality,
    Provenance,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
DESCRIPTOR = LocalEncoderDescriptor(
    encoder_uid="encoder:local-test-v1",
    name="deterministic-local-test",
    version="1.0.0",
    artifact_hash="b" * 64,
    vector_dimension=3,
)


class FakeLocalEncoder:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], EmbeddingMode]] = []

    def encode(self, texts: tuple[str, ...], *, mode: EmbeddingMode) -> tuple[tuple[float, ...], ...]:
        self.calls.append((texts, mode))
        mode_marker = 1.0 if mode == EmbeddingMode.DOCUMENT else 2.0
        return tuple((float(len(text)), mode_marker, 0.5) for text in texts)


def provenance(*, source_uid: str = "source:local") -> Provenance:
    return Provenance(
        source_uid=source_uid,
        raw_artifact_uid="raw:local-artifact",
        parser_version="phase15-embeddings-test",
        content_hash="a" * 64,
        retrieved_at=T0,
        source_url="https://example.test/raw",
    )


def reliability(*, source_uid: str = "source:local") -> SourceReliability:
    return SourceReliability(
        source_uid=source_uid,
        source_class=SourceClass.PRIMARY_SOURCE,
        tier=ReliabilityTier.HIGH,
        score=Decimal("0.9"),
        assessed_at=T0,
        rationale="locally captured primary artifact",
    )


def candidate(
    *,
    record_uid: str = "record:local-one",
    modality: Modality = Modality.PUBLIC_EVIDENCE,
    content: str = "A timestamped primary-source filing.",
    event_time: datetime = T0,
    available_at: datetime = T0,
    as_of: datetime = T0 + timedelta(minutes=1),
    coverage: CoverageState = CoverageState.COMPLETE,
    source_uid: str = "source:local",
) -> EmbeddingCandidate:
    return EmbeddingCandidate(
        record_uid=record_uid,
        modality=modality,
        content=content,
        event_time=event_time,
        available_at=available_at,
        as_of=as_of,
        coverage=coverage,
        provenance=provenance(source_uid=source_uid),
        reliability=reliability(source_uid=source_uid),
        entity_uids=("entity:alpha",),
    )


def test_preparation_is_deterministic_and_preserves_content_encoder_and_provenance_hashes():
    encoder = FakeLocalEncoder()
    source = candidate()

    first = prepare_embedding_record(source, mode=EmbeddingMode.DOCUMENT, encoder=encoder, descriptor=DESCRIPTOR)
    second = prepare_embedding_record(source, mode=EmbeddingMode.DOCUMENT, encoder=encoder, descriptor=DESCRIPTOR)
    record = first.to_retrieval_record()

    assert first.vector == second.vector
    assert first.content_hash == second.content_hash
    assert first.vector_hash == second.vector_hash
    assert encoder.calls == [((source.content,), EmbeddingMode.DOCUMENT)] * 2
    assert record.metadata.content_hash == first.content_hash
    assert record.metadata.embedding_hash == first.vector_hash
    assert record.metadata.encoder.artifact_hash == DESCRIPTOR.artifact_hash
    assert record.metadata.provenance == source.provenance


def test_document_query_modes_and_all_supported_modalities_stay_explicit():
    encoder = FakeLocalEncoder()

    for modality in Modality:
        prepared = prepare_embedding_record(
            candidate(record_uid=f"record:{modality.value}", modality=modality),
            mode=EmbeddingMode.DOCUMENT,
            encoder=encoder,
            descriptor=DESCRIPTOR,
        )
        assert prepared.to_retrieval_record().metadata.modality == modality

    query = prepare_embedding_record(
        candidate(record_uid="record:query"),
        mode=EmbeddingMode.QUERY,
        encoder=encoder,
        descriptor=DESCRIPTOR,
    )
    assert query.to_retrieval_query().as_of == query.candidate.as_of
    assert query.to_retrieval_query().encoder == DESCRIPTOR.to_provenance()
    with pytest.raises(ValueError, match="query-mode"):
        query.to_retrieval_record()
    with pytest.raises(ValueError, match="document-mode"):
        prepare_embedding_record(
            candidate(),
            mode=EmbeddingMode.DOCUMENT,
            encoder=encoder,
            descriptor=DESCRIPTOR,
        ).to_retrieval_query()


def test_candidate_rejects_future_uncovered_or_invalid_provenance_availability():
    with pytest.raises(ValueError, match="future"):
        candidate(event_time=T0 + timedelta(minutes=2), as_of=T0 + timedelta(minutes=1))
    with pytest.raises(ValueError, match="future"):
        candidate(available_at=T0 + timedelta(minutes=2), as_of=T0 + timedelta(minutes=1))
    with pytest.raises(ValueError, match="uncovered"):
        candidate(coverage=CoverageState.PARTIAL)
    with pytest.raises(ValueError, match="before raw provenance"):
        candidate(available_at=T0 - timedelta(seconds=1))
    with pytest.raises(ValueError, match="reliability source"):
        EmbeddingCandidate(
            record_uid="record:mismatched-source",
            modality=Modality.ONCHAIN_SETTLEMENT,
            content="Raw settlement receipt.",
            event_time=T0,
            available_at=T0,
            as_of=T0 + timedelta(minutes=1),
            coverage=CoverageState.COMPLETE,
            provenance=provenance(source_uid="source:one"),
            reliability=reliability(source_uid="source:two"),
        )


def test_local_adapter_requires_existing_hashed_local_model_and_never_loads_it_in_constructor(tmp_path):
    model_dir = tmp_path / "local-model"
    model_dir.mkdir()
    (model_dir / "config.json").write_text('{"local": true}', encoding="utf-8")
    descriptor = LocalEncoderDescriptor(
        encoder_uid="encoder:adapter-v1",
        name="already-local",
        version="1.0.0",
        artifact_hash=hash_local_model_tree(model_dir),
        vector_dimension=3,
    )

    adapter = SentenceTransformerLocalAdapter(model_dir, descriptor=descriptor)

    assert adapter.model_path == model_dir.resolve()
    assert adapter._model is None
    with pytest.raises(ValueError, match="already-local"):
        SentenceTransformerLocalAdapter("https://models.example.test/not-local", descriptor=descriptor)
    with pytest.raises(ValueError, match="existing local"):
        SentenceTransformerLocalAdapter(tmp_path / "missing", descriptor=descriptor)
    with pytest.raises(ValueError, match="artifact hash"):
        SentenceTransformerLocalAdapter(
            model_dir,
            descriptor=LocalEncoderDescriptor(
                encoder_uid="encoder:wrong-hash",
                name="already-local",
                version="1.0.0",
                artifact_hash="0" * 64,
                vector_dimension=3,
            ),
        )


def test_prepared_embedding_exposes_only_retrieval_material_not_prohibited_outcomes():
    prepared = prepare_embedding_record(
        candidate(content="Source text with no behavioral conclusion."),
        mode=EmbeddingMode.DOCUMENT,
        encoder=FakeLocalEncoder(),
        descriptor=DESCRIPTOR,
    )

    public_shape = repr(prepared).lower()
    for prohibited in ("fraud", "insider", "misconduct", "outcome_score"):
        assert prohibited not in public_shape
    assert set(prepared.__dataclass_fields__) == {"candidate", "mode", "encoder", "vector"}
