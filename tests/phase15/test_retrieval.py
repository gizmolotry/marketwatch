from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketleak.multimodal.retrieval import (
    ANN_STATUS,
    CoverageState,
    EncoderProvenance,
    MetadataCollisionError,
    RecallBenchmarkCase,
    RetrievalCatalog,
    RetrievalMetadata,
    RetrievalQuery,
    RetrievalRecord,
    benchmark_flat_recall_at_k,
    benchmark_temporal_provenance,
    embedding_hash,
)
from marketleak.multimodal.schemas import (
    Modality,
    Provenance,
    ReliabilityTier,
    SourceClass,
    SourceReliability,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
ENCODER = EncoderProvenance(
    encoder_uid="encoder:market-v1",
    name="test-market-encoder",
    version="1.0.0",
    artifact_hash="c" * 64,
)


def provenance(*, source_uid: str = "source:venue", minutes: int = 0) -> Provenance:
    return Provenance(
        source_uid=source_uid,
        raw_artifact_uid=f"raw:{source_uid.split(':', 1)[1]}-{minutes}",
        parser_version="phase15-retrieval-test",
        content_hash="a" * 64,
        retrieved_at=T0 + timedelta(minutes=minutes),
        source_url="https://example.test/raw",
    )


def reliability(*, source_uid: str = "source:venue") -> SourceReliability:
    return SourceReliability(
        source_uid=source_uid,
        source_class=SourceClass.OFFICIAL_VENUE,
        tier=ReliabilityTier.HIGH,
        score=Decimal("0.9"),
        assessed_at=T0,
        rationale="captured from the configured official endpoint",
    )


def record(
    *,
    uid: str,
    vector: tuple[float, ...],
    event_minutes: int = 1,
    available_minutes: int = 2,
    coverage: CoverageState = CoverageState.COMPLETE,
    entities: tuple[str, ...] = (),
    relevance: str = "1",
) -> RetrievalRecord:
    source = "source:venue"
    metadata = RetrievalMetadata(
        record_uid=uid,
        modality=Modality.MARKET_STATE,
        content_hash="b" * 64,
        encoder=ENCODER,
        event_time=T0 + timedelta(minutes=event_minutes),
        available_at=T0 + timedelta(minutes=available_minutes),
        coverage=coverage,
        entity_uids=entities,
        provenance=provenance(source_uid=source, minutes=0),
        reliability=reliability(source_uid=source),
        relevance_hint=Decimal(relevance),
        embedding_hash=embedding_hash(vector),
    )
    return RetrievalRecord(metadata=metadata, embedding=vector)


def query(
    *,
    uid: str = "query:one",
    vector: tuple[float, ...] = (1.0, 0.0),
    as_of_minutes: int = 20,
    entities: tuple[str, ...] = (),
    allowed_coverage: tuple[CoverageState, ...] = (CoverageState.COMPLETE,),
    anchor_minutes: int | None = None,
) -> RetrievalQuery:
    return RetrievalQuery(
        query_uid=uid,
        modality=Modality.MARKET_STATE,
        embedding=vector,
        encoder=ENCODER,
        as_of=T0 + timedelta(minutes=as_of_minutes),
        top_k=10,
        entity_uids=entities,
        allowed_coverage=allowed_coverage,
        anchor_time=None if anchor_minutes is None else T0 + timedelta(minutes=anchor_minutes),
    )


def test_exact_normalized_cosine_uses_flat_inner_product():
    catalog = RetrievalCatalog.build(
        [
            record(uid="record:aligned", vector=(2.0, 0.0)),
            record(uid="record:orthogonal", vector=(0.0, 4.0)),
        ],
        built_at=T0 + timedelta(minutes=30),
    )

    response = catalog.retrieve(query())

    assert catalog.manifest.index_type == "faiss.IndexFlatIP"
    assert catalog.manifest.ann_state.value == ANN_STATUS
    assert response.results[0].metadata.record_uid == "record:aligned"
    assert response.results[0].cosine_similarity == pytest.approx(1.0, abs=1e-6)
    assert response.results[1].cosine_similarity == pytest.approx(0.0, abs=1e-6)


def test_future_and_coverage_filters_run_before_ranking():
    catalog = RetrievalCatalog.build(
        [
            record(uid="record:admissible", vector=(0.8, 0.2)),
            record(uid="record:future", vector=(1.0, 0.0), event_minutes=30, available_minutes=31),
            record(uid="record:partial", vector=(0.99, 0.01), coverage=CoverageState.PARTIAL),
        ],
        built_at=T0 + timedelta(minutes=40),
    )

    response = catalog.retrieve(query(as_of_minutes=20))

    assert response.eligible_count == 1
    assert [item.metadata.record_uid for item in response.results] == ["record:admissible"]
    partial_allowed = catalog.retrieve(
        query(
            uid="query:partial",
            as_of_minutes=20,
            allowed_coverage=(CoverageState.COMPLETE, CoverageState.PARTIAL),
        )
    )
    assert [item.metadata.record_uid for item in partial_allowed.results] == ["record:partial", "record:admissible"]
    assert "record:future" not in {item.metadata.record_uid for item in partial_allowed.results}


def test_structured_reranking_is_deterministic_and_uses_entity_time_features():
    older = record(
        uid="record:a-older",
        vector=(1.0, 0.0),
        event_minutes=1,
        available_minutes=2,
        entities=(),
    )
    matched = record(
        uid="record:b-matched",
        vector=(1.0, 0.0),
        event_minutes=10,
        available_minutes=11,
        entities=("entity:bitcoin",),
    )
    first = RetrievalCatalog.build([older, matched], built_at=T0 + timedelta(minutes=30))
    second = RetrievalCatalog.build([matched, older], built_at=T0 + timedelta(minutes=30))
    request = query(entities=("entity:bitcoin",), anchor_minutes=10)

    one = first.retrieve(request)
    two = second.retrieve(request)

    assert one.results == two.results
    assert [item.metadata.record_uid for item in one.results] == ["record:b-matched", "record:a-older"]
    assert one.results[0].rerank.entity == 1.0
    assert one.results[0].rerank.time == 1.0
    assert one.results[1].rerank.entity == 0.0
    assert one.results[1].rerank.time < one.results[0].rerank.time


def test_flat_recall_and_temporal_provenance_benchmark_have_no_violations():
    expected = record(uid="record:expected", vector=(1.0, 0.0))
    other = record(uid="record:other", vector=(0.0, 1.0))
    catalog = RetrievalCatalog.build([other, expected], built_at=T0 + timedelta(minutes=30))
    request = query(uid="query:benchmark")

    recall = benchmark_flat_recall_at_k(
        catalog,
        [RecallBenchmarkCase(case_uid="case:1", query=request, relevant_record_uids=("record:expected",))],
        k=1,
    )
    safety = benchmark_temporal_provenance(catalog, [request])

    assert recall.recall_at_k == 1.0
    assert recall.hit_count == 1
    assert safety.result_count == 2
    assert safety.violation_count == 0


def test_metadata_collision_rejects_duplicate_stable_uid():
    item = record(uid="record:duplicate", vector=(1.0, 0.0))

    with pytest.raises(MetadataCollisionError, match="duplicate stable metadata UID"):
        RetrievalCatalog.build([item, item], built_at=T0 + timedelta(minutes=30))
