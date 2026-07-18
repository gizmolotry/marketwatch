from datetime import datetime, timedelta, timezone

from marketleak.domain import CoverageStatus
from marketleak.evidence.archive import EvidenceArchive
from marketleak.evidence.collector import (
    HttpResponse,
    PublicEvidenceCollector,
    PublicSourceConfig,
    load_source_config,
)
from marketleak.ingestion.raw_store import RawArtifactStore


UTC = timezone.utc
T0 = datetime(2026, 7, 1, 12, tzinfo=UTC)


def _rss(*, title="Acme merger approved", body="Regulators approved the Acme merger.", updated=None):
    updated_xml = f"<lastBuildDate>{updated}</lastBuildDate>" if updated else ""
    item_updated = f"<updated>{updated}</updated>" if updated else ""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel><title>Example Wire</title>{updated_xml}
      <item>
        <guid>acme-merger-1</guid>
        <link>HTTPS://EXAMPLE.TEST:443/story?b=2&amp;a=1#fragment</link>
        <title>{title}</title>
        <description>{body}</description>
        <pubDate>Wed, 01 Jul 2026 10:00:00 GMT</pubDate>
        {item_updated}
      </item>
    </channel></rss>""".encode("utf-8")


class SequenceTransport:
    def __init__(self, *items):
        self.items = list(items)
        self.requests = []

    def get(self, url, *, timeout, headers):
        self.requests.append({"url": url, "timeout": timeout, "headers": dict(headers)})
        item = self.items.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class SequenceClock:
    def __init__(self, *times):
        self.times = list(times)

    def __call__(self):
        return self.times.pop(0)


def _source(*, backfill=False, max_entries=100):
    return PublicSourceConfig(
        source_id="example-wire",
        url="https://example.test/feed.xml",
        format="rss",
        publisher="Example Wire",
        backfill=backfill,
        timeout_seconds=3,
        max_pages=1,
        max_entries=max_entries,
    )


def _response(body):
    return HttpResponse(
        status_code=200,
        body=body,
        headers={"Content-Type": "application/rss+xml", "ETag": '"rev-1"'},
        url="https://example.test/feed.xml",
    )


def test_raw_response_is_captured_before_parser_failure(tmp_path):
    malformed = b"<rss><not-closed>"
    transport = SequenceTransport(_response(malformed))
    store = RawArtifactStore(tmp_path / "raw")
    collector = PublicEvidenceCollector(
        sources=[_source()],
        raw_store=store,
        archive=EvidenceArchive(),
        transport=transport,
        clock=SequenceClock(T0, T0 + timedelta(seconds=1)),
    )

    result = collector.collect()

    assert result.documents == ()
    assert len(result.raw_captures) == 1
    assert store.read(result.raw_captures[0].sha256) == malformed
    assert store.verify(result.raw_captures[0]) is True
    receipt = store.read_receipt(result.raw_captures[0])
    assert receipt["request"]["url"] == "https://example.test/feed.xml"
    assert receipt["response_metadata"]["status_code"] == 200
    assert "parse failed" in result.errors[0]
    assert result.coverage[0].status == CoverageStatus.UNAVAILABLE


def test_replay_preserves_earliest_local_first_seen(tmp_path):
    first_received = T0 + timedelta(seconds=1)
    second_started = T0 + timedelta(hours=1)
    second_received = second_started + timedelta(seconds=1)
    archive = EvidenceArchive()
    collector = PublicEvidenceCollector(
        sources=[_source()],
        raw_store=RawArtifactStore(tmp_path / "raw"),
        archive=archive,
        transport=SequenceTransport(_response(_rss()), _response(_rss())),
        clock=SequenceClock(T0, first_received, second_started, second_received),
    )

    first = collector.collect().documents[0]
    replay = collector.collect().documents[0]

    assert first.first_seen_at == first_received
    assert replay.first_seen_at == first_received
    assert replay.retrieved_at == first_received
    assert replay.content_hash == first.content_hash
    revisions = archive.revisions(first.document_uid)
    assert len(revisions) == 1
    assert all(item.first_seen_at == first_received for item in revisions)


def test_changed_document_retains_both_content_revisions(tmp_path):
    archive = EvidenceArchive()
    collector = PublicEvidenceCollector(
        sources=[_source()],
        raw_store=RawArtifactStore(tmp_path / "raw"),
        archive=archive,
        transport=SequenceTransport(
            _response(_rss(title="Acme merger proposed", body="The transaction remains pending.")),
            _response(_rss(title="Acme merger approved", body="The regulator approved the transaction.")),
        ),
        clock=SequenceClock(
            T0,
            T0 + timedelta(seconds=1),
            T0 + timedelta(hours=1),
            T0 + timedelta(hours=1, seconds=1),
        ),
    )

    first = collector.collect().documents[0]
    second = collector.collect().documents[0]
    revisions = archive.revisions(first.document_uid)

    assert first.document_uid == second.document_uid
    assert first.content_hash != second.content_hash
    assert first.revision_uid != second.revision_uid
    assert {item.content_hash for item in revisions} == {first.content_hash, second.content_hash}


def test_backfill_keeps_claimed_publication_separate_from_local_receipt(tmp_path):
    received = T0 + timedelta(seconds=1)
    collector = PublicEvidenceCollector(
        sources=[_source(backfill=True)],
        raw_store=RawArtifactStore(tmp_path / "raw"),
        archive=EvidenceArchive(),
        transport=SequenceTransport(_response(_rss())),
        clock=SequenceClock(T0, received),
    )

    evidence = collector.collect().documents[0]

    assert evidence.backfill is True
    assert evidence.claimed_published_at == T0 - timedelta(hours=2)
    assert evidence.first_seen_at == received
    assert evidence.first_seen_at > evidence.claimed_published_at
    assert evidence.metadata["publisher"] == "Example Wire"
    assert evidence.url == "https://example.test/story?a=1&b=2"
    assert evidence.raw_artifact_uid.startswith("public:raw/")


def test_transport_failure_records_explicit_coverage_gap(tmp_path):
    collector = PublicEvidenceCollector(
        sources=[_source()],
        raw_store=RawArtifactStore(tmp_path / "raw"),
        archive=EvidenceArchive(),
        transport=SequenceTransport(TimeoutError("upstream timed out")),
        clock=SequenceClock(T0, T0 + timedelta(seconds=3)),
    )

    result = collector.collect()

    assert result.documents == ()
    assert result.raw_captures == ()
    assert result.coverage[0].status == CoverageStatus.UNAVAILABLE
    assert "timed out" in result.coverage[0].details


def test_archive_cutoff_excludes_documents_not_yet_locally_observed(tmp_path):
    received = T0 + timedelta(seconds=1)
    archive = EvidenceArchive()
    collector = PublicEvidenceCollector(
        sources=[_source()],
        raw_store=RawArtifactStore(tmp_path / "raw"),
        archive=archive,
        transport=SequenceTransport(_response(_rss())),
        clock=SequenceClock(T0, received),
    )

    result = collector.collect()

    assert len(result.documents) == 1
    assert archive.as_of(received - timedelta(microseconds=1)) == ()
    assert archive.as_of(received) == result.documents


def test_example_config_declares_no_undocumented_default_source():
    sources = load_source_config("configs/evidence/sources.example.json")
    assert sources == ()
