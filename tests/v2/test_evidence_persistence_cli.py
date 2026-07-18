import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from marketleak.cli_v2 import collect_evidence_once
from marketleak.domain import CoverageStatus
from marketleak.evidence.archive import (
    EvidenceConflictError,
    PersistentEvidenceArchive,
)
from marketleak.evidence.collector import (
    HttpResponse,
    PublicEvidenceCollector,
    PublicSourceConfig,
)
from marketleak.evidence.coverage import CoverageLedger
from marketleak.evidence.normalize import normalize_evidence
from marketleak.ingestion.raw_store import RawArtifactStore


UTC = timezone.utc
T0 = datetime(2026, 8, 1, 12, tzinfo=UTC)


def _rss(title="Acme filing published", body="Acme published a filing."):
    return f"""<rss version="2.0"><channel><title>Wire</title><item>
    <guid>acme-1</guid><link>https://example.test/acme</link>
    <title>{title}</title><description>{body}</description>
    <pubDate>Sat, 01 Aug 2026 10:00:00 GMT</pubDate>
    </item></channel></rss>""".encode()


class SequenceTransport:
    def __init__(self, *responses):
        self.responses = list(responses)

    def get(self, url, *, timeout, headers):
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class SequenceClock:
    def __init__(self, *values):
        self.values = list(values)

    def __call__(self):
        return self.values.pop(0)


def _response(body):
    return HttpResponse(
        status_code=200,
        body=body,
        headers={"Content-Type": "application/rss+xml"},
        url="https://example.test/feed",
    )


def _source():
    return PublicSourceConfig(
        source_id="wire",
        url="https://example.test/feed",
        format="rss",
        publisher="Example Wire",
        max_pages=1,
        max_entries=10,
    )


def _collector(root, *, archive, ledger, transport, clock):
    return PublicEvidenceCollector(
        sources=[_source()],
        raw_store=RawArtifactStore(root / "raw"),
        archive=archive,
        coverage_ledger=ledger,
        transport=transport,
        clock=clock,
    )


def test_restart_loads_archive_and_replay_is_revision_idempotent(tmp_path):
    normalized_root = tmp_path / "normalized"
    coverage_path = tmp_path / "coverage" / "ledger.jsonl"
    first_seen = T0 + timedelta(seconds=1)
    first_archive = PersistentEvidenceArchive(normalized_root)
    first_ledger = CoverageLedger(path=coverage_path)
    first = _collector(
        tmp_path,
        archive=first_archive,
        ledger=first_ledger,
        transport=SequenceTransport(_response(_rss())),
        clock=SequenceClock(T0, first_seen),
    ).collect()

    restarted_archive = PersistentEvidenceArchive(normalized_root)
    restarted_ledger = CoverageLedger(path=coverage_path)
    replay = _collector(
        tmp_path,
        archive=restarted_archive,
        ledger=restarted_ledger,
        transport=SequenceTransport(_response(_rss())),
        clock=SequenceClock(T0 + timedelta(hours=1), T0 + timedelta(hours=1, seconds=1)),
    ).collect()

    assert len(first.documents) == len(replay.documents) == 1
    assert len(restarted_archive.verify()) == 1
    assert replay.documents[0].first_seen_at == first_seen
    assert replay.documents[0].retrieved_at == first_seen
    assert len(restarted_ledger.verify()) == 2
    assert len(list((tmp_path / "raw" / "receipts").rglob("*.json"))) == 2


def test_restart_retains_distinct_document_revisions(tmp_path):
    normalized_root = tmp_path / "normalized"
    first_archive = PersistentEvidenceArchive(normalized_root)
    _collector(
        tmp_path,
        archive=first_archive,
        ledger=CoverageLedger(path=tmp_path / "coverage.jsonl"),
        transport=SequenceTransport(_response(_rss(title="Acme filing pending"))),
        clock=SequenceClock(T0, T0 + timedelta(seconds=1)),
    ).collect()
    restarted = PersistentEvidenceArchive(normalized_root)
    changed = _collector(
        tmp_path,
        archive=restarted,
        ledger=CoverageLedger(path=tmp_path / "coverage.jsonl"),
        transport=SequenceTransport(_response(_rss(title="Acme filing published"))),
        clock=SequenceClock(T0 + timedelta(hours=1), T0 + timedelta(hours=1, seconds=1)),
    ).collect().documents[0]

    reloaded = PersistentEvidenceArchive(normalized_root)
    revisions = reloaded.revisions(changed.document_uid)
    assert len(revisions) == 2
    assert len({item.revision_uid for item in revisions}) == 2
    assert len({item.content_hash for item in revisions}) == 2


def test_failed_attempt_persists_hash_verified_coverage_gap(tmp_path):
    ledger_path = tmp_path / "coverage" / "ledger.jsonl"
    collector = _collector(
        tmp_path,
        archive=PersistentEvidenceArchive(tmp_path / "normalized"),
        ledger=CoverageLedger(path=ledger_path),
        transport=SequenceTransport(TimeoutError("timed out")),
        clock=SequenceClock(T0, T0 + timedelta(seconds=3)),
    )

    result = collector.collect()
    restarted = CoverageLedger(path=ledger_path)

    assert result.coverage[0].status == CoverageStatus.UNAVAILABLE
    assert restarted.verify()[0].status == CoverageStatus.UNAVAILABLE
    assert "timed out" in restarted.intervals[0].details
    assert restarted.tail_hash != "0" * 64


def test_evidence_uid_or_revision_collision_is_quarantined(tmp_path):
    archive = PersistentEvidenceArchive(tmp_path / "normalized")
    original = normalize_evidence(
        source="wire",
        source_document_id="doc-1",
        retrieved_at=T0,
        title="Original title",
        body="Original body",
    )
    archive.ingest(original)
    changed = normalize_evidence(
        source="wire",
        source_document_id="doc-1",
        retrieved_at=T0,
        title="Changed title",
        body="Changed body",
    ).model_copy(
        update={
            "evidence_uid": original.evidence_uid,
            "revision_uid": original.revision_uid,
            "source_revision": original.source_revision,
        }
    )

    with pytest.raises(EvidenceConflictError):
        archive.ingest(changed)

    assert archive.quarantine_count == 1
    assert len(list((tmp_path / "normalized" / "quarantine").glob("*.json"))) == 1
    assert len(PersistentEvidenceArchive(tmp_path / "normalized").all()) == 1


def test_collect_evidence_once_writes_raw_normalized_and_coverage_outputs(tmp_path):
    config = tmp_path / "sources.json"
    config.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "source_id": "wire",
                        "url": "https://example.test/feed",
                        "format": "rss",
                        "publisher": "Example Wire",
                        "max_pages": 3,
                        "max_entries": 500,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "evidence-output"
    payload = collect_evidence_once(
        config_path=config,
        output_dir=output,
        max_pages=1,
        max_entries=10,
        timeout_seconds=2,
        transport=SequenceTransport(_response(_rss())),
        clock=SequenceClock(T0, T0 + timedelta(seconds=1)),
    )

    assert payload["command"] == "collect-evidence-once"
    assert payload["documents_observed"] == 1
    assert payload["stored_revisions"] == 1
    assert payload["raw_captures"] == 1
    assert payload["collection_attempts"] == 1
    assert payload["coverage"][0]["status"] == "complete"
    assert payload["effectiveness_unknown"] is True
    assert payload["not_proof_of_fraud"] is True
    assert list((output / "raw" / "objects" / "sha256").rglob("*.raw"))
    assert list((output / "normalized" / "revisions").rglob("*.json"))
    assert (output / "coverage" / "ledger.jsonl").is_file()
    assert len(PersistentEvidenceArchive(output / "normalized").verify()) == 1
    assert len(CoverageLedger(path=output / "coverage" / "ledger.jsonl").verify()) == 1


def test_cli_requires_an_explicit_enabled_source(tmp_path):
    config = tmp_path / "empty.json"
    config.write_text('{"sources": []}', encoding="utf-8")
    with pytest.raises(ValueError, match="does not supply a default feed"):
        collect_evidence_once(config_path=config, output_dir=tmp_path / "out")


def test_direct_script_exists_and_selects_the_explicit_command():
    script = Path("scripts/collect_evidence_v2.py").read_text(encoding="utf-8")
    assert '"collect-evidence-once"' in script
    assert "sys.argv[1:]" in script

