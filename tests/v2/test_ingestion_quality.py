from datetime import UTC, datetime, timedelta

from marketleak.ingestion.coverage import CoverageLedger, CoverageRecord
from marketleak.ingestion.quality import DataQualityGate, DataQualityReport


def test_quality_gate_reports_specific_failures():
    report = DataQualityReport(
        source="polymarket:trades",
        received=100,
        normalized=96,
        invalid=4,
        conflicts=1,
        raw_hash_failures=1,
        missing_required={"actor": 3},
    )
    result = DataQualityGate(
        min_normalized=10,
        max_invalid_rate=0.01,
        max_missing_rates={"actor": 0.02},
    ).evaluate(report)

    assert result.passed is False
    assert any("invalid_rate" in reason for reason in result.reasons)
    assert any("conflicts" in reason for reason in result.reasons)
    assert any("raw_hash_failures" in reason for reason in result.reasons)
    assert any("missing_rate[actor]" in reason for reason in result.reasons)


def test_coverage_ledger_only_counts_explicitly_complete_intervals(tmp_path):
    ledger = CoverageLedger(tmp_path / "coverage.jsonl")
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ledger.append(
        CoverageRecord(
            platform="kalshi",
            dataset="public_trades",
            interval_start=start,
            interval_end=start + timedelta(hours=1),
            fetched_at=start + timedelta(hours=2),
            record_count=5,
            complete=True,
            raw_sha256=("a" * 64,),
        )
    )
    ledger.append(
        CoverageRecord(
            platform="kalshi",
            dataset="public_trades",
            interval_start=start + timedelta(hours=1),
            interval_end=start + timedelta(hours=2),
            fetched_at=start + timedelta(hours=2),
            record_count=2,
            complete=False,
            continuation="cursor",
        )
    )

    gaps = ledger.gaps(
        platform="kalshi",
        dataset="public_trades",
        start=start,
        end=start + timedelta(hours=3),
    )

    assert gaps == [(start + timedelta(hours=1), start + timedelta(hours=3))]
    assert len(ledger.records(platform="kalshi", dataset="public_trades")) == 2
