"""Operational CLI for the validation-first MarketLeak v2 path."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from marketleak.evidence.archive import PersistentEvidenceArchive
from marketleak.evidence.collector import (
    HttpTransport,
    PublicEvidenceCollector,
    load_source_config,
)
from marketleak.evidence.coverage import CoverageLedger as PublicCoverageLedger
from marketleak.detectors import CausalActivityDetector, DetectorConfig
from marketleak.ingestion import CoverageLedger, CoverageRecord, NormalizedStore, RawArtifactStore
from marketleak.ingestion.connectors import KalshiConnector, PolymarketConnector
from marketleak.ingestion.connectors.http import EvidenceHttpClient
from marketleak.ingestion.connectors.models import IngestionBatch
from marketleak.pipeline_v2 import (
    CANONICAL_PIPELINE_SOURCE,
    CAPABILITIES_V2,
    CanonicalInputSnapshot,
    ValidationFirstPipeline,
    load_canonical_inputs,
)
from marketleak.shadow import (
    ShadowInputRecord,
    ShadowLedger,
    ShadowRunManifestV2,
    ShadowRunner,
    build_shadow_manifest,
)
from marketleak.shadow.manifest import hash_file


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    raise TypeError(f"cannot JSON serialize {type(value).__name__}")


def _emit(payload: Mapping[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=_json_default))


def _datetime(value: str | datetime | None, *, default: datetime | None = None) -> datetime:
    if value is None:
        if default is None:
            raise ValueError("timestamp is required")
        return default
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return parsed.astimezone(UTC)


def capabilities_payload() -> dict[str, Any]:
    return {
        **CAPABILITIES_V2,
        "official_public_connectors": {
            "polymarket_trades": asdict(PolymarketConnector.trade_capability),
            "polymarket_orderbook": asdict(PolymarketConnector.orderbook_capability),
            "kalshi_trades": asdict(KalshiConnector.trade_capability),
            "kalshi_orderbook": asdict(KalshiConnector.orderbook_capability),
        },
        "current_limitations": [
            "Effectiveness is unknown; the public case registry is not sufficient for estimation.",
            "Kalshi public trades do not expose account identity.",
            "Kalshi trade direction is not guaranteed; no fill is created when it is absent.",
            "The connectors collect current L2 snapshots; historical L2 is unavailable from these endpoints.",
            "Public-information coverage is unknown unless a point-in-time archive explicitly proves coverage.",
            "Graph context must be independently sourced and cannot corroborate an edge created from the alert itself.",
        ],
        "claim_scope": "surveillance candidates for human review; never an automatic fraud finding",
    }


def audit_legacy_fixture(ticks_path: str | Path) -> dict[str, Any]:
    path = Path(ticks_path)
    if not path.is_file():
        raise FileNotFoundError(f"legacy ticks fixture not found: {path}")
    result = ValidationFirstPipeline().run(ticks_path=path)
    payload = result.run_payload()
    payload.update(
        {
            "command": "audit-legacy",
            "fixture": str(path),
            "actionable_count": len(result.assessments),
            "legacy_fixture_only": True,
            "training_eligible": False,
            "limitations": [
                "The legacy fixture lacks immutable raw response lineage.",
                "It does not contain reliable trade size, maker, taker, or account attribution.",
                "It cannot support B public-timeline or C actor-misconduct conclusions.",
            ],
        }
    )
    return payload


def _write_batch(store: NormalizedStore, batch: IngestionBatch) -> dict[str, Any]:
    results = {
        "fills": store.write(batch.fills, record_type="trade_fill"),
        "observations": store.write(batch.observations, record_type="price_observation"),
        "snapshots": store.write(batch.snapshots, record_type="orderbook_snapshot"),
    }
    return {
        name: {
            "inserted": result.inserted,
            "duplicates": result.duplicates,
            "conflicts": result.conflicts,
            "rejected": result.rejected,
        }
        for name, result in results.items()
    }


def _coverage_for_batch(
    ledger: CoverageLedger,
    *,
    platform: str,
    dataset: str,
    batch: IngestionBatch,
    requested_start: datetime | None,
    requested_end: datetime | None,
    filters: Mapping[str, Any] | None = None,
    resumed_from: str | None = None,
) -> CoverageRecord:
    times = [record.event_time for record in batch.records]
    now = datetime.now(UTC)
    exact_filters = dict(filters or {})
    filter_start = exact_filters.get("start")
    filter_end = exact_filters.get("end")
    start = requested_start
    if start is None and isinstance(filter_start, int) and not isinstance(filter_start, bool):
        start = datetime.fromtimestamp(filter_start, tz=UTC)
    start = start or (min(times) if times else now)
    end = requested_end
    if end is None and isinstance(filter_end, int) and not isinstance(filter_end, bool):
        end = datetime.fromtimestamp(filter_end, tz=UTC)
    if end is None:
        end = max(times) if times else (max(now, start) if exact_filters else start)

    raw_sha256 = tuple(
        artifact.content_hash.removeprefix("sha256:")
        for artifact in batch.raw_artifacts
    )
    record_count = len(batch.records)
    complete = batch.complete
    if resumed_from is not None:
        prior = next(
            (
                row
                for row in reversed(ledger.records(platform=platform, dataset=dataset))
                if row.continuation == resumed_from and dict(row.filters) == exact_filters
            ),
            None,
        )
        if prior is None:
            # A cursor proves where to resume, but not that this output root
            # contains the earlier deliveries. Never claim interval coverage
            # without that raw lineage.
            complete = False
        else:
            raw_sha256 = (*prior.raw_sha256, *raw_sha256)
            record_count += prior.record_count

    record = CoverageRecord(
        platform=platform,
        dataset=dataset,
        interval_start=start,
        interval_end=end,
        fetched_at=now,
        record_count=record_count,
        complete=complete,
        raw_sha256=raw_sha256,
        continuation=batch.continuation,
        filters=exact_filters,
    )
    ledger.append(record)
    return record


def collect_once(
    *,
    output_dir: str | Path,
    platform: str = "all",
    polymarket_market: str | None = None,
    polymarket_token: str | None = None,
    kalshi_ticker: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    page_size: int = 100,
    max_pages: int = 1,
    http_client: EvidenceHttpClient | None = None,
) -> dict[str, Any]:
    if platform not in {"all", "polymarket", "kalshi"}:
        raise ValueError("platform must be all, polymarket, or kalshi")
    if not 1 <= max_pages <= 100:
        raise ValueError("max_pages must be in [1, 100]")
    if not 1 <= page_size <= 10_000:
        raise ValueError("page_size must be in [1, 10000]")
    if start is not None and end is not None and end < start:
        raise ValueError("end must be at or after start")
    root = Path(output_dir)
    raw_store = RawArtifactStore(root / "raw")
    http = http_client or EvidenceHttpClient(raw_store)
    # Injected clients must still target this run's raw store to preserve the
    # raw-before-normalized guarantee.
    if http.raw_store.root.resolve() != raw_store.root.resolve():
        raise ValueError("injected HTTP client must use output_dir/raw as its RawArtifactStore")
    normalized = NormalizedStore(root)
    coverage = CoverageLedger(root / "coverage" / "ledger.jsonl")
    summaries: dict[str, Any] = {}

    if platform in {"all", "polymarket"}:
        connector = PolymarketConnector(http)
        exact_filters = connector.resolve_trade_query_filters(
            market=polymarket_market,
            start=start,
            end=end,
        )
        trades = connector.fetch_trades(
            market=polymarket_market,
            start=start,
            end=datetime.fromtimestamp(exact_filters["end"], tz=UTC),
            page_size=min(page_size, 10_000),
            max_pages=max_pages,
        )
        summaries["polymarket_trades"] = {
            "records": len(trades.records),
            "complete": trades.complete,
            "continuation": trades.continuation,
            "writes": _write_batch(normalized, trades),
        }
        _coverage_for_batch(
            coverage, platform="polymarket", dataset="public_trades", batch=trades,
            requested_start=start, requested_end=end,
            filters=exact_filters,
        )
        if polymarket_token:
            book = connector.fetch_orderbook(polymarket_token)
            summaries["polymarket_orderbook"] = {
                "records": len(book.records), "writes": _write_batch(normalized, book)
            }
            _coverage_for_batch(
                coverage, platform="polymarket", dataset="orderbook_snapshot", batch=book,
                requested_start=None, requested_end=None,
            )

    if platform in {"all", "kalshi"}:
        connector = KalshiConnector(http)
        trades = connector.fetch_trades(
            ticker=kalshi_ticker,
            start=start,
            end=end,
            page_size=min(page_size, 1000),
            max_pages=max_pages,
        )
        summaries["kalshi_trades"] = {
            "records": len(trades.records),
            "complete": trades.complete,
            "continuation": trades.continuation,
            "writes": _write_batch(normalized, trades),
        }
        _coverage_for_batch(
            coverage, platform="kalshi", dataset="public_trades", batch=trades,
            requested_start=start, requested_end=end,
        )
        if kalshi_ticker:
            book = connector.fetch_orderbook(kalshi_ticker)
            summaries["kalshi_orderbook"] = {
                "records": len(book.records), "writes": _write_batch(normalized, book)
            }
            _coverage_for_batch(
                coverage, platform="kalshi", dataset="orderbook_snapshot", batch=book,
                requested_start=None, requested_end=None,
            )
    return {
        "command": "collect-once",
        "output_dir": str(root),
        "bounded": {"page_size": page_size, "max_pages": max_pages},
        "sources": summaries,
        "network_was_requested": True,
        "effectiveness_unknown": True,
    }


def _continuation_state(continuation: str | None) -> str | None:
    if continuation is None:
        return None
    try:
        payload = json.loads(continuation)
    except (json.JSONDecodeError, TypeError):
        return "legacy_offset"
    if isinstance(payload, Mapping) and isinstance(payload.get("state"), str):
        return payload["state"]
    return "unknown"


def collect_polymarket_wallet_history(
    *,
    output_dir: str | Path,
    user: str,
    market: str | None = None,
    event_id: int | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    continuation: str | None = None,
    page_size: int = 1000,
    max_pages: int = 1,
    taker_only: bool = False,
    http_client: EvidenceHttpClient | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Collect a bounded, resumable public Polymarket wallet-trade history."""

    if market is not None or event_id is not None:
        raise ValueError(
            "collect-polymarket-wallet is user-only; collect full wallet history "
            "and filter market/event scope in the local normalized index"
        )
    if not 1 <= max_pages <= 100:
        raise ValueError("max_pages must be in [1, 100]")
    if not 1 <= page_size <= PolymarketConnector.MAX_TRADE_LIMIT:
        raise ValueError("page_size must be in [1, 10000]")
    root = Path(output_dir)
    raw_store = RawArtifactStore(root / "raw")
    http = http_client or EvidenceHttpClient(raw_store)
    if http.raw_store.root.resolve() != raw_store.root.resolve():
        raise ValueError("injected HTTP client must use output_dir/raw as its RawArtifactStore")

    connector = PolymarketConnector(http, clock=clock)
    exact_filters = connector.resolve_trade_query_filters(
        market=market,
        event_id=event_id,
        user=user,
        start=start,
        end=end,
        taker_only=taker_only,
        continuation=continuation,
    )
    frozen_end = datetime.fromtimestamp(exact_filters["end"], tz=UTC)
    trades = connector.fetch_trades(
        market=market,
        event_id=event_id,
        user=user,
        start=start,
        end=frozen_end,
        taker_only=taker_only,
        page_size=page_size,
        max_pages=max_pages,
        continuation=continuation,
    )
    writes = _write_batch(NormalizedStore(root), trades)
    coverage_record = _coverage_for_batch(
        CoverageLedger(root / "coverage" / "ledger.jsonl"),
        platform="polymarket",
        dataset="public_wallet_trades",
        batch=trades,
        requested_start=start,
        requested_end=end,
        filters=exact_filters,
        resumed_from=continuation,
    )
    continuation_state = _continuation_state(trades.continuation)
    return {
        "command": "collect-polymarket-wallet",
        "output_dir": str(root),
        "wallet": exact_filters["user"],
        "query_filters": exact_filters,
        "bounded": {"page_size": page_size, "max_pages": max_pages},
        "records": len(trades.records),
        "complete": coverage_record.complete,
        "continuation": trades.continuation,
        "continuation_state": continuation_state,
        "requires_narrower_time_window": continuation_state == "split_required",
        "writes": writes,
        "coverage": {
            "dataset": coverage_record.dataset,
            "complete": coverage_record.complete,
            "record_count": coverage_record.record_count,
            "filters": dict(coverage_record.filters),
            "raw_sha256": list(coverage_record.raw_sha256),
        },
        "network_was_requested": True,
        "pseudonymous_wallet_only": True,
        "not_identity_attribution": True,
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
    }


def collect_evidence_once(
    *,
    config_path: str | Path,
    output_dir: str | Path,
    max_pages: int = 1,
    max_entries: int = 100,
    timeout_seconds: float = 10.0,
    transport: HttpTransport | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Run one bounded, explicit-source public-evidence collection cycle."""
    config_file = Path(config_path)
    if not config_file.is_file():
        raise FileNotFoundError(f"public evidence source config not found: {config_file}")
    if not 1 <= max_pages <= 10:
        raise ValueError("max_pages must be in [1, 10]")
    if not 1 <= max_entries <= 1000:
        raise ValueError("max_entries must be in [1, 1000]")
    if not 0.0 < timeout_seconds <= 60.0:
        raise ValueError("timeout_seconds must be in (0, 60]")

    configured = load_source_config(config_file)
    enabled = [source for source in configured if source.enabled]
    if not enabled:
        raise ValueError(
            "No enabled public evidence sources were explicitly configured; "
            "MarketLeak does not supply a default feed."
        )
    bounded_sources = tuple(
        source.model_copy(
            update={
                "max_pages": min(source.max_pages, max_pages),
                "max_entries": min(source.max_entries, max_entries),
                "timeout_seconds": min(source.timeout_seconds, timeout_seconds),
            }
        )
        for source in enabled
    )
    root = Path(output_dir)
    raw_store = RawArtifactStore(root / "raw")
    archive = PersistentEvidenceArchive(root / "normalized")
    coverage = PublicCoverageLedger(path=root / "coverage" / "ledger.jsonl")
    collector = PublicEvidenceCollector(
        sources=bounded_sources,
        raw_store=raw_store,
        archive=archive,
        coverage_ledger=coverage,
        transport=transport,
        clock=clock,
    )
    result = collector.collect()
    archive.verify()
    coverage.verify()
    return {
        "command": "collect-evidence-once",
        "config": str(config_file),
        "output_dir": str(root),
        "bounded": {
            "max_pages": max_pages,
            "max_entries": max_entries,
            "timeout_seconds": timeout_seconds,
        },
        "sources": [source.source_id for source in bounded_sources],
        "documents_observed": len(result.documents),
        "stored_revisions": len(archive.all()),
        "raw_captures": len(result.raw_captures),
        "collection_attempts": len(result.coverage),
        "total_coverage_entries": len(coverage.intervals),
        "coverage": [
            {
                "source": interval.source,
                "status": interval.status.value,
                "started_at": interval.started_at,
                "ended_at": interval.ended_at,
                "details": interval.details,
            }
            for interval in result.coverage
        ],
        "errors": list(result.errors),
        "paths": {
            "raw": str(raw_store.root),
            "normalized": str(archive.root),
            "coverage": str(coverage.path),
        },
        "network_was_requested": True,
        "effectiveness_unknown": True,
        "not_proof_of_fraud": True,
    }


def _load_shadow_records(path: str | Path) -> list[ShadowInputRecord]:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"shadow input not found: {source}")
    text = source.read_text(encoding="utf-8")
    if source.suffix.lower() == ".jsonl":
        payloads = [json.loads(line) for line in text.splitlines() if line.strip()]
    else:
        payloads = json.loads(text)
        if not isinstance(payloads, list):
            raise ValueError("shadow JSON input must be a list")
    return [ShadowInputRecord.model_validate(payload) for payload in payloads]


def _simple_yaml_values(source: Path | bytes) -> dict[str, Any]:
    """Parse the scalar, at-most-one-level mappings used by frozen v2 configs."""

    values: dict[str, Any] = {}
    section: str | None = None
    text = source.decode("utf-8") if isinstance(source, bytes) else source.read_text(encoding="utf-8")
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip() or ":" not in line:
            continue
        indent = len(line) - len(line.lstrip())
        key, raw_value = line.strip().split(":", 1)
        raw_value = raw_value.strip()
        if indent == 0:
            section = key if not raw_value else None
            full_key = key
        else:
            full_key = f"{section}.{key}" if section else key
        if not raw_value:
            continue
        try:
            values[full_key] = json.loads(raw_value)
        except json.JSONDecodeError:
            values[full_key] = raw_value.strip('"\'')
    return values


def _detector_from_config(source: Path | bytes) -> CausalActivityDetector:
    values = _simple_yaml_values(source)
    config = DetectorConfig(
        bucket_width=timedelta(minutes=int(values.get("bucket_minutes", 5))),
        max_carry_age=timedelta(minutes=float(values.get("max_carry_age_minutes", 10))),
        baseline_window=timedelta(days=float(values.get("baseline_days", 7))),
        min_baseline_observations=int(values.get("min_baseline_observations", 288)),
        min_baseline_elapsed=timedelta(hours=float(values.get("min_baseline_elapsed_hours", 24))),
        empirical_tail_min_observations=int(values.get("empirical_tail_min_observations", 100)),
        fdr_alpha=float(values.get("fdr_alpha", 0.05)),
        incident_cooldown=timedelta(minutes=float(values.get("incident_cooldown_minutes", 30))),
        scheduled_window=timedelta(minutes=float(values.get("controls.scheduled_window_minutes", 60))),
        near_close_window=timedelta(minutes=float(values.get("controls.near_close_window_minutes", 60))),
        thin_liquidity_threshold=float(values.get("controls.thin_liquidity_threshold", 100.0)),
        sibling_control_ratio=float(values.get("controls.sibling_control_ratio", 0.25)),
    )
    return CausalActivityDetector(config)


def create_shadow_run(
    *,
    input_path: str | Path,
    run_dir: str | Path,
    config_path: str | Path,
    as_of: datetime,
    coverage_status: str,
    coverage_limitations: Sequence[str] = (),
    random_seed: int = 20260712,
    control_sampling_rate: float = 0.05,
    repo_root: str | Path = ".",
) -> ShadowRunManifestV2:
    """Freeze caller-prepared outcomes for engineering harness tests only."""

    input_file = Path(input_path)
    config_file = Path(config_path)
    if not input_file.is_file() or not config_file.is_file():
        raise FileNotFoundError("shadow input and config files must exist")
    records = _load_shadow_records(input_file)
    watermark = max((record.event_time for record in records), default=as_of)
    manifest = build_shadow_manifest(
        repo_root=repo_root,
        config=config_file.read_bytes(),
        schema={"canonical_domain": "2.0.0", "targets": ["A", "B", "C"]},
        model={"type": "untrained_rank", "effectiveness_unknown": True},
        detector={"name": "causal_activity_v2", "scores_are_probabilities": False},
        evidence={"name": "point_in_time_v1", "absence_requires_coverage": True},
        graph={"name": "independent_claims_v1", "circular_edges_forbidden": True},
        schema_version="2.0.0",
        model_version="untrained-rank-v1",
        detector_version="causal-v2",
        evidence_version="point-in-time-v1",
        graph_version="independent-claims-v1",
        random_seed=random_seed,
        control_sampling_rate=control_sampling_rate,
        input_partitions={"records": input_file},
        source_high_watermarks={"input": min(watermark, as_of)},
        source_coverage={
            "input": {"status": coverage_status, "limitations": tuple(coverage_limitations)}
        },
        as_of=as_of,
        started_at=as_of,
    )
    ShadowLedger(run_dir, manifest)  # writes and freezes manifest.json
    return manifest


def _load_manifest(run_dir: str | Path) -> ShadowRunManifestV2:
    path = Path(run_dir) / "manifest.json"
    if not path.is_file():
        raise FileNotFoundError(f"shadow manifest not found: {path}")
    return ShadowRunManifestV2.model_validate(json.loads(path.read_text(encoding="utf-8")))


def run_shadow_cycle(*, run_dir: str | Path, input_path: str | Path) -> dict[str, Any]:
    """Run the legacy prepared-input harness for engineering tests only."""

    manifest = _load_manifest(run_dir)
    input_file = Path(input_path)
    expected = manifest.input_partition_hashes.get("records")
    if expected is None or hash_file(input_file) != expected:
        raise ValueError("shadow input hash does not match frozen manifest")
    records = _load_shadow_records(input_file)
    ledger = ShadowLedger(run_dir, manifest)

    def evaluator(record: ShadowInputRecord, _manifest: ShadowRunManifestV2) -> Mapping[str, Any]:
        assessment = record.payload.get("assessment")
        return {
            "is_alert": bool(record.payload.get("is_alert", False)),
            "assessment": assessment,
            "source": "engineering_test_only_prepared_input",
            "not_proof_of_fraud": True,
        }

    result = ShadowRunner(manifest=manifest, ledger=ledger, evaluator=evaluator).run(records)
    verified = ledger.verify()
    return {
        "command": "shadow-test-run",
        **result.model_dump(mode="json", exclude={"assessment_payload_bytes"}),
        "verified_ledger_entries": len(verified),
        "ledger_verified": True,
        "run_dir": str(run_dir),
        "prepared_input_engineering_test_only": True,
        "caller_supplied_outcomes_trusted": True,
    }


def run_prospective_shadow_cycle(
    *,
    data_root: str | Path | None,
    run_dir: str | Path,
    config_path: str | Path,
    detector_config_path: str | Path,
    as_of: datetime,
    coverage_status: str = "unknown",
    coverage_limitations: Sequence[str] = (),
    random_seed: int = 20260712,
    control_sampling_rate: float = 0.05,
    repo_root: str | Path = ".",
    input_snapshot: CanonicalInputSnapshot | None = None,
) -> dict[str, Any]:
    """Freeze canonical collection state and derive alerts through the v2 pipeline."""

    config_file = Path(config_path)
    detector_file = Path(detector_config_path)
    if not config_file.is_file() or not detector_file.is_file():
        raise FileNotFoundError("shadow and detector config files must exist")
    config_bytes = config_file.read_bytes()
    detector_bytes = detector_file.read_bytes()
    snapshot = input_snapshot or load_canonical_inputs(data_root, as_of=as_of)
    if input_snapshot is not None and data_root is not None:
        if Path(data_root).resolve() != input_snapshot.root:
            raise ValueError("input_snapshot root does not match data_root")
    if snapshot.as_of != as_of.astimezone(UTC):
        raise ValueError("input_snapshot as_of does not match the prospective run cutoff")
    partitions = {
        partition.name: partition.payload
        for partition in snapshot.partitions
    }
    code_hashes = {
        path.relative_to(Path(repo_root).resolve()).as_posix(): hash_file(path)
        for path in sorted((Path(repo_root).resolve() / "marketleak").rglob("*.py"))
    }
    manifest = build_shadow_manifest(
        repo_root=repo_root,
        config=config_bytes,
        schema={"canonical_domain": "2.0.0", "targets": ["A", "B", "C"]},
        model={"type": "untrained_rank", "effectiveness_unknown": True},
        detector={
            "name": "causal_activity_v2",
            "scores_are_probabilities": False,
            "config_sha256": hashlib.sha256(detector_bytes).hexdigest(),
            "code_sha256": code_hashes,
        },
        evidence={"name": "point_in_time_v1", "absence_requires_coverage": True},
        graph={"name": "independent_claims_v1", "circular_edges_forbidden": True},
        schema_version="2.0.0",
        model_version="untrained-rank-v1",
        detector_version="causal-v2",
        evidence_version="point-in-time-v1",
        graph_version="independent-claims-v1",
        random_seed=random_seed,
        control_sampling_rate=control_sampling_rate,
        input_partitions=partitions,
        source_high_watermarks=snapshot.source_high_watermarks,
        source_coverage={
            "canonical_normalized_store": {
                "status": coverage_status,
                "limitations": tuple(coverage_limitations),
            }
        },
        as_of=as_of,
        started_at=as_of,
    )
    ledger = ShadowLedger(run_dir, manifest)
    pipeline = ValidationFirstPipeline(detector=_detector_from_config(detector_bytes))
    pipeline_result = pipeline.run(
        observations=snapshot.observations,
        fills=snapshot.fills,
        books=snapshot.books,
        quality_report=snapshot.quality_report,
        as_of=as_of,
        pipeline_source=CANONICAL_PIPELINE_SOURCE,
        run_started_at=as_of,
        run_identity=manifest.manifest_hash,
    )
    entries = []
    for assessment in pipeline_result.assessments:
        entries.append(
            ledger.append_assessment(
                assessment.assessment_uid,
                {
                    "assessment": assessment.model_dump(mode="json"),
                    "pipeline_source": CANONICAL_PIPELINE_SOURCE,
                    "pipeline_run_uid": pipeline_result.run_uid,
                    "pipeline_status": pipeline_result.status,
                    "as_of": as_of,
                    "not_proof_of_fraud": True,
                    "effectiveness_unknown": True,
                },
            )
        )
    if not entries:
        entries.append(
            ledger.append_control_sample(
                pipeline_result.run_uid,
                {
                    "assessment": {
                        "status": pipeline_result.status,
                        "review_candidate_count": 0,
                        "not_scorable": pipeline_result.status.startswith("not_scorable"),
                    },
                    "pipeline_source": CANONICAL_PIPELINE_SOURCE,
                    "pipeline_run_uid": pipeline_result.run_uid,
                    "as_of": as_of,
                    "not_proof_of_fraud": True,
                    "effectiveness_unknown": True,
                },
            )
        )
    verified = ledger.verify()
    return {
        "command": "shadow-prospective",
        "shadow_run_uid": manifest.shadow_run_uid,
        "manifest_hash": manifest.manifest_hash,
        "pipeline_run_uid": pipeline_result.run_uid,
        "pipeline_source": CANONICAL_PIPELINE_SOURCE,
        "pipeline_status": pipeline_result.status,
        "data_root": str(snapshot.root),
        "partition_count": len(snapshot.partition_paths),
        "source_high_watermarks": dict(snapshot.source_high_watermarks),
        "evaluated_count": snapshot.eligible_count,
        "future_excluded_count": snapshot.future_excluded_count,
        "alert_count": len(pipeline_result.assessments),
        "assessment_count": len(pipeline_result.assessments),
        "assessments": pipeline_result.assessments_payload(),
        "data_quality": dict(pipeline_result.data_quality),
        "coverage_complete": manifest.coverage_complete,
        "coverage_gaps": [
            source for source, state in manifest.source_coverage.items() if not state.complete
        ],
        "ledger_entries": len(verified),
        "ledger_tail_hash": ledger.tail_hash,
        "ledger_verified": True,
        "run_dir": str(run_dir),
        "caller_supplied_outcomes_accepted": False,
        "not_proof_of_fraud": True,
        "effectiveness_unknown": True,
    }


def verify_shadow_run(run_dir: str | Path) -> dict[str, Any]:
    manifest = _load_manifest(run_dir)
    ledger = ShadowLedger(run_dir, manifest)
    entries = ledger.verify()
    return {
        "command": "shadow-verify",
        "shadow_run_uid": manifest.shadow_run_uid,
        "manifest_hash": manifest.manifest_hash,
        "ledger_entries": len(entries),
        "ledger_tail_hash": ledger.tail_hash,
        "ledger_verified": True,
        "frozen": manifest.frozen,
        "effectiveness_unknown": manifest.effectiveness_unknown,
    }


def _add_shadow_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--input",
        required=True,
        help="ENGINEERING TEST ONLY: prepared ShadowInputRecord JSON/JSONL with caller outcomes",
    )
    parser.add_argument("--run-dir", required=True, help="Immutable output directory for this run")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="marketleak-v2", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("capabilities", help="Print supported claims and explicit limitations")

    audit = commands.add_parser("audit-legacy", help="Audit the current legacy fixture through v2")
    audit.add_argument("--ticks", default="demo_data/ticks.parquet")

    collect = commands.add_parser("collect-once", help="Run bounded official public-source collection")
    collect.add_argument("--output-dir", default="data/v2")
    collect.add_argument("--platform", choices=("all", "polymarket", "kalshi"), default="all")
    collect.add_argument("--polymarket-market")
    collect.add_argument("--polymarket-token")
    collect.add_argument("--kalshi-ticker")
    collect.add_argument("--start", help="Inclusive ISO-8601 UTC lower bound")
    collect.add_argument("--end", help="Inclusive ISO-8601 UTC upper bound")
    collect.add_argument("--page-size", type=int, default=100)
    collect.add_argument("--max-pages", type=int, choices=range(1, 101), default=1)

    wallet = commands.add_parser(
        "collect-polymarket-wallet",
        help="Collect a bounded, resumable public Polymarket wallet-trade history",
    )
    wallet.add_argument("--output-dir", default="data/v2")
    wallet.add_argument("--user", required=True, help="Public Polymarket profile wallet address")
    wallet.add_argument("--start", help="Inclusive ISO-8601 UTC lower bound; defaults to full user history")
    wallet.add_argument("--end", help="Inclusive ISO-8601 UTC upper bound")
    wallet.add_argument("--continuation", help="Opaque continuation returned by a previous invocation")
    wallet.add_argument("--page-size", type=int, default=1000)
    wallet.add_argument("--max-pages", type=int, choices=range(1, 101), default=1)
    wallet.add_argument(
        "--taker-only",
        action="store_true",
        help="Request taker-side records only; by default both maker and taker roles are collected",
    )

    evidence = commands.add_parser(
        "collect-evidence-once",
        help="Run bounded point-in-time collection from an explicit source config",
    )
    evidence.add_argument("--config", required=True, help="Explicit JSON public-source config")
    evidence.add_argument("--output-dir", required=True, help="Persistent raw/normalized/coverage root")
    evidence.add_argument("--max-pages", type=int, choices=range(1, 11), default=1)
    evidence.add_argument("--max-entries", type=int, default=100, help="Bounded to [1, 1000]")
    evidence.add_argument("--timeout-seconds", type=float, default=10.0)

    prospective = commands.add_parser(
        "shadow-prospective",
        help="Run one genuine prospective cycle from canonical collected partitions",
    )
    prospective.add_argument("--data-root", help="Canonical collection root (defaults to MARKETLEAK_V2_DATA_ROOT, data/v2, or data)")
    prospective.add_argument("--run-dir", required=True, help="Immutable output directory for this run")
    prospective.add_argument("--config", default="configs/shadow/v1.yaml")
    prospective.add_argument("--detector-config", default="configs/detector/v2.yaml")
    prospective.add_argument("--as-of", required=True, help="Frozen ISO-8601 cutoff")
    prospective.add_argument(
        "--coverage-status", choices=("complete", "partial", "unavailable", "unknown"),
        default="unknown",
    )
    prospective.add_argument("--coverage-limitation", action="append", default=[])
    prospective.add_argument("--random-seed", type=int, default=20260712)
    prospective.add_argument("--control-sampling-rate", type=float, default=0.05)

    create = commands.add_parser(
        "shadow-create",
        help="ENGINEERING TEST ONLY: freeze prepared caller-supplied outcomes",
    )
    _add_shadow_common(create)
    create.add_argument("--config", default="configs/shadow/v1.yaml")
    create.add_argument("--as-of", required=True, help="Frozen ISO-8601 cutoff")
    create.add_argument(
        "--coverage-status", choices=("complete", "partial", "unavailable", "unknown"),
        default="unknown",
    )
    create.add_argument("--coverage-limitation", action="append", default=[])
    create.add_argument("--random-seed", type=int, default=20260712)
    create.add_argument("--control-sampling-rate", type=float, default=0.05)

    run = commands.add_parser(
        "shadow-run",
        help="ENGINEERING TEST ONLY: evaluate prepared caller-supplied outcomes",
    )
    _add_shadow_common(run)

    cycle = commands.add_parser(
        "shadow-cycle",
        help="ENGINEERING TEST ONLY: create/run prepared caller-supplied outcomes",
    )
    _add_shadow_common(cycle)
    cycle.add_argument("--config", default="configs/shadow/v1.yaml")
    cycle.add_argument("--as-of", required=True)
    cycle.add_argument(
        "--coverage-status", choices=("complete", "partial", "unavailable", "unknown"),
        default="unknown",
    )
    cycle.add_argument("--coverage-limitation", action="append", default=[])
    cycle.add_argument("--random-seed", type=int, default=20260712)
    cycle.add_argument("--control-sampling-rate", type=float, default=0.05)

    verify = commands.add_parser("shadow-verify", help="Verify manifest and ledger hash chain")
    verify.add_argument("--run-dir", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "capabilities":
        payload = capabilities_payload()
    elif args.command == "audit-legacy":
        payload = audit_legacy_fixture(args.ticks)
    elif args.command == "collect-once":
        payload = collect_once(
            output_dir=args.output_dir,
            platform=args.platform,
            polymarket_market=args.polymarket_market,
            polymarket_token=args.polymarket_token,
            kalshi_ticker=args.kalshi_ticker,
            start=_datetime(args.start) if args.start else None,
            end=_datetime(args.end) if args.end else None,
            page_size=args.page_size,
            max_pages=args.max_pages,
        )
    elif args.command == "collect-polymarket-wallet":
        payload = collect_polymarket_wallet_history(
            output_dir=args.output_dir,
            user=args.user,
            start=_datetime(args.start) if args.start else None,
            end=_datetime(args.end) if args.end else None,
            continuation=args.continuation,
            page_size=args.page_size,
            max_pages=args.max_pages,
            taker_only=args.taker_only,
        )
    elif args.command == "collect-evidence-once":
        payload = collect_evidence_once(
            config_path=args.config,
            output_dir=args.output_dir,
            max_pages=args.max_pages,
            max_entries=args.max_entries,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "shadow-prospective":
        payload = run_prospective_shadow_cycle(
            data_root=args.data_root,
            run_dir=args.run_dir,
            config_path=args.config,
            detector_config_path=args.detector_config,
            as_of=_datetime(args.as_of),
            coverage_status=args.coverage_status,
            coverage_limitations=args.coverage_limitation,
            random_seed=args.random_seed,
            control_sampling_rate=args.control_sampling_rate,
        )
    elif args.command in {"shadow-create", "shadow-cycle"}:
        manifest = create_shadow_run(
            input_path=args.input,
            run_dir=args.run_dir,
            config_path=args.config,
            as_of=_datetime(args.as_of),
            coverage_status=args.coverage_status,
            coverage_limitations=args.coverage_limitation,
            random_seed=args.random_seed,
            control_sampling_rate=args.control_sampling_rate,
        )
        payload = {
            "command": "shadow-test-create",
            "shadow_run_uid": manifest.shadow_run_uid,
            "manifest_hash": manifest.manifest_hash,
            "run_dir": args.run_dir,
            "frozen": True,
            "effectiveness_unknown": True,
            "prepared_input_engineering_test_only": True,
        }
        if args.command == "shadow-cycle":
            payload = run_shadow_cycle(run_dir=args.run_dir, input_path=args.input)
    elif args.command == "shadow-run":
        payload = run_shadow_cycle(run_dir=args.run_dir, input_path=args.input)
    else:
        payload = verify_shadow_run(args.run_dir)
    _emit(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
