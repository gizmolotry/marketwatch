"""Validation-first orchestration for the v2 surveillance pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from marketleak.detectors import CausalActivityDetector, DetectionBatch, DetectorConfig
from marketleak.domain import (
    ActivitySignal,
    ActivityStatus,
    ActorEvidence,
    ActorEvidenceStatus,
    ActorVisibility,
    IntegrityAssessment,
    OrderBookSnapshot,
    PriceObservation,
    PublicExplanation,
    PublicExplanationStatus,
    TradeFill,
)
from marketleak.graph.scoring import ActorAccessAssessment, score_actor_access
from marketleak.ingestion.quality import DataQualityGate, DataQualityReport, GateResult
from marketleak.ingestion.normalize import canonical_json_bytes


CAPABILITIES_V2: dict[str, Any] = {
    "schema_version": "2.0.0",
    "targets": {
        "A": "abnormal_market_activity",
        "B": "point_in_time_public_explanation",
        "C": "actor_specific_access_context",
    },
    "targets_are_independent": True,
    "scores_are_probabilities": False,
    "fraud_prediction": False,
    "automatic_fraud_finding": False,
    "human_review_required": True,
    "not_proof_of_fraud": True,
    "effectiveness_unknown": True,
}


CANONICAL_PIPELINE_SOURCE = "canonical_normalized_partitions"
CANONICAL_RECORD_TYPES: dict[str, type[PriceObservation | TradeFill | OrderBookSnapshot]] = {
    "price_observation": PriceObservation,
    "trade_fill": TradeFill,
    "orderbook_snapshot": OrderBookSnapshot,
}


@dataclass(frozen=True, slots=True)
class CanonicalPartitionSnapshot:
    name: str
    source_path: Path
    payload: bytes
    record_type: str


@dataclass(frozen=True, slots=True)
class CanonicalInputSnapshot:
    root: Path
    as_of: datetime
    observations: tuple[PriceObservation, ...]
    fills: tuple[TradeFill, ...]
    books: tuple[OrderBookSnapshot, ...]
    partitions: tuple[CanonicalPartitionSnapshot, ...]
    source_high_watermarks: Mapping[str, datetime]
    quality_report: DataQualityReport
    future_excluded_count: int

    @property
    def eligible_count(self) -> int:
        return len(self.observations) + len(self.fills) + len(self.books)

    @property
    def partition_paths(self) -> tuple[Path, ...]:
        """Compatibility view; callers must hash ``partitions[*].payload`` instead."""

        return tuple(partition.source_path for partition in self.partitions)


def canonical_data_root(root: str | Path | None = None) -> Path:
    """Resolve the canonical normalized store without selecting legacy fixtures."""

    if root is not None:
        return Path(root)
    configured = os.getenv("MARKETLEAK_V2_DATA_ROOT")
    if configured:
        return Path(configured)
    for candidate in (Path("data/v2"), Path("data")):
        if any(
            path
            for kind in CANONICAL_RECORD_TYPES
            for path in (candidate / "normalized" / kind).rglob("*.json")
        ):
            return candidate
    return Path("data/v2")


def _raw_digest(raw_artifact_uid: str) -> str | None:
    candidate = raw_artifact_uid.rsplit("/", 1)[-1].lower()
    if len(candidate) == 64 and all(character in "0123456789abcdef" for character in candidate):
        return candidate
    return None


def _raw_artifact_is_valid(root: Path, raw_artifact_uid: str) -> bool:
    digest = _raw_digest(raw_artifact_uid)
    if digest is None:
        return False
    path = root / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"
    if not path.is_file():
        return False
    file_hash = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            file_hash.update(block)
    return file_hash.hexdigest() == digest


def _partition_date(path: Path) -> date | None:
    for part in path.parts:
        if part.startswith("date="):
            try:
                return datetime.strptime(part.removeprefix("date="), "%Y-%m-%d").date()
            except ValueError:
                return None
    return None


def _payload_event_time(payload: Any) -> datetime | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("event_time")
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def load_canonical_inputs(
    root: str | Path | None = None,
    *,
    as_of: datetime | None = None,
) -> CanonicalInputSnapshot:
    """Read, validate, and causally freeze collected normalized partitions."""

    resolved_root = canonical_data_root(root).resolve()
    cutoff = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
    records: dict[str, list[Any]] = {kind: [] for kind in CANONICAL_RECORD_TYPES}
    partitions: list[CanonicalPartitionSnapshot] = []
    high_watermarks: dict[str, datetime] = {}
    quality = DataQualityReport(source=f"canonical:{resolved_root.as_posix()}")
    future_excluded = 0
    candidate_count = 0
    seen: dict[tuple[str, str], bytes] = {}
    raw_validity: dict[str, bool] = {}

    for kind, model in CANONICAL_RECORD_TYPES.items():
        kind_root = resolved_root / "normalized" / kind
        for path in sorted(kind_root.rglob("*.json")) if kind_root.is_dir() else ():
            candidate_count += 1
            # Read each candidate exactly once. All later parsing and manifest
            # hashing use this immutable byte snapshot, never the live path.
            raw = path.read_bytes()
            partition_name = f"canonical:{path.relative_to(resolved_root).as_posix()}"
            declared_date = _partition_date(path)
            if declared_date is not None and declared_date > cutoff.date():
                # NormalizedStore is one-record-per-file and date-partitioned by
                # event_time. A future date partition is outside this causal run
                # even if its bytes are malformed or its raw artifact is absent.
                future_excluded += 1
                continue
            try:
                decoded = json.loads(raw)
            except Exception:
                partitions.append(CanonicalPartitionSnapshot(partition_name, path, raw, kind))
                quality.received += 1
                quality.invalid += 1
                continue
            if isinstance(decoded, list):
                # The canonical store contract is one record per JSON file.
                # Future-only arrays can be causally ignored; any array with a
                # past/unknown member is frozen and rejected as one invalid
                # partition instead of ambiguously mixing cutoffs.
                event_times = [_payload_event_time(item) for item in decoded]
                if decoded and all(item is not None and item > cutoff for item in event_times):
                    future_excluded += len(decoded)
                    continue
                partitions.append(CanonicalPartitionSnapshot(partition_name, path, raw, kind))
                quality.received += 1
                quality.invalid += 1
                quality.notes.append(f"rejected multi-record canonical partition: {partition_name}")
                continue
            event_time = _payload_event_time(decoded)
            if event_time is not None and event_time > cutoff:
                future_excluded += 1
                continue
            partitions.append(CanonicalPartitionSnapshot(partition_name, path, raw, kind))
            quality.received += 1
            try:
                record = model.model_validate_json(raw)
            except Exception:
                quality.invalid += 1
                continue
            if canonical_json_bytes(record) != raw:
                quality.invalid += 1
                quality.notes.append(f"non-canonical normalized encoding: {partition_name}")
                continue
            uid_name = {
                "price_observation": "observation_uid",
                "trade_fill": "fill_uid",
                "orderbook_snapshot": "snapshot_uid",
            }[kind]
            uid = str(getattr(record, uid_name))
            identity = (kind, uid)
            if identity in seen:
                if seen[identity] == raw:
                    quality.duplicates += 1
                else:
                    quality.conflicts += 1
                continue
            seen[identity] = raw
            raw_uid = str(record.raw_artifact_uid)
            if raw_uid not in raw_validity:
                raw_validity[raw_uid] = _raw_artifact_is_valid(resolved_root, raw_uid)
            if not raw_validity[raw_uid]:
                quality.raw_hash_failures += 1
                continue
            records[kind].append(record)
            quality.normalized += 1
            watermark_key = f"{kind}:{record.platform}"
            previous = high_watermarks.get(watermark_key)
            if previous is None or record.event_time > previous:
                high_watermarks[watermark_key] = record.event_time

    if not candidate_count:
        quality.notes.append("canonical normalized partitions were not found; legacy fixtures were not used")
    elif not partitions:
        quality.notes.append("no canonical partitions were eligible at or before as_of; legacy fixtures were not used")
    if future_excluded:
        quality.notes.append(f"excluded {future_excluded} canonical records after as_of")
    if quality.raw_hash_failures:
        quality.notes.append("records with missing or hash-invalid raw artifacts were excluded")
    return CanonicalInputSnapshot(
        root=resolved_root,
        as_of=cutoff,
        observations=tuple(records["price_observation"]),
        fills=tuple(records["trade_fill"]),
        books=tuple(records["orderbook_snapshot"]),
        partitions=tuple(partitions),
        source_high_watermarks=high_watermarks,
        quality_report=quality,
        future_excluded_count=future_excluded,
    )


def _uid(prefix: str, *parts: str) -> str:
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest}"


def _quality_payload(report: DataQualityReport, gate: GateResult) -> dict[str, Any]:
    return {
        "source": report.source,
        "received": report.received,
        "normalized": report.normalized,
        "invalid": report.invalid,
        "duplicates": report.duplicates,
        "conflicts": report.conflicts,
        "raw_hash_failures": report.raw_hash_failures,
        "missing_required": dict(report.missing_required),
        "out_of_order": report.out_of_order,
        "invalid_rate": report.invalid_rate,
        "duplicate_rate": report.duplicate_rate,
        "gate_passed": gate.passed,
        "gate_reasons": list(gate.reasons),
        "notes": list(report.notes),
    }


def assess_legacy_tick_quality(frame: pd.DataFrame, *, source: str = "legacy:price-snapshots") -> DataQualityReport:
    """Account for legacy rows without pretending they contain fills or lineage."""

    received = len(frame)
    required = [name for name in ("tick_uid", "market_uid", "timestamp", "price") if name in frame.columns]
    missing_columns = [name for name in ("tick_uid", "market_uid", "timestamp", "price") if name not in frame.columns]
    invalid_mask = pd.Series(False, index=frame.index)
    if missing_columns:
        invalid_mask[:] = True
    elif received:
        invalid_mask = frame[required].isna().any(axis=1)
        numeric_price = pd.to_numeric(frame["price"], errors="coerce")
        numeric_time = pd.to_numeric(frame["timestamp"], errors="coerce")
        invalid_mask |= numeric_price.isna() | ~numeric_price.between(0.0, 1.0) | numeric_time.isna()
    valid = frame.loc[~invalid_mask]
    semantic_columns = [name for name in ("market_uid", "timestamp", "price") if name in valid.columns]
    duplicate_mask = valid.duplicated(subset=semantic_columns, keep="first") if semantic_columns else pd.Series(False, index=valid.index)
    missing_required = {name: received for name in missing_columns}
    # A Parquet snapshot row is not trusted lineage merely because the file is
    # local. These fields were never captured by the legacy schema, so every
    # row must be accounted for as missing rather than silently promoted.
    for lineage_field in ("raw_artifact_uid", "source_uid", "parser_version"):
        if lineage_field not in frame.columns:
            missing_required[lineage_field] = received
    return DataQualityReport(
        source=source,
        received=received,
        normalized=max(0, len(valid) - int(duplicate_mask.sum())),
        invalid=int(invalid_mask.sum()),
        duplicates=int(duplicate_mask.sum()),
        missing_required=missing_required,
        notes=[
            "legacy snapshots have no immutable raw-artifact lineage",
            "legacy snapshots do not establish trade or actor availability",
        ],
    )


@dataclass(frozen=True, slots=True)
class PipelineV2Result:
    run_uid: str
    started_at: datetime
    completed_at: datetime
    status: str
    data_quality: Mapping[str, Any]
    detector_batch: DetectionBatch
    assessments: tuple[IntegrityAssessment, ...]
    actor_availability: Mapping[str, Any]
    source_availability: Mapping[str, Any]
    validation_status: Mapping[str, Any]
    replay_detector_batch: DetectionBatch | None = None
    pipeline_source: str = "unspecified"

    def assessments_payload(self) -> list[dict[str, Any]]:
        return [item.model_dump(mode="json") for item in self.assessments]

    def run_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "2.0.0",
            "run_uid": self.run_uid,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "incident_count": len(self.detector_batch.incidents),
            "replay_incident_count": (
                len(self.replay_detector_batch.incidents) if self.replay_detector_batch is not None else 0
            ),
            "assessment_count": len(self.assessments),
            "pipeline_source": self.pipeline_source,
            "data_quality": dict(self.data_quality),
            "actor_availability": dict(self.actor_availability),
            "source_availability": dict(self.source_availability),
            "not_proof_of_fraud": True,
            "effectiveness_unknown": True,
        }


class ValidationFirstPipeline:
    def __init__(
        self,
        *,
        detector: CausalActivityDetector | None = None,
        quality_gate: DataQualityGate | None = None,
    ) -> None:
        self.detector = detector or CausalActivityDetector()
        self.quality_gate = quality_gate or DataQualityGate(
            min_normalized=1,
            max_invalid_rate=0.01,
            max_missing_rates={"raw_artifact_uid": 0.0, "source_uid": 0.0},
        )

    def run(
        self,
        *,
        observations: Iterable[Any] | None = None,
        fills: Iterable[Any] = (),
        books: Iterable[Any] = (),
        controls: Iterable[Any] | Mapping[str, Any] = (),
        ticks_path: str | Path = "demo_data/ticks.parquet",
        quality_report: DataQualityReport | None = None,
        public_explanations: Mapping[str, PublicExplanation] | None = None,
        actor_access: Mapping[str, ActorAccessAssessment] | None = None,
        evidence_graph: Any | None = None,
        observed_actors: Mapping[str, Any] | None = None,
        as_of: datetime | None = None,
        pipeline_source: str | None = None,
        run_started_at: datetime | None = None,
        run_identity: str | None = None,
    ) -> PipelineV2Result:
        wall_clock_start = datetime.now(timezone.utc)
        started_at = (run_started_at or wall_clock_start).astimezone(timezone.utc)
        cutoff = (as_of or wall_clock_start).astimezone(timezone.utc)
        if observations is None:
            frame = pd.read_parquet(ticks_path)
            quality_report = quality_report or assess_legacy_tick_quality(frame)
            from marketleak.detectors import detect_legacy_ticks

            detection = detect_legacy_ticks(frame, self.detector.config)
            resolved_pipeline_source = pipeline_source or "legacy_fixture_audit"
        else:
            observation_list = list(observations)
            quality_report = quality_report or DataQualityReport(
                source="canonical:observations",
                received=len(observation_list),
                normalized=len(observation_list),
            )
            detection = self.detector.detect(
                observation_list,
                fills=fills,
                books=books,
                controls=controls,
                as_of=cutoff,
            )
            resolved_pipeline_source = pipeline_source or "caller_supplied_canonical_records"
        gate = self.quality_gate.evaluate(quality_report)
        replay_detection = detection
        if not gate.passed:
            detection = DetectionBatch((), (), ())

        public_explanations = public_explanations or {}
        actor_access = dict(actor_access or {})
        observed_actors = observed_actors or {}
        if evidence_graph is not None:
            for incident in detection.incidents:
                if incident.market_uid in actor_access:
                    continue
                actor_access[incident.market_uid] = score_actor_access(
                    evidence_graph,
                    observed_actor=observed_actors.get(incident.market_uid),
                    event_uid=f"event:{incident.market_uid}",
                    as_of=cutoff,
                )

        assessments = tuple(
            self._assessment_for_incident(
                incident,
                detection,
                public_explanations=public_explanations,
                actor_access=actor_access,
                cutoff=cutoff,
            )
            for incident in detection.incidents
        )
        if not gate.passed:
            status = "blocked_by_data_quality"
        elif assessments:
            status = "completed_with_review_candidates"
        elif not detection.signals or all(
            signal.status == ActivityStatus.NOT_SCORABLE for signal in detection.signals
        ):
            status = "not_scorable_insufficient_history"
        else:
            status = "completed_no_actionable_activity"
        completed_at = datetime.now(timezone.utc)
        actor_available = bool(observed_actors or actor_access)
        source_available = bool(public_explanations)
        validation_status = {
            "validated": False,
            "effectiveness_unknown": True,
            "calibration_available": False,
            "prospective_shadow_runs_completed": 0,
            "labeled_case_count": 0,
            "claim_scope": "activity_surveillance_for_human_review",
            "not_proof_of_fraud": True,
        }
        return PipelineV2Result(
            run_uid=_uid(
                "run",
                run_identity or started_at.isoformat(),
                resolved_pipeline_source,
                str(quality_report.received),
                str(quality_report.normalized),
                str(len(detection.incidents)),
            ),
            started_at=started_at,
            completed_at=completed_at,
            status=status,
            data_quality=_quality_payload(quality_report, gate),
            detector_batch=detection,
            assessments=assessments,
            actor_availability={
                "available": actor_available,
                "visibility": "source_dependent" if actor_available else ActorVisibility.NOT_AVAILABLE.value,
                "derived_from_price_snapshots": False,
            },
            source_availability={
                "point_in_time_public_sources_available": source_available,
                "coverage_status": "provided" if source_available else "unknown_coverage",
            },
            validation_status=validation_status,
            replay_detector_batch=replay_detection,
            pipeline_source=resolved_pipeline_source,
        )

    def _assessment_for_incident(
        self,
        incident: Any,
        detection: DetectionBatch,
        *,
        public_explanations: Mapping[str, PublicExplanation],
        actor_access: Mapping[str, ActorAccessAssessment],
        cutoff: datetime,
    ) -> IntegrityAssessment:
        incident_signals = [
            item
            for item in detection.signals
            if item.incident_uid == incident.incident_uid
        ]
        peak = max(incident_signals, key=lambda item: item.score or 0.0)
        signal_uid = _uid("signal", incident.incident_uid, peak.bucket_time.isoformat())
        raw_uid = _uid("raw", incident.incident_uid)
        lineage = {
            "event_time": peak.bucket_time,
            "ingested_at": cutoff,
            "source_uid": "system:detector-v2",
            "raw_artifact_uid": raw_uid,
            "parser_version": "pipeline-v2",
        }
        activity = ActivitySignal(
            signal_uid=signal_uid,
            market_uid=incident.market_uid,
            outcome_uid=incident.outcome_uid,
            status=peak.status,
            detector_name="causal_activity_v2",
            detector_version="2.0.0",
            score=Decimal(str(peak.score)),
            threshold=Decimal(str(self.detector.config.fdr_alpha)),
            reason_codes=tuple(sorted({*peak.reason_codes, *peak.control_tags, "bh_fdr_significant"})),
            limitations=("The diagnostic score is not a probability of fraud.",),
            **lineage,
        )
        public = public_explanations.get(signal_uid) or public_explanations.get(incident.market_uid)
        if public is None:
            public = PublicExplanation(
                explanation_uid=_uid("explanation", signal_uid),
                signal_uid=signal_uid,
                status=PublicExplanationStatus.UNKNOWN_COVERAGE,
                shock_time=peak.bucket_time,
                summary="Point-in-time public-source coverage was not supplied for this run.",
                limitations=("No claim about absence of public information can be made.",),
                **lineage,
            )
        access = actor_access.get(incident.market_uid)
        if access is None:
            actor = ActorEvidence(
                actor_evidence_uid=_uid("actor-evidence", signal_uid),
                signal_uid=signal_uid,
                status=ActorEvidenceStatus.NO_ACTOR_DATA,
                actor_visibility=ActorVisibility.NOT_AVAILABLE,
                summary="No fill-attributed actor was available for this activity incident.",
                limitations=("Price observations cannot establish actor identity.",),
                **lineage,
            )
        else:
            visibility = ActorVisibility.PUBLIC_WALLET if access.actor_uid else ActorVisibility.NOT_AVAILABLE
            actor = ActorEvidence(
                actor_evidence_uid=_uid("actor-evidence", signal_uid),
                signal_uid=signal_uid,
                status=access.status,
                actor_visibility=visibility,
                actor_uid=access.actor_uid,
                evidence_uids=access.evidence_uids,
                summary="Actor access context was evaluated from independently sourced graph claims.",
                limitations=("Access context and trading activity do not establish intent or misconduct.",),
                **lineage,
            )
        return IntegrityAssessment(
            assessment_uid=_uid("assessment", signal_uid),
            market_uid=incident.market_uid,
            activity=activity,
            public_explanation=public,
            actor_evidence=actor,
            limitations=("Human review is required.",),
            **lineage,
        )


def run_validation_pipeline(
    *,
    canonical_root: str | Path | None = None,
    pipeline: ValidationFirstPipeline | None = None,
    **kwargs: Any,
) -> PipelineV2Result:
    """Run v2 from canonical collection by default; legacy replay is explicit only."""

    selected = pipeline or ValidationFirstPipeline()
    if "observations" in kwargs or "ticks_path" in kwargs:
        return selected.run(**kwargs)
    cutoff = kwargs.pop("as_of", None) or datetime.now(timezone.utc)
    snapshot = load_canonical_inputs(canonical_root, as_of=cutoff)
    return selected.run(
        observations=snapshot.observations,
        fills=snapshot.fills,
        books=snapshot.books,
        quality_report=snapshot.quality_report,
        as_of=cutoff,
        pipeline_source=CANONICAL_PIPELINE_SOURCE,
        **kwargs,
    )


__all__ = [
    "CAPABILITIES_V2",
    "CANONICAL_PIPELINE_SOURCE",
    "CanonicalInputSnapshot",
    "CanonicalPartitionSnapshot",
    "PipelineV2Result",
    "ValidationFirstPipeline",
    "assess_legacy_tick_quality",
    "canonical_data_root",
    "load_canonical_inputs",
    "run_validation_pipeline",
]
