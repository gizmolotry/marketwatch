"""Deterministic shadow evaluator with frozen low-risk control sampling."""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Callable, Iterable, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

from marketleak.shadow.ledger import FrozenRunMutationError, LedgerEntry, ShadowLedger
from marketleak.shadow.manifest import ShadowRunManifestV2, canonical_json_bytes, utc_datetime


class ShadowInputRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    record_uid: str = Field(min_length=1)
    event_time: datetime
    risk_tier: Literal["low", "standard", "high"] = "standard"
    payload: dict[str, Any]

    @field_validator("event_time", mode="before")
    @classmethod
    def _event_time_is_utc(cls, value):
        return utc_datetime(value, field_name="event_time")


class ShadowRunResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    shadow_run_uid: str
    evaluated_count: int = Field(ge=0)
    future_excluded_count: int = Field(ge=0)
    alert_count: int = Field(ge=0)
    control_sample_count: int = Field(ge=0)
    control_record_uids: tuple[str, ...]
    coverage_complete: bool
    coverage_gaps: tuple[str, ...]
    ledger_tail_hash: str
    assessment_payload_bytes: tuple[bytes, ...]
    engineering_validation_only: Literal[True] = True
    effectiveness_unknown: Literal[True] = True


Evaluator = Callable[[ShadowInputRecord, ShadowRunManifestV2], Mapping[str, Any] | None]


def _sample_low_risk(record_uid: str, *, seed: int, rate: float) -> bool:
    if rate <= 0.0:
        return False
    if rate >= 1.0:
        return True
    digest = hashlib.sha256(f"{seed}|{record_uid}".encode("utf-8")).digest()
    draw = int.from_bytes(digest[:8], "big") / float(2**64)
    return draw < rate


class ShadowRunner:
    def __init__(
        self,
        *,
        manifest: ShadowRunManifestV2,
        ledger: ShadowLedger,
        evaluator: Evaluator,
    ):
        ledger.assert_manifest(manifest)
        self.manifest = manifest
        self.ledger = ledger
        self.evaluator = evaluator

    def run(self, records: Iterable[ShadowInputRecord | Mapping[str, Any]]) -> ShadowRunResult:
        normalized = [
            item if isinstance(item, ShadowInputRecord) else ShadowInputRecord.model_validate(item)
            for item in records
        ]
        normalized.sort(key=lambda item: (item.event_time, item.record_uid))
        seen: set[str] = set()
        payload_by_uid: dict[str, bytes] = {}
        eligible: list[ShadowInputRecord] = []
        future_excluded = 0
        for item in normalized:
            if item.record_uid in seen:
                if payload_by_uid[item.record_uid] != canonical_json_bytes(item):
                    raise FrozenRunMutationError(
                        f"Conflicting shadow inputs share record_uid {item.record_uid}."
                    )
                continue
            seen.add(item.record_uid)
            payload_by_uid[item.record_uid] = canonical_json_bytes(item)
            if item.event_time > self.manifest.as_of:
                future_excluded += 1
                continue
            eligible.append(item)

        entries: list[LedgerEntry] = []
        payload_bytes: list[bytes] = []
        control_uids: list[str] = []
        alert_count = 0
        for record in eligible:
            evaluation = self.evaluator(record, self.manifest)
            evaluation_payload = dict(evaluation or {})
            is_alert = bool(evaluation_payload.get("is_alert", evaluation is not None))
            if is_alert:
                payload = {
                    "record_uid": record.record_uid,
                    "event_time": record.event_time,
                    "assessment": evaluation_payload,
                    "engineering_validation_only": True,
                    "effectiveness_unknown": True,
                }
                entries.append(self.ledger.append_assessment(record.record_uid, payload))
                payload_bytes.append(canonical_json_bytes(payload))
                alert_count += 1
            elif record.risk_tier == "low" and _sample_low_risk(
                record.record_uid,
                seed=self.manifest.random_seed,
                rate=self.manifest.control_sampling_rate,
            ):
                payload = {
                    "record_uid": record.record_uid,
                    "event_time": record.event_time,
                    "input": record.payload,
                    "evaluation": evaluation_payload,
                    "sampling": {
                        "strategy": "fixed_seed_low_risk_control",
                        "random_seed": self.manifest.random_seed,
                        "rate": self.manifest.control_sampling_rate,
                    },
                    "engineering_validation_only": True,
                    "effectiveness_unknown": True,
                }
                entries.append(self.ledger.append_control_sample(record.record_uid, payload))
                payload_bytes.append(canonical_json_bytes(payload))
                control_uids.append(record.record_uid)

        coverage_gaps = tuple(
            source
            for source, state in sorted(self.manifest.source_coverage.items())
            if not state.complete
        )
        return ShadowRunResult(
            shadow_run_uid=self.manifest.shadow_run_uid,
            evaluated_count=len(eligible),
            future_excluded_count=future_excluded,
            alert_count=alert_count,
            control_sample_count=len(control_uids),
            control_record_uids=tuple(control_uids),
            coverage_complete=self.manifest.coverage_complete,
            coverage_gaps=coverage_gaps,
            ledger_tail_hash=entries[-1].entry_hash if entries else self.ledger.tail_hash,
            assessment_payload_bytes=tuple(payload_bytes),
        )


__all__ = ["ShadowInputRecord", "ShadowRunResult", "ShadowRunner"]
