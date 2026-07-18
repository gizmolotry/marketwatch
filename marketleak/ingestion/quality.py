"""Auditable data-quality accounting and explicit promotion gates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(slots=True)
class DataQualityReport:
    source: str
    received: int = 0
    normalized: int = 0
    invalid: int = 0
    duplicates: int = 0
    conflicts: int = 0
    raw_hash_failures: int = 0
    missing_required: dict[str, int] = field(default_factory=dict)
    out_of_order: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def invalid_rate(self) -> float:
        return self.invalid / self.received if self.received else 0.0

    @property
    def conflict_rate(self) -> float:
        return self.conflicts / self.received if self.received else 0.0

    @property
    def duplicate_rate(self) -> float:
        return self.duplicates / self.received if self.received else 0.0

    def missing_rate(self, field_name: str) -> float:
        count = self.missing_required.get(field_name, 0)
        return count / self.received if self.received else 0.0

    def record_missing(self, field_name: str) -> None:
        self.missing_required[field_name] = self.missing_required.get(field_name, 0) + 1

    def merge(self, other: "DataQualityReport") -> "DataQualityReport":
        if self.source != other.source:
            raise ValueError("quality reports must have the same source")
        merged_missing = dict(self.missing_required)
        for name, count in other.missing_required.items():
            merged_missing[name] = merged_missing.get(name, 0) + count
        return DataQualityReport(
            source=self.source,
            received=self.received + other.received,
            normalized=self.normalized + other.normalized,
            invalid=self.invalid + other.invalid,
            duplicates=self.duplicates + other.duplicates,
            conflicts=self.conflicts + other.conflicts,
            raw_hash_failures=self.raw_hash_failures + other.raw_hash_failures,
            missing_required=merged_missing,
            out_of_order=self.out_of_order + other.out_of_order,
            notes=[*self.notes, *other.notes],
        )


@dataclass(frozen=True, slots=True)
class GateResult:
    passed: bool
    reasons: tuple[str, ...]

    def require(self) -> None:
        if not self.passed:
            raise DataQualityError("; ".join(self.reasons))


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DataQualityGate:
    min_normalized: int = 1
    max_invalid_rate: float = 0.01
    max_conflicts: int = 0
    max_raw_hash_failures: int = 0
    max_out_of_order_rate: float = 0.01
    max_missing_rates: Mapping[str, float] = field(default_factory=dict)

    def evaluate(self, report: DataQualityReport) -> GateResult:
        reasons: list[str] = []
        if report.normalized < self.min_normalized:
            reasons.append(f"normalized={report.normalized} below minimum {self.min_normalized}")
        if report.invalid_rate > self.max_invalid_rate:
            reasons.append(f"invalid_rate={report.invalid_rate:.6f} exceeds {self.max_invalid_rate:.6f}")
        if report.conflicts > self.max_conflicts:
            reasons.append(f"conflicts={report.conflicts} exceeds {self.max_conflicts}")
        if report.raw_hash_failures > self.max_raw_hash_failures:
            reasons.append(
                f"raw_hash_failures={report.raw_hash_failures} exceeds {self.max_raw_hash_failures}"
            )
        out_of_order_rate = report.out_of_order / report.received if report.received else 0.0
        if out_of_order_rate > self.max_out_of_order_rate:
            reasons.append(
                f"out_of_order_rate={out_of_order_rate:.6f} exceeds {self.max_out_of_order_rate:.6f}"
            )
        for name, maximum in self.max_missing_rates.items():
            rate = report.missing_rate(name)
            if rate > maximum:
                reasons.append(f"missing_rate[{name}]={rate:.6f} exceeds {maximum:.6f}")
        return GateResult(not reasons, tuple(reasons))

