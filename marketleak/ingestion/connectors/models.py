"""Connector result envelopes keep data, lineage, gaps, and errors together."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, Iterator, TypeVar

from marketleak.domain import OrderBookSnapshot, PriceObservation, RawArtifact, TradeFill

from ..coverage import CapabilityMetadata
from ..quality import DataQualityReport
from ..raw_store import RawCapture

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ConnectorPage(Generic[T]):
    records: tuple[T, ...]
    raw_artifact: RawArtifact
    continuation: str | None
    complete: bool
    # Optional for backward compatibility with non-v2 connectors. Population
    # acquisition requires this receipt-bearing envelope and fails closed when
    # it is absent.
    raw_capture: RawCapture | None = None
    # Ordered HTTP response attempts, including retryable failures and ending
    # with ``raw_capture``.  Empty remains supported for legacy connectors.
    raw_attempt_captures: tuple[RawCapture, ...] = ()

    def __post_init__(self) -> None:
        attempts = tuple(self.raw_attempt_captures)
        if self.raw_capture is not None:
            if not attempts:
                attempts = (self.raw_capture,)
            elif attempts[-1] != self.raw_capture:
                raise ValueError("raw_attempt_captures must end with raw_capture")
        elif attempts:
            raise ValueError("raw_attempt_captures require a final raw_capture")
        if len({item.receipt_path for item in attempts}) != len(attempts):
            raise ValueError("raw_attempt_captures must preserve distinct receipts")
        object.__setattr__(self, "raw_attempt_captures", attempts)


@dataclass(slots=True)
class IngestionBatch:
    fills: list[TradeFill] = field(default_factory=list)
    observations: list[PriceObservation] = field(default_factory=list)
    snapshots: list[OrderBookSnapshot] = field(default_factory=list)
    raw_artifacts: list[RawArtifact] = field(default_factory=list)
    raw_captures: list[RawCapture] = field(default_factory=list)
    capabilities: list[CapabilityMetadata] = field(default_factory=list)
    quality: DataQualityReport | None = None
    continuation: str | None = None
    complete: bool = True

    @property
    def records(self) -> tuple[TradeFill | PriceObservation | OrderBookSnapshot, ...]:
        return (*self.fills, *self.observations, *self.snapshots)
