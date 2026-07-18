"""Connector result envelopes keep data, lineage, gaps, and errors together."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, Iterator, TypeVar

from marketleak.domain import OrderBookSnapshot, PriceObservation, RawArtifact, TradeFill

from ..coverage import CapabilityMetadata
from ..quality import DataQualityReport

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ConnectorPage(Generic[T]):
    records: tuple[T, ...]
    raw_artifact: RawArtifact
    continuation: str | None
    complete: bool


@dataclass(slots=True)
class IngestionBatch:
    fills: list[TradeFill] = field(default_factory=list)
    observations: list[PriceObservation] = field(default_factory=list)
    snapshots: list[OrderBookSnapshot] = field(default_factory=list)
    raw_artifacts: list[RawArtifact] = field(default_factory=list)
    capabilities: list[CapabilityMetadata] = field(default_factory=list)
    quality: DataQualityReport | None = None
    continuation: str | None = None
    complete: bool = True

    @property
    def records(self) -> tuple[TradeFill | PriceObservation | OrderBookSnapshot, ...]:
        return (*self.fills, *self.observations, *self.snapshots)

