"""Trustworthy, evidence-preserving market data ingestion.

The v2 ingestion package deliberately lives beside the legacy demo scraper.  It
never writes to ``demo_data`` and captures every HTTP response before attempting
to normalize it.
"""

from .coverage import CapabilityMetadata, CoverageLedger, CoverageRecord
from .quality import DataQualityGate, DataQualityReport, GateResult
from .raw_store import RawArtifactStore, RawCapture
from .polymarket_population import (
    PolymarketPopulationBackfill,
    PolymarketPopulationConflictError,
    PolymarketPopulationError,
    PolymarketPopulationLeaf,
    PolymarketPopulationManifest,
    PolymarketPopulationRequest,
    PolymarketPopulationResult,
    PolymarketPopulationStorageWrite,
    PolymarketReceiptBinding,
    PolymarketPopulationEvidenceBound,
)
from .storage import NormalizedStore, WriteResult

__all__ = [
    "CapabilityMetadata",
    "CoverageLedger",
    "CoverageRecord",
    "DataQualityGate",
    "DataQualityReport",
    "GateResult",
    "NormalizedStore",
    "PolymarketPopulationBackfill",
    "PolymarketPopulationConflictError",
    "PolymarketPopulationError",
    "PolymarketPopulationLeaf",
    "PolymarketPopulationManifest",
    "PolymarketPopulationRequest",
    "PolymarketPopulationResult",
    "PolymarketPopulationStorageWrite",
    "PolymarketReceiptBinding",
    "PolymarketPopulationEvidenceBound",
    "RawArtifactStore",
    "RawCapture",
    "WriteResult",
]
