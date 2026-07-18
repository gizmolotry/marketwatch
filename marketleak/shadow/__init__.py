"""Frozen prospective shadow-run infrastructure."""

from marketleak.shadow.adjudication import AdjudicationRecord, AdjudicationStore
from marketleak.shadow.ledger import (
    FrozenRunMutationError,
    LedgerEntry,
    LedgerTamperError,
    ShadowLedger,
)
from marketleak.shadow.manifest import (
    ShadowRunManifestV2,
    SourceCoverageState,
    build_shadow_manifest,
    resolve_git_revision,
)
from marketleak.shadow.runner import ShadowInputRecord, ShadowRunResult, ShadowRunner

__all__ = [
    "AdjudicationRecord",
    "AdjudicationStore",
    "FrozenRunMutationError",
    "LedgerEntry",
    "LedgerTamperError",
    "ShadowInputRecord",
    "ShadowLedger",
    "ShadowRunManifestV2",
    "ShadowRunResult",
    "ShadowRunner",
    "SourceCoverageState",
    "build_shadow_manifest",
    "resolve_git_revision",
]

