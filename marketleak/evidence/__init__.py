"""Point-in-time public evidence collection and assessment."""

from marketleak.evidence.archive import EvidenceArchive
from marketleak.evidence.coverage import (
    CoverageAssessment,
    CoverageLedger,
    CoverageStatus,
    SourceCoverageInterval,
)
from marketleak.evidence.explanation import (
    PublicExplanation,
    PublicExplanationStatus,
    assess_public_explanation,
)
from marketleak.evidence.matching import EvidenceMatch, EvidenceMatcher, MatchResult
from marketleak.evidence.normalize import NormalizedEvidence, normalize_evidence

__all__ = [
    "CoverageAssessment",
    "CoverageLedger",
    "CoverageStatus",
    "EvidenceArchive",
    "EvidenceMatch",
    "EvidenceMatcher",
    "MatchResult",
    "NormalizedEvidence",
    "PublicExplanation",
    "PublicExplanationStatus",
    "SourceCoverageInterval",
    "assess_public_explanation",
    "normalize_evidence",
]
