"""Leakage-safe evaluation and honest effectiveness reporting."""

from .metrics import CalibrationGate, EvaluationReport, evaluate
from .schemas import EvaluationRow, unique_evaluation_rows
from .splits import DatasetSplit, assert_no_split_leakage, forward_disjoint_split

__all__ = [
    "CalibrationGate",
    "DatasetSplit",
    "EvaluationReport",
    "EvaluationRow",
    "assert_no_split_leakage",
    "evaluate",
    "forward_disjoint_split",
    "unique_evaluation_rows",
]
