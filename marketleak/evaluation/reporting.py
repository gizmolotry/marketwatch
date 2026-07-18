from __future__ import annotations

from .metrics import EvaluationReport


def effectiveness_summary(report: EvaluationReport) -> str:
    if report.effectiveness_status == "effectiveness_unknown_insufficient_labels":
        return f"Effectiveness unknown: {report.effectiveness_reason}."
    return (
        "Effectiveness estimate available for the labeled evaluation set only; "
        "it is not a finding of fraud or evidence of production effectiveness."
    )
