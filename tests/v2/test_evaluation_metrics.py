from datetime import UTC, datetime, timedelta
from decimal import Decimal

from marketleak.evaluation.baselines import feature_ablation_scores, score_baselines
from marketleak.evaluation.metrics import CalibrationGate, evaluate
from marketleak.evaluation.reporting import effectiveness_summary
from marketleak.evaluation.schemas import EvaluationRow
from marketleak.labels import LabelTarget, LabelValue


def make_row(
    uid,
    label,
    score,
    *,
    target=LabelTarget.ACTIVITY_A,
    cluster=None,
    detected=True,
    **kwargs,
):
    event_time = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=int(uid[-1]))
    return EvaluationRow(
        row_uid=uid,
        target=target,
        label=label,
        score=Decimal(score),
        event_time=event_time,
        detected_at=event_time + timedelta(seconds=30) if detected else None,
        market_uid=f"market:{uid}",
        event_cluster_uid=cluster or f"cluster:{uid}",
        platform=kwargs.pop("platform", "kalshi"),
        category=kwargs.pop("category", "politics"),
        exposure_market_days=Decimal("1"),
        **kwargs,
    )


def test_metrics_treat_unknown_as_unlabeled_not_negative():
    rows = [
        make_row("r1", LabelValue.POSITIVE, "0.90", cluster="event:1"),
        make_row("r2", LabelValue.UNKNOWN, "0.85", cluster="event:2"),
        make_row("r3", LabelValue.NEGATIVE, "0.80", cluster="event:3"),
        make_row("r4", LabelValue.POSITIVE, "0.10", cluster="event:4"),
    ]
    report = evaluate(
        rows,
        target=LabelTarget.ACTIVITY_A,
        analyst_capacity=3,
        calibration_gate=CalibrationGate(min_labeled=10, min_positive=2, min_negative=1),
        bootstrap_samples=10,
    )

    assert report.precision_at_capacity == 0.5  # r2 is excluded, not counted false
    assert report.labeled_fraction_at_capacity == 2 / 3
    assert report.recall_at_capacity == 0.5
    assert report.false_alerts_per_1000_market_days == 250.0
    assert report.unknown_rows == 1 and report.negative_rows == 1
    assert report.effectiveness_status == "effectiveness_unknown_insufficient_labels"
    assert report.brier_score is None and report.ece is None
    assert effectiveness_summary(report).startswith("Effectiveness unknown")


def test_calibration_is_computed_only_after_sufficiency_gate():
    rows = [
        make_row("r1", LabelValue.POSITIVE, "0.8"),
        make_row("r2", LabelValue.NEGATIVE, "0.2"),
    ]
    report = evaluate(
        rows,
        target=LabelTarget.ACTIVITY_A,
        analyst_capacity=2,
        calibration_gate=CalibrationGate(min_labeled=2, min_positive=1, min_negative=1, ece_bins=2),
        bootstrap_samples=0,
    )

    assert report.effectiveness_status == "estimated_from_available_labels"
    assert report.brier_score == 0.04
    assert report.ece is not None
    assert report.pr_auc == 1.0


def test_B_timeline_and_C_evidence_metrics_are_separate():
    b_rows = [
        make_row("b1", LabelValue.NEGATIVE, "0.9", target=LabelTarget.PUBLIC_EXPLANATION_B),
        make_row("b2", LabelValue.UNKNOWN, "0.8", target=LabelTarget.PUBLIC_EXPLANATION_B, abstained=True),
    ]
    b_report = evaluate(
        b_rows, target=LabelTarget.PUBLIC_EXPLANATION_B, analyst_capacity=2, bootstrap_samples=0
    )
    assert b_report.timeline_b["label_coverage"] == 0.5
    assert b_report.timeline_b["false_claim_rate"] == 1.0
    assert b_report.timeline_b["abstention_rate"] == 0.5

    c_rows = [
        make_row(
            "c1", LabelValue.POSITIVE, "0.9", target=LabelTarget.ACTOR_EVIDENCE_C,
            actor_visible=True, evidence_supported=True,
        ),
        make_row(
            "c2", LabelValue.NEGATIVE, "0.8", target=LabelTarget.ACTOR_EVIDENCE_C,
            actor_visible=False, evidence_supported=False,
        ),
    ]
    c_report = evaluate(
        c_rows, target=LabelTarget.ACTOR_EVIDENCE_C, analyst_capacity=2, bootstrap_samples=0
    )
    assert c_report.actor_evidence_c["actor_visibility_rate"] == 0.5
    assert c_report.actor_evidence_c["evidence_support_rate"] == 0.5
    assert c_report.actor_evidence_c["evidence_supported_precision"] == 1.0


def test_naive_baselines_and_feature_ablations_are_explicit():
    row = make_row("r1", LabelValue.NEGATIVE, "0.5")
    features = {"price_change": Decimal("-0.7"), "volume": Decimal("3")}
    baselines = score_baselines([(row, features)])
    assert baselines["naive_price_change"][0].score == Decimal("0.7")
    assert baselines["naive_volume"][0].score == Decimal("0.75")

    scores = feature_ablation_scores(features, lambda values: sum(values.values(), Decimal(0)))
    assert set(scores) == {"all_features", "without:price_change", "without:volume"}
