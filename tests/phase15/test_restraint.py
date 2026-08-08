from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from marketleak.multimodal.evaluation import (
    EvaluationRecord,
    brier_ece_when_adequate,
    selective_queue_metrics,
    temporal_group_disjoint_split,
)
from marketleak.multimodal.fusion import DeterministicLateFusion, ModalityEvidence
from marketleak.multimodal.uncertainty import (
    AbstentionDecision,
    AdaptiveFeedbackThresholdController,
    ConformalFeedback,
    QueueCandidate,
    ReferenceOODScorer,
    RollingConformalRiskController,
    SelectiveAbstentionPolicy,
)


NOW = datetime(2026, 7, 13, 12, tzinfo=UTC)


def _fusion(*, modalities: int = 2):
    evidence = [
        ModalityEvidence(
            modality="market",
            observed_at=NOW - timedelta(seconds=5),
            reliability=0.9,
            mechanism_scores={"scheduled_release": 0.8},
        )
    ]
    if modalities > 1:
        evidence.append(
            ModalityEvidence(
                modality="public_source",
                observed_at=NOW - timedelta(seconds=5),
                reliability=0.9,
                mechanism_scores={"scheduled_release": 0.7},
            )
        )
    return DeterministicLateFusion(expected_modalities=("market", "public_source")).fuse(evidence, as_of=NOW)


def test_ood_scoring_marks_far_vector_outside_reference() -> None:
    scorer = ReferenceOODScorer(k_neighbors=2, min_reference=5).fit(
        [[0.0, 0.0], [0.1, 0.0], [0.0, 0.1], [0.1, 0.1], [0.05, 0.05]]
    )
    near = scorer.score([0.05, 0.05])
    far = scorer.score([10.0, 10.0])
    assert near.sufficient_reference is True
    assert far.sufficient_reference is True
    assert far.ood_score is not None and near.ood_score is not None
    assert far.ood_score > near.ood_score
    assert far.ood_score > 0.50


def test_selective_policy_abstains_for_missing_modalities() -> None:
    scorer = ReferenceOODScorer(k_neighbors=1, min_reference=3).fit([[0.0], [0.1], [0.2]])
    decision = SelectiveAbstentionPolicy(min_observed_modalities=2).decide(_fusion(modalities=1), scorer.score([0.1]))
    assert decision.abstain is True
    assert "insufficient_modalities" in decision.reasons


def test_disjoint_mechanism_support_is_maximal_disagreement_and_abstains() -> None:
    # Minimal fixture for sparse-map mechanics; it is not effectiveness evidence.
    fusion = DeterministicLateFusion(expected_modalities=("market", "public_source")).fuse(
        [
            ModalityEvidence("market", NOW, 1.0, {"mechanism_x": 1.0}),
            ModalityEvidence("public_source", NOW, 1.0, {"mechanism_y": 1.0}),
        ],
        as_of=NOW,
    )
    ood = ReferenceOODScorer(k_neighbors=1, min_reference=3).fit([[0.0], [0.1], [0.2]]).score([0.1])

    assert fusion.disagreement == pytest.approx(1.0)
    decision = SelectiveAbstentionPolicy(max_disagreement=0.40).decide(fusion, ood)
    assert "cross_modality_disagreement" in decision.reasons


def test_adaptive_controller_excludes_future_feedback() -> None:
    controller = AdaptiveFeedbackThresholdController(min_feedback=2, window_size=10)
    controller.record(
        ConformalFeedback(NOW - timedelta(hours=2), NOW - timedelta(hours=1), 0.8, True, "old-supported")
    )
    controller.record(
        ConformalFeedback(NOW - timedelta(minutes=5), NOW + timedelta(hours=1), 0.95, False, "future-feedback")
    )
    threshold, reason = controller.threshold(as_of=NOW)
    assert threshold is None
    assert reason == "adaptive_feedback_history_unavailable"

    threshold, reason = controller.threshold(as_of=NOW + timedelta(hours=2))
    assert reason is None
    assert threshold is not None
    decision = controller.route(
        [QueueCandidate("candidate", 0.99, SelectiveAbstentionPolicy().decide(_fusion(), ReferenceOODScorer(min_reference=3).fit([[0.0], [0.1], [0.2]]).score([0.1])))],
        as_of=NOW + timedelta(hours=2),
        analyst_daily_budget=1,
    )
    assert len(decision) == 1


def test_adaptive_feedback_is_idempotent_and_conflicts_fail_closed() -> None:
    controller = AdaptiveFeedbackThresholdController(min_feedback=1)
    feedback = ConformalFeedback(NOW - timedelta(hours=2), NOW - timedelta(hours=1), 0.8, True, "same")
    controller.record(feedback)
    controller.record(feedback)
    assert controller.available_feedback(as_of=NOW) == (feedback,)

    with pytest.raises(ValueError, match="conflicting feedback"):
        controller.record(
            ConformalFeedback(NOW - timedelta(hours=2), NOW - timedelta(minutes=30), 0.8, False, "same")
        )


def test_adaptive_threshold_is_strict_and_legacy_name_is_deprecated() -> None:
    controller = AdaptiveFeedbackThresholdController(min_feedback=2)
    controller.record(ConformalFeedback(NOW - timedelta(hours=3), NOW - timedelta(hours=2), 0.7, False, "a"))
    controller.record(ConformalFeedback(NOW - timedelta(hours=2), NOW - timedelta(hours=1), 0.9, True, "b"))
    threshold, reason = controller.threshold(as_of=NOW)
    assert reason is None
    assert threshold == pytest.approx(0.7)

    neutral = AbstentionDecision(False, (), "mechanism_x", 0.7, 0.0)
    routed = controller.route([QueueCandidate("candidate", 0.7, neutral)], as_of=NOW, analyst_daily_budget=1)
    assert routed[0].escalate is False
    assert routed[0].reasons == ("at_or_below_adaptive_threshold",)

    with pytest.warns(DeprecationWarning, match="adaptive heuristic"):
        RollingConformalRiskController(min_feedback=1)


def test_calibration_metrics_withhold_brier_and_ece_without_labels() -> None:
    rows = [
        EvaluationRecord("a", NOW, "event-a", 0.8, True, True),
        EvaluationRecord("b", NOW + timedelta(minutes=1), "event-b", 0.2, False, False),
        EvaluationRecord("c", NOW + timedelta(minutes=2), "event-c", 0.5, None, False),
    ]
    metrics = brier_ece_when_adequate(rows, min_labels=5, min_positive=1, min_negative=1)
    assert metrics.available is False
    assert metrics.reason == "insufficient_labels"
    assert metrics.brier is None and metrics.ece is None


def test_temporal_group_split_drops_boundary_groups_and_reports_analyst_day_rate() -> None:
    rows = [
        EvaluationRecord("old-a", NOW, "reused", 0.8, True, True),
        EvaluationRecord("new-a", NOW + timedelta(days=2), "reused", 0.9, False, True),
        EvaluationRecord("old-b", NOW, "old-only", 0.3, False, False),
        EvaluationRecord("new-b", NOW + timedelta(days=2), "new-only", 0.9, False, True),
    ]
    split = temporal_group_disjoint_split(rows, cutoff=NOW + timedelta(days=1))
    assert {row.group_uid for row in split.train}.isdisjoint({row.group_uid for row in split.test})
    assert {row.group_uid for row in split.dropped} == {"reused"}
    metrics = selective_queue_metrics(rows)
    assert metrics.false_escalations == 2
    assert metrics.false_escalations_per_analyst_day == 1.0


def test_prohibited_outcome_language_cannot_be_a_ranked_mechanism() -> None:
    with pytest.raises(ValueError, match="prohibited outcome language"):
        ModalityEvidence(
            modality="market",
            observed_at=NOW,
            reliability=1.0,
            mechanism_scores={"insider_trading": 0.9},
        )
