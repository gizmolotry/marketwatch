from datetime import UTC, datetime
from decimal import Decimal

from marketleak.evaluation.schemas import EvaluationRow
from marketleak.evaluation.splits import assert_no_split_leakage, forward_disjoint_split
from marketleak.labels import LabelTarget, LabelValue


def row(uid, date, market, cluster, actors=()):
    return EvaluationRow(
        row_uid=uid,
        target=LabelTarget.ACTIVITY_A,
        label=LabelValue.NEGATIVE,
        score=Decimal("0.1"),
        event_time=datetime.fromisoformat(date).replace(tzinfo=UTC),
        market_uid=market,
        event_cluster_uid=cluster,
        actor_uids=actors,
        platform="kalshi",
        category="politics",
    )


def test_forward_split_is_event_market_and_wallet_disjoint_and_drops_crossers():
    rows = [
        row("train", "2026-01-01", "m-train", "e-train", ("wallet-a",)),
        row("validation", "2026-02-01", "m-validation", "e-validation", ("wallet-b",)),
        row("test", "2026-03-01", "m-test", "e-test", ("wallet-c",)),
        # These two are transitively joined by wallet and straddle train/test.
        row("cross-early", "2026-01-02", "m-cross-1", "e-cross-1", ("wallet-x",)),
        row("cross-late", "2026-03-02", "m-cross-2", "e-cross-2", ("wallet-x",)),
    ]
    split = forward_disjoint_split(
        rows,
        train_end=datetime(2026, 2, 1, tzinfo=UTC),
        validation_end=datetime(2026, 3, 1, tzinfo=UTC),
    )

    assert [item.row_uid for item in split.train] == ["train"]
    assert [item.row_uid for item in split.validation] == ["validation"]
    assert [item.row_uid for item in split.test] == ["test"]
    assert {item.row_uid for item in split.dropped_cross_boundary} == {"cross-early", "cross-late"}
    assert_no_split_leakage(split)

