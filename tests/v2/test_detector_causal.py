from datetime import datetime, timedelta, timezone
from decimal import Decimal
import random

from marketleak.domain import ActorVisibility, ObservationKind, PriceObservation
from marketleak.detectors import CausalActivityDetector, DetectorConfig, MarketControl


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def observation(index: int, price: str, *, market: str = "test:m1", suffix: str = "") -> PriceObservation:
    event_time = BASE + timedelta(minutes=5 * index + 1)
    return PriceObservation(
        observation_uid=f"test:obs-{market.split(':')[-1]}-{index}{suffix}",
        market_uid=market,
        outcome_uid=f"test:{market.split(':')[-1]}-yes",
        platform="test",
        price=Decimal(price),
        kind=ObservationKind.PLATFORM_SNAPSHOT,
        actor_visibility=ActorVisibility.NOT_AVAILABLE,
        event_time=event_time,
        ingested_at=event_time + timedelta(seconds=1),
        source_uid="test:source",
        raw_artifact_uid=f"raw:artifact-{index}{suffix or '-a'}",
        parser_version="2.0.0",
    )


def config(**overrides) -> DetectorConfig:
    values = {
        "min_baseline_observations": 30,
        "min_baseline_elapsed": timedelta(hours=2),
        "empirical_tail_min_observations": 30,
        "fdr_alpha": 0.05,
    }
    values.update(overrides)
    return DetectorConfig(**values)


def stable_then_jump(*, market: str = "test:m1") -> list[PriceObservation]:
    return [observation(index, "0.50", market=market) for index in range(120)] + [
        observation(120, "0.80", market=market),
        observation(121, "0.80", market=market),
    ]


def signal_projection(batch):
    return [
        (item.market_uid, item.outcome_uid, item.bucket_time, item.status, item.p_value, item.q_value, item.rank)
        for item in batch.signals
    ]


def test_future_rows_cannot_change_earlier_scores() -> None:
    rows = stable_then_jump()
    detector = CausalActivityDetector(config())
    first = detector.detect(rows)
    extended = detector.detect(rows + [observation(122, "0.79"), observation(123, "0.81")])
    cutoff = first.signals[-1].bucket_time
    assert signal_projection(first) == signal_projection(
        type("Batch", (), {"signals": tuple(item for item in extended.signals if item.bucket_time <= cutoff)})
    )


def test_duplicate_order_and_cadence_do_not_change_detection() -> None:
    rows = stable_then_jump()
    duplicated = rows + rows[:40]
    random.Random(7).shuffle(duplicated)
    detector = CausalActivityDetector(config())
    assert signal_projection(detector.detect(rows)) == signal_projection(detector.detect(duplicated))

    extra = []
    for index in range(120):
        item = observation(index, "0.50", suffix="-extra")
        shifted = item.model_copy(update={"event_time": item.event_time + timedelta(minutes=1)})
        extra.append(shifted)
    cadence = detector.detect(rows + extra)
    assert [(item.bucket_time, item.status, item.p_value) for item in detector.detect(rows).signals] == [
        (item.bucket_time, item.status, item.p_value) for item in cadence.signals
    ]


def test_zero_mad_never_gets_magic_scale_and_thin_history_abstains() -> None:
    detector = CausalActivityDetector(
        config(
            min_baseline_observations=10,
            min_baseline_elapsed=timedelta(minutes=30),
            empirical_tail_min_observations=100,
        )
    )
    batch = detector.detect([observation(index, "0.50") for index in range(20)] + [observation(20, "0.80")])
    final = batch.signals[-1]
    assert final.status.value == "not_scorable"
    assert final.score is None
    assert "zero_mad_or_thin_empirical_history" in final.reason_codes


def test_adequate_history_and_jump_emit_one_clustered_incident() -> None:
    batch = CausalActivityDetector(config()).detect(stable_then_jump())
    assert len(batch.incidents) == 1
    assert batch.incidents[0].signal_count >= 1
    abnormal = [item for item in batch.signals if item.status.value == "abnormal"]
    assert abnormal
    assert all(item.score is not None and item.score >= 0 for item in abnormal)
    assert all(item.p_value is not None and item.q_value is not None for item in abnormal)
    assert all(item.p_value != item.score for item in abnormal)
    assert abnormal[0].diagnostics["price_5m"].baseline_mad == 0
    assert abnormal[0].diagnostics["price_5m"].method == "smoothed_empirical_tail"


def test_sibling_repricing_is_residualized_and_tagged() -> None:
    first = stable_then_jump(market="test:m1")
    second = stable_then_jump(market="test:m2")
    controls = [
        MarketControl(market_uid="test:m1", event_uid="event:shared", liquidity=1000),
        MarketControl(market_uid="test:m2", event_uid="event:shared", liquidity=1000),
    ]
    batch = CausalActivityDetector(config()).detect(first + second, controls=controls)
    jump_buckets = [item for item in batch.buckets if item.bucket_time == BASE + timedelta(minutes=605)]
    assert len(jump_buckets) == 2
    assert all(item.sibling_residual == 0 for item in jump_buckets)
    assert all("sibling_repricing" in item.control_tags for item in jump_buckets)
    assert not batch.incidents


def test_stale_gaps_and_short_current_fixture_abstain() -> None:
    rows = [observation(0, "0.5"), observation(20, "0.8")]
    batch = CausalActivityDetector(config()).detect(rows)
    assert any("stale_or_missing_price" in signal.reason_codes for signal in batch.signals)
    assert not batch.incidents


def test_legacy_fixture_semantic_dedupe_has_no_actionable_incident() -> None:
    from marketleak.detectors import detect_legacy_ticks

    batch = detect_legacy_ticks("demo_data/ticks.parquet")
    assert not batch.incidents
    assert all(item.status.value != "abnormal" for item in batch.signals)
