from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from marketleak.multimodal.baselines import (
    BaselineAvailability,
    MarketMechanismBaseline,
    MechanismExample,
)
from marketleak.multimodal.bundles import (
    ARTIFACT_NAMES,
    FrozenBundleRepository,
    ServingBundleManifest,
    sha256_file,
)
from marketleak.multimodal.labels import ObservableMechanism


T0 = datetime(2026, 7, 13, tzinfo=UTC)


def _examples(start: datetime, prefix: str, groups: tuple[str, ...]) -> list[MechanismExample]:
    labels = (
        ObservableMechanism.SCHEDULED_EVENT_RESPONSE,
        ObservableMechanism.SCHEDULED_EVENT_RESPONSE,
        ObservableMechanism.THIN_LIQUIDITY_ARTIFACT,
        ObservableMechanism.THIN_LIQUIDITY_ARTIFACT,
    )
    values = ((0.9, 0.2), (0.8, 0.3), (0.1, 0.9), (0.2, 0.8))
    return [
        MechanismExample(
            event_uid=f"{prefix}-{index}",
            group_uid=groups[index],
            observed_at=start + timedelta(minutes=index),
            features={"price_move": value[0], "depth_change": value[1]},
            mechanism=label,
        )
        for index, (label, value) in enumerate(zip(labels, values, strict=True))
    ]


def _baseline() -> MarketMechanismBaseline:
    return MarketMechanismBaseline(
        min_fit_examples=4,
        min_calibration_examples=4,
        min_evaluation_examples=4,
        min_examples_per_mechanism=2,
        random_state=7,
    )


def test_label_gate_fails_closed_without_sufficient_mechanism_data() -> None:
    fit = _examples(T0, "fit", ("f1", "f2", "f3", "f4"))
    calibration = _examples(T0 + timedelta(days=1), "cal", ("c1", "c2", "c3", "c4"))
    evaluation = _examples(T0 + timedelta(days=2), "eval", ("e1", "e2", "e3", "e4"))
    model = MarketMechanismBaseline(min_fit_examples=10, min_calibration_examples=4, min_evaluation_examples=4)
    status = model.fit(fit, calibration, evaluation)
    assert status.availability is BaselineAvailability.UNAVAILABLE
    assert status.reason == "insufficient_labeled_mechanism_data"
    assert model.predict({"price_move": 0.5, "depth_change": 0.5}).ranked_mechanisms == ()


def test_calibration_partition_must_be_group_disjoint_from_fit() -> None:
    fit = _examples(T0, "fit", ("f1", "f2", "f3", "f4"))
    calibration = _examples(T0 + timedelta(days=1), "cal", ("f1", "c2", "c3", "c4"))
    evaluation = _examples(T0 + timedelta(days=2), "eval", ("e1", "e2", "e3", "e4"))
    status = _baseline().fit(fit, calibration, evaluation)
    assert status.availability is BaselineAvailability.UNAVAILABLE
    assert status.reason == "partitions_not_group_disjoint"


def test_baseline_outputs_observable_mechanisms_only() -> None:
    fit = _examples(T0, "fit", ("f1", "f2", "f3", "f4"))
    calibration = _examples(T0 + timedelta(days=1), "cal", ("c1", "c2", "c3", "c4"))
    evaluation = _examples(T0 + timedelta(days=2), "eval", ("e1", "e2", "e3", "e4"))
    model = _baseline()
    assert model.fit(fit, calibration, evaluation).availability is BaselineAvailability.AVAILABLE
    prediction = model.predict({"price_move": 0.85, "depth_change": 0.25})
    assert prediction.availability is BaselineAvailability.AVAILABLE
    names = {item.mechanism for item in prediction.ranked_mechanisms}
    assert names == {
        ObservableMechanism.SCHEDULED_EVENT_RESPONSE.value,
        ObservableMechanism.THIN_LIQUIDITY_ARTIFACT.value,
    }
    assert all(term not in " ".join(names) for term in ("fraud", "insider", "misconduct"))


def _artifact_paths(root: Path) -> dict[str, Path]:
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for name in ARTIFACT_NAMES:
        path = root / f"{name}.json"
        path.write_text(f'{{"artifact":"{name}","version":1}}', encoding="utf-8")
        paths[name] = path
    return paths


def _manifest(paths: dict[str, Path], *, version: str = "v1.0.0") -> ServingBundleManifest:
    hashes = {name: sha256_file(path) for name, path in paths.items()}
    return ServingBundleManifest(
        bundle_version=version,
        created_at=T0,
        dataset_hash=hashes["dataset"],
        feature_spec_hash=hashes["feature_spec"],
        model_hash=hashes["model"],
        calibration_hash=hashes["calibration"],
        ood_hash=hashes["ood"],
        conformal_hash=hashes["conformal"],
        retrieval_hash=hashes["retrieval"],
        code_hash=hashes["code"],
        mechanisms=("scheduled_event_response", "thin_liquidity_artifact"),
    )


def test_manifest_and_atomic_current_pointer_are_hash_verified(tmp_path: Path) -> None:
    paths = _artifact_paths(tmp_path / "artifacts")
    repo = FrozenBundleRepository(tmp_path / "serving")
    first = _manifest(paths)
    pointer = repo.publish(first, paths)
    assert pointer.bundle_uid == first.bundle_uid
    assert repo.read_current().manifest_hash == first.manifest_hash

    # A new immutable version switches only the tiny current pointer; prior
    # manifest remains in place for reproducible rollback/read-only audit.
    paths["code"].write_text('{"artifact":"code","version":2}', encoding="utf-8")
    second = _manifest(paths, version="v1.0.1")
    repo.publish(second, paths)
    assert repo.read_current().bundle_uid == second.bundle_uid
    assert (repo.bundle_root / str(first.bundle_uid) / "manifest.json").is_file()
    with pytest.raises(ValueError, match="bundle_version"):
        _manifest(paths, version="../not-a-version")


def test_bad_artifact_hash_is_rejected_before_publication(tmp_path: Path) -> None:
    paths = _artifact_paths(tmp_path / "artifacts")
    valid = _manifest(paths)
    broken = ServingBundleManifest(
        bundle_version=valid.bundle_version,
        created_at=valid.created_at,
        dataset_hash=valid.dataset_hash,
        feature_spec_hash=valid.feature_spec_hash,
        model_hash="0" * 64,
        calibration_hash=valid.calibration_hash,
        ood_hash=valid.ood_hash,
        conformal_hash=valid.conformal_hash,
        retrieval_hash=valid.retrieval_hash,
        code_hash=valid.code_hash,
        mechanisms=valid.mechanisms,
    )
    with pytest.raises(ValueError, match="artifact hash mismatch: model"):
        FrozenBundleRepository(tmp_path / "serving").publish(broken, paths)
