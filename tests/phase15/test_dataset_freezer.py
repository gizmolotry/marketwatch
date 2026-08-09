from __future__ import annotations

import json
import socket
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from marketleak.domain import ActorVisibility, TradeFill, TradeSide
from marketleak.ingestion.coverage import CoverageLedger, CoverageRecord
from marketleak.ingestion.normalize import canonical_json_bytes
from marketleak.ingestion.raw_store import RawArtifactStore
from marketleak.multimodal.datasets import (
    CanonicalCorpusFileError,
    ManifestVerificationError,
    ObservationalCorpusManifest,
    freeze_observational_trade_fills,
    make_selection_policy,
    verify_observational_trade_fill_freeze,
)


T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


def _policy(platform: str = "polymarket"):
    source_uid = f"{platform}:source/public-trades"
    receipt_source = "data-api/trades" if platform == "polymarket" else "markets/trades"
    return make_selection_policy(
        source_root_label="mechanics-fixture-trade-fills",
        platform=platform,
        source_uids=(source_uid,),
        coverage_dataset="public_trades",
        coverage_filters={},
        receipt_sources_by_source_uid={source_uid: (receipt_source,)},
    )


def _capture(raw_root: Path, *, platform: str, index: int, received_at: datetime):
    source = "data-api/trades" if platform == "polymarket" else "markets/trades"
    # This is a minimal transport mechanics fixture, never evaluation evidence.
    payload = canonical_json_bytes({"fixture_delivery": index, "platform": platform})
    return RawArtifactStore(raw_root).capture(
        payload,
        platform=platform,
        source=source,
        request={"method": "GET", "fixture": True},
        received_at=received_at,
        response_metadata={"status_code": 200},
    )


def _fill(
    raw_root: Path,
    *,
    platform: str = "polymarket",
    index: int = 1,
    fill_uid: str | None = None,
    size: str = "2.5",
) -> TradeFill:
    capture = _capture(raw_root, platform=platform, index=index, received_at=T0 + timedelta(minutes=index, seconds=10))
    actor_visible = platform == "polymarket"
    return TradeFill(
        event_time=T0 + timedelta(minutes=index),
        ingested_at=capture.received_at,
        source_uid=f"{platform}:source/public-trades",
        raw_artifact_uid=f"{platform}:raw/{capture.sha256}",
        parser_version=f"{platform}-public-v2.0.0",
        fill_uid=fill_uid or f"{platform}:fill/{index}",
        market_uid=f"{platform}:market/{index % 2}",
        outcome_uid=f"{platform}:outcome/{index % 3}",
        platform=platform,
        price=Decimal("0.40"),
        size=Decimal(size),
        side=TradeSide.BUY,
        actor_visibility=(ActorVisibility.PUBLIC_WALLET if actor_visible else ActorVisibility.NOT_AVAILABLE),
        actor_uid=(f"{platform}:wallet/actor-{index % 2}" if actor_visible else None),
    )


def _write_fill(root: Path, relative: str, fill: TradeFill, *, canonical: bool = True) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(fill)
    if not canonical:
        payload = json.dumps(fill.model_dump(mode="json"), indent=2, sort_keys=True).encode("utf-8")
    path.write_bytes(payload)
    return path


def _coverage(
    path: Path,
    fills: tuple[TradeFill, ...],
    *,
    complete: bool = False,
    continuation: str | None = "next-page",
) -> None:
    digests = tuple(sorted({fill.raw_artifact_uid.rsplit("/", 1)[1] for fill in fills}))
    CoverageLedger(path).append(
        CoverageRecord(
            platform="polymarket",
            dataset="public_trades",
            interval_start=min(fill.event_time for fill in fills),
            interval_end=max(fill.event_time for fill in fills),
            fetched_at=max(fill.ingested_at for fill in fills),
            record_count=len(fills),
            complete=complete,
            raw_sha256=digests,
            continuation=continuation,
            filters={},
        )
    )


def _freeze(source_root: Path, raw_root: Path, ledger: Path):
    return freeze_observational_trade_fills(
        source_root=source_root,
        raw_root=raw_root,
        coverage_ledger_path=ledger,
        selection=_policy(),
        code_revision="fixture-revision-1",
        source_contract_version="polymarket-public-trades-contract-v1",
    )


def test_freeze_is_order_independent_and_selects_from_validated_record_fields(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    first = _fill(raw_root, index=1)
    second = _fill(raw_root, index=2)
    kalshi = _fill(raw_root, platform="kalshi", index=3)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (first, second))

    root_a = tmp_path / "root-a"
    root_b = tmp_path / "root-b"
    for root, records in ((root_a, (("b.json", second), ("a.json", first), ("z.json", kalshi))),
                          (root_b, (("z.json", kalshi), ("a.json", first), ("b.json", second)))):
        for relative, fill in records:
            _write_fill(root, relative, fill)

    left = _freeze(root_a, raw_root, ledger)
    right = _freeze(root_b, raw_root, ledger)

    assert left.to_bytes() == right.to_bytes()
    assert left.files.canonical_file_count == 3
    assert left.files.selected_file_count == 2
    assert left.files.excluded_platform_counts == (("kalshi", 1),)
    assert left.population.unique_actor_count == 2
    assert left.population.unique_market_count == 2
    assert left.population.unique_outcome_count == 2
    assert left.status == "ready_with_limitations"
    assert left.coverage.status == "partial"
    assert left.coverage.absence_claims_permitted is False
    assert left.effectiveness_status == "effectiveness_unknown"
    assert left.revision.code_revision_binding == "caller_asserted"


def test_duplicate_and_conflicting_fill_uids_are_distinguished_and_fail_closed(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    original = _fill(raw_root, index=1, fill_uid="polymarket:fill/shared", size="2.5")
    conflict = original.model_copy(update={"size": Decimal("9.0")})
    source_root = tmp_path / "fills"
    _write_fill(source_root, "one/a.json", original)
    _write_fill(source_root, "two/b.json", original)
    _write_fill(source_root, "three/c.json", conflict)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (original,))

    manifest = _freeze(source_root, raw_root, ledger)

    assert manifest.files.selected_file_count == 3
    assert manifest.files.unique_record_count == 1
    assert manifest.files.duplicate_file_count == 1
    assert manifest.files.conflict_uid_count == 1
    assert manifest.files.unique_record_version_count == 2
    assert manifest.status == "failed_closed"
    assert "conflicting_fill_uids" in manifest.failure_reasons


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_missing_or_corrupt_raw_objects_are_explicit_and_fail_closed(tmp_path: Path, damage: str) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))
    digest = fill.raw_artifact_uid.rsplit("/", 1)[1]
    object_path = raw_root / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"
    if damage == "missing":
        object_path.unlink()
    else:
        payload = object_path.read_bytes()
        object_path.write_bytes(b"X" + payload[1:])

    manifest = _freeze(source_root, raw_root, ledger)

    assert manifest.status == "failed_closed"
    assert manifest.lineage.missing_raw_object_count == (1 if damage == "missing" else 0)
    assert manifest.lineage.corrupt_raw_object_count == (1 if damage == "corrupt" else 0)


def test_missing_matching_receipt_is_explicit_and_fail_closed(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))
    for receipt in (raw_root / "receipts").rglob("*.json"):
        receipt.unlink()

    manifest = _freeze(source_root, raw_root, ledger)

    assert manifest.lineage.verified_raw_object_count == 1
    assert manifest.lineage.missing_receipt_count == 1
    assert manifest.status == "failed_closed"
    assert "missing_matching_raw_receipts" in manifest.failure_reasons


def test_valid_later_replay_receipt_is_nonmatching_not_corrupt(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))
    payload = canonical_json_bytes({"fixture_delivery": 1, "platform": "polymarket"})
    RawArtifactStore(raw_root).capture(
        payload,
        platform="polymarket",
        source="data-api/trades",
        request={"method": "GET", "fixture": True},
        received_at=fill.ingested_at + timedelta(minutes=5),
        response_metadata={"status_code": 200},
    )

    manifest = _freeze(source_root, raw_root, ledger)

    assert manifest.lineage.matching_receipt_count == 1
    assert manifest.lineage.nonmatching_replay_receipt_count == 1
    assert manifest.lineage.corrupt_relevant_receipt_count == 0
    assert manifest.status == "ready_with_limitations"


@pytest.mark.parametrize(
    "mutation",
    ("missing_request", "boolean_schema", "boolean_byte_length", "receipt_id_path_mismatch"),
)
def test_partial_or_malformed_relevant_receipt_cannot_satisfy_lineage(
    tmp_path: Path, mutation: str
) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))
    receipt_path = next((raw_root / "receipts").rglob("*.json"))
    receipt = json.loads(receipt_path.read_bytes())
    if mutation == "missing_request":
        del receipt["request"]
    elif mutation == "boolean_schema":
        receipt["schema_version"] = True
    elif mutation == "boolean_byte_length":
        receipt["byte_length"] = True
    else:
        receipt["receipt_id"] = receipt["receipt_id"].rsplit("-", 1)[0] + "-" + "0" * 32
    receipt_path.write_bytes(canonical_json_bytes(receipt))

    manifest = _freeze(source_root, raw_root, ledger)

    assert manifest.lineage.corrupt_relevant_receipt_count == 1
    assert manifest.lineage.matching_receipt_count == 0
    assert manifest.lineage.missing_receipt_count == 1
    assert manifest.status == "failed_closed"


def test_receipt_source_and_clock_must_match_the_same_selected_lineage_pair(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _fill(raw_root, index=1)
    first_source = "polymarket:source/a"
    second_source = "polymarket:source/b"
    first = base.model_copy(
        update={
            "fill_uid": "polymarket:fill/cross-a",
            "source_uid": first_source,
            "ingested_at": base.ingested_at + timedelta(minutes=5),
        }
    )
    second = base.model_copy(
        update={
            "fill_uid": "polymarket:fill/cross-b",
            "source_uid": second_source,
        }
    )
    source_root = tmp_path / "fills"
    _write_fill(source_root, "a.json", first)
    _write_fill(source_root, "b.json", second)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (first, second))
    policy = make_selection_policy(
        source_root_label="shared-raw-pairing-mechanics-fixture",
        platform="polymarket",
        source_uids=(first_source, second_source),
        coverage_dataset="public_trades",
        coverage_filters={},
        receipt_sources_by_source_uid={
            first_source: ("data-api/trades",),
            second_source: ("other/trades",),
        },
    )

    manifest = freeze_observational_trade_fills(
        source_root=source_root,
        raw_root=raw_root,
        coverage_ledger_path=ledger,
        selection=policy,
        code_revision="fixture-revision-1",
        source_contract_version="shared-raw-pairing-contract-v1",
    )

    assert manifest.lineage.corrupt_relevant_receipt_count == 0
    assert manifest.lineage.nonmatching_replay_receipt_count == 1
    assert manifest.lineage.matching_receipt_count == 0
    assert manifest.lineage.missing_receipt_count == 1
    assert manifest.status == "failed_closed"


def test_hashing_clean_files_does_not_promote_incomplete_coverage(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,), complete=False, continuation="still-more")

    manifest = _freeze(source_root, raw_root, ledger)

    assert manifest.integrity_usable is True
    assert manifest.coverage.status == "partial"
    assert manifest.coverage.incomplete_row_count == 1
    assert manifest.coverage.continuation_row_count == 1
    assert "incomplete_coverage_rows_present" in manifest.coverage.reason_codes
    assert manifest.coverage.absence_claims_permitted is False


def test_source_tamper_changes_hash_and_breaks_manifest_reproduction(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    path = _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))
    manifest = _freeze(source_root, raw_root, ledger)

    changed = fill.model_copy(update={"fill_uid": "polymarket:fill/changed"})
    path.write_bytes(canonical_json_bytes(changed))
    changed_manifest = _freeze(source_root, raw_root, ledger)

    assert changed_manifest.corpus_sha256 != manifest.corpus_sha256
    with pytest.raises(ManifestVerificationError, match="differs from the manifest"):
        verify_observational_trade_fill_freeze(
            manifest,
            source_root=source_root,
            raw_root=raw_root,
            coverage_ledger_path=ledger,
        )


def test_noncanonical_trade_fill_encoding_is_rejected(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill, canonical=False)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))

    with pytest.raises(CanonicalCorpusFileError, match="not exact canonical JSON"):
        _freeze(source_root, raw_root, ledger)


def test_freezer_performs_no_network_io(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))

    def forbidden_socket(*_args, **_kwargs):
        raise AssertionError("network access is forbidden during an offline freeze")

    monkeypatch.setattr(socket, "socket", forbidden_socket)
    manifest = _freeze(source_root, raw_root, ledger)
    assert manifest.files.unique_record_count == 1


def test_manifest_roundtrip_self_verification_and_immutable_write(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))
    manifest = _freeze(source_root, raw_root, ledger)

    payload = manifest.to_bytes()
    assert ObservationalCorpusManifest.from_bytes(payload) == manifest
    manifest_path = tmp_path / "manifest.json"
    manifest.write_new(manifest_path)
    assert ObservationalCorpusManifest.read(manifest_path) == manifest
    assert manifest.write_new(manifest_path) == manifest_path

    different = freeze_observational_trade_fills(
        source_root=source_root,
        raw_root=raw_root,
        coverage_ledger_path=ledger,
        selection=_policy(),
        code_revision="fixture-revision-2",
        source_contract_version="polymarket-public-trades-contract-v1",
    )
    with pytest.raises(ManifestVerificationError, match="different immutable manifest"):
        different.write_new(manifest_path)

    tampered = json.loads(payload)
    tampered["files"]["unique_actor_count"] = 999
    with pytest.raises(ManifestVerificationError):
        ObservationalCorpusManifest.from_bytes(canonical_json_bytes(tampered))


def test_atomic_publication_failure_leaves_no_partial_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_root = tmp_path / "raw"
    fill = _fill(raw_root, index=1)
    source_root = tmp_path / "fills"
    _write_fill(source_root, "fill.json", fill)
    ledger = tmp_path / "coverage.jsonl"
    _coverage(ledger, (fill,))
    manifest = _freeze(source_root, raw_root, ledger)
    destination = tmp_path / "published" / "manifest.json"

    def fail_link(_source, _destination):
        raise OSError("mechanics fixture: atomic publication unavailable")

    monkeypatch.setattr("marketleak.multimodal.datasets.os.link", fail_link)
    with pytest.raises(ManifestVerificationError, match="atomic no-overwrite"):
        manifest.write_new(destination)

    assert not destination.exists()
    assert not list(destination.parent.glob(".manifest.json.pending-*.tmp"))
