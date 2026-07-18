"""Deterministically export one recorded-snapshot review case from local capture files.

The builder is deliberately offline.  It reads a fixed registry and completed
normalized/raw capture, verifies the selected raw-object hashes, and emits a
hash-checked review-case configuration.  Missing modalities stay unavailable;
this module never fetches a market, metadata, a BTC reference, or public
evidence to fill them.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from marketleak.multimodal.review_cases import (
    CoverageModalityState,
    FrozenReviewCase,
    OrdinaryExplanationCheck,
    RedactedEvidenceLedgerEntry,
    ReviewCaseKind,
    ReviewCheckStatus,
    ReviewCoverage,
    ReviewCoverageStatus,
    ReviewDecision,
    ReviewRouting,
    ReviewTrigger,
    review_cases_config_document,
)


DEFAULT_CAPTURE_ROOT = Path("data/polymarket-btc65k-live-verified")
DEFAULT_REGISTRY_PATH = Path("configs/phase15/polymarket_btc65k_live.json")
DEFAULT_OUTPUT_PATH = Path("data/review-cases/polymarket_btc65k_recorded.json")
DEFAULT_CASE_CONTEXT_FILENAME = "case-context.json"
DEFAULT_BINANCE_REFERENCE_FILENAME = "binance-btcusdt-reference.json"
CASE_CONTEXT_SCHEMA_VERSION = "phase15-polymarket-case-context-v1"
BINANCE_REFERENCE_SCHEMA_VERSION = "phase15-binance-btcusdt-reference-v1"

CASE_UID = "review:polymarket-btc65k-recorded"
TRIGGER_UID = "trigger:polymarket-btc65k-recorded-window"
COCAPTURED_CASE_UID = "review:polymarket-btc65k-cocaptured"
COCAPTURED_TRIGGER_UID = "trigger:polymarket-btc65k-cocaptured-window"

MISSING_COVERAGE_REASONS = (
    "market_metadata_question_rule_mapping_unavailable",
    "btc_reference_unavailable",
    "public_evidence_unavailable",
)


def _as_mapping(value: Any, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _required_text(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value.strip()


def _utc(value: Any, *, label: str) -> datetime:
    text = _required_text(value, label=label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _raw_digest(raw_artifact_uid: str) -> str:
    prefix = "polymarket:raw/"
    if not raw_artifact_uid.startswith(prefix):
        raise ValueError("raw_artifact_uid must reference a Polymarket raw artifact")
    digest = raw_artifact_uid.removeprefix(prefix)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("raw_artifact_uid has an invalid SHA-256 digest")
    return digest


def _raw_path(capture_root: Path, digest: str) -> Path:
    return capture_root / "raw" / "objects" / "sha256" / digest[:2] / digest[2:4] / f"{digest}.raw"


def _read_json(path: Path, *, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is unreadable: {path}") from exc


def _registry_target(registry_path: Path) -> tuple[str, tuple[str, ...]]:
    document = _as_mapping(_read_json(registry_path, label="registry"), label="registry")
    targets = document.get("targets")
    if not isinstance(targets, list) or len(targets) != 1:
        raise ValueError("registry must contain exactly one fixed target")
    target = _as_mapping(targets[0], label="registry target")
    if target.get("venue") != "polymarket":
        raise ValueError("registry target venue must be polymarket")
    market = _required_text(target.get("market_uid"), label="registry market_uid")
    market_prefix = "polymarket:"
    if not market.startswith(market_prefix):
        raise ValueError("registry market_uid must be a Polymarket condition identifier")
    condition_id = market.removeprefix(market_prefix)
    assets = target.get("polymarket_asset_ids")
    if not isinstance(assets, list) or len(assets) < 1:
        raise ValueError("registry target requires at least one Polymarket asset ID")
    asset_ids = tuple(_required_text(asset, label="registry asset ID") for asset in assets)
    if len(set(asset_ids)) != len(asset_ids):
        raise ValueError("registry asset IDs must be distinct")
    return f"polymarket:market/{condition_id}", asset_ids


def _raw_receipts(capture_root: Path) -> tuple[dict[str, datetime], int]:
    receipt_paths = sorted((capture_root / "raw" / "receipts").glob("**/*.json"))
    if not receipt_paths:
        raise ValueError("completed capture has no raw receipts")
    receipt_times: dict[str, datetime] = {}
    for path in receipt_paths:
        receipt = _as_mapping(_read_json(path, label="raw receipt"), label="raw receipt")
        # A co-capture root also contains Gamma/CLOB and (possibly) Binance
        # receipts.  They are verified separately; only market-channel
        # receipts may satisfy selected price-observation lineage here.
        if receipt.get("platform") != "polymarket" or receipt.get("source") != "ws/market":
            continue
        digest = _required_text(receipt.get("sha256"), label="raw receipt sha256")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("raw receipt SHA-256 is invalid")
        received_at = _utc(receipt.get("received_at"), label="raw receipt received_at")
        receipt_times[digest] = max(receipt_times.get(digest, received_at), received_at)
    if not receipt_times:
        raise ValueError("completed capture has no Polymarket market-channel raw receipts")
    return receipt_times, len(receipt_times)


def _completed_observations(capture_root: Path, *, market_uid: str, outcome_uid: str) -> list[dict[str, Any]]:
    paths = sorted((capture_root / "normalized" / "priceobservation").glob("**/*.json"))
    if not paths:
        raise ValueError("completed capture has no normalized price observations")
    observations: list[dict[str, Any]] = []
    for path in paths:
        record = _as_mapping(_read_json(path, label="normalized price observation"), label="normalized price observation")
        if record.get("platform") != "polymarket":
            raise ValueError("normalized price observation is not Polymarket data")
        if record.get("market_uid") != market_uid or record.get("outcome_uid") != outcome_uid:
            continue
        observation_uid = _required_text(record.get("observation_uid"), label="observation_uid")
        raw_artifact_uid = _required_text(record.get("raw_artifact_uid"), label="raw_artifact_uid")
        observations.append(
            {
                "observation_uid": observation_uid,
                "raw_artifact_uid": raw_artifact_uid,
                "source_uid": _required_text(record.get("source_uid"), label="source_uid"),
                "event_time": _utc(record.get("event_time"), label="event_time"),
                "ingested_at": _utc(record.get("ingested_at"), label="ingested_at"),
                "price": Decimal(_required_text(record.get("price"), label="price")),
            }
        )
    observations.sort(key=lambda item: (item["event_time"], item["observation_uid"], item["raw_artifact_uid"]))
    if len(observations) < 2:
        raise ValueError("recorded snapshot requires at least two selected price observations")
    if observations[0]["event_time"] >= observations[-1]["event_time"]:
        raise ValueError("recorded snapshot requires a positive observation window")
    if len({item["source_uid"] for item in observations}) != 1:
        raise ValueError("selected price observations must have one source UID")
    return observations


def _verify_selected_raw_lineage(
    capture_root: Path,
    observations: Iterable[Mapping[str, Any]],
    receipt_times: Mapping[str, datetime],
) -> dict[str, datetime]:
    available_at: dict[str, datetime] = {}
    for observation in observations:
        raw_artifact_uid = _required_text(observation.get("raw_artifact_uid"), label="raw_artifact_uid")
        digest = _raw_digest(raw_artifact_uid)
        object_path = _raw_path(capture_root, digest)
        try:
            raw_bytes = object_path.read_bytes()
        except OSError as exc:
            raise ValueError(f"selected raw object is unavailable: {raw_artifact_uid}") from exc
        if hashlib.sha256(raw_bytes).hexdigest() != digest:
            raise ValueError(f"selected raw object fails SHA-256 verification: {raw_artifact_uid}")
        if digest not in receipt_times:
            raise ValueError(f"selected raw object has no recorded receipt: {raw_artifact_uid}")
        available_at[raw_artifact_uid] = receipt_times[digest]
    return available_at


def build_recorded_snapshot(
    capture_root: str | Path = DEFAULT_CAPTURE_ROOT,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
) -> FrozenReviewCase:
    """Build one offline recorded-snapshot case from a fixed local capture."""

    root = Path(capture_root)
    market_uid, asset_ids = _registry_target(Path(registry_path))
    outcome_uid = f"polymarket:outcome/{asset_ids[0]}"
    receipt_times, receipt_count = _raw_receipts(root)
    observations = _completed_observations(root, market_uid=market_uid, outcome_uid=outcome_uid)
    raw_available_at = _verify_selected_raw_lineage(root, observations, receipt_times)

    first = observations[0]
    last = observations[-1]
    selected = (first, last)
    trigger_raw_uids = tuple(dict.fromkeys(item["raw_artifact_uid"] for item in selected))
    evidence_entries = tuple(
        RedactedEvidenceLedgerEntry(
            evidence_uid=item["observation_uid"],
            modality="market_state",
            event_time=item["event_time"],
            available_at=max(item["event_time"], item["ingested_at"], raw_available_at[item["raw_artifact_uid"]]),
            source_uid=item["source_uid"],
            raw_artifact_uid=item["raw_artifact_uid"],
            content_hash=_raw_digest(item["raw_artifact_uid"]),
            reliability_tier="unassessed",
        )
        for item in selected
    )
    evidence_by_raw = {item.raw_artifact_uid: item for item in evidence_entries}
    if not set(trigger_raw_uids).issubset(evidence_by_raw):
        raise ValueError("selected trigger raw lineage is incomplete")

    available_at = max(item.available_at for item in evidence_entries)
    as_of = max(available_at, last["event_time"])
    evidence_uids = tuple(item.evidence_uid for item in evidence_entries)
    source_uid = first["source_uid"]

    return FrozenReviewCase(
        case_uid=CASE_UID,
        case_kind=ReviewCaseKind.RECORDED_SNAPSHOT,
        published_at=as_of,
        as_of=as_of,
        venue="polymarket",
        market_uid=market_uid,
        question="Unavailable: fixed market metadata, question, and rule mapping are not present in this recorded capture.",
        outcome_uid=outcome_uid,
        trigger=ReviewTrigger(
            trigger_uid=TRIGGER_UID,
            event_uid=last["observation_uid"],
            window_starts_at=first["event_time"],
            window_ends_at=last["event_time"],
            observed_at=last["event_time"],
            available_at=available_at,
            price_open=first["price"],
            price_close=last["price"],
            price_change=last["price"] - first["price"],
            observation_count=len(observations),
            fill_count=0,
            orderbook_snapshot_count=0,
            trade_notional=None,
            raw_artifact_uids=trigger_raw_uids,
        ),
        ordinary_checks=(
            OrdinaryExplanationCheck(
                check_uid="check:polymarket-btc65k-collection-health",
                kind="collection_health",
                status=ReviewCheckStatus.OBSERVED,
                coverage_status=ReviewCoverageStatus.COMPLETE,
                summary="Recorded local price observations and verified raw receipts are available for the selected outcome.",
                evidence_uids=evidence_uids,
            ),
            OrdinaryExplanationCheck(
                check_uid="check:polymarket-btc65k-market-context",
                kind="market_context",
                status=ReviewCheckStatus.UNAVAILABLE,
                coverage_status=ReviewCoverageStatus.UNAVAILABLE,
                summary="Fixed market metadata, question, and rule mapping are unavailable in the recorded capture.",
            ),
            OrdinaryExplanationCheck(
                check_uid="check:polymarket-btc65k-underlying-reference",
                kind="underlying_reference",
                status=ReviewCheckStatus.UNAVAILABLE,
                coverage_status=ReviewCoverageStatus.UNAVAILABLE,
                summary="BTC reference data and its fixed mapping are unavailable in the recorded capture.",
            ),
            OrdinaryExplanationCheck(
                check_uid="check:polymarket-btc65k-public-evidence",
                kind="public_evidence",
                status=ReviewCheckStatus.UNAVAILABLE,
                coverage_status=ReviewCoverageStatus.UNAVAILABLE,
                summary="Point-in-time public evidence is unavailable in the recorded capture.",
            ),
            OrdinaryExplanationCheck(
                check_uid="check:polymarket-btc65k-market-mechanics",
                kind="market_mechanics",
                status=ReviewCheckStatus.UNAVAILABLE,
                coverage_status=ReviewCoverageStatus.PARTIAL,
                summary="Depth and notional coverage are unavailable; this recorded snapshot contains price observations only.",
            ),
        ),
        coverage=ReviewCoverage(
            overall_status=ReviewCoverageStatus.PARTIAL,
            modality_states=(
                CoverageModalityState(
                    modality="market_state",
                    status=ReviewCoverageStatus.COMPLETE,
                    reason="Selected price observations and verified raw receipts are recorded locally.",
                ),
                CoverageModalityState(
                    modality="market_context",
                    status=ReviewCoverageStatus.UNAVAILABLE,
                    reason="Fixed market metadata, question, and rule mapping are unavailable.",
                ),
                CoverageModalityState(
                    modality="reference_price",
                    status=ReviewCoverageStatus.UNAVAILABLE,
                    reason="BTC reference data and its fixed mapping are unavailable.",
                ),
                CoverageModalityState(
                    modality="public_evidence",
                    status=ReviewCoverageStatus.UNAVAILABLE,
                    reason="Point-in-time public evidence is unavailable.",
                ),
            ),
            source_high_watermarks={source_uid: as_of},
            gap_reasons=MISSING_COVERAGE_REASONS,
            raw_receipt_count=receipt_count,
            late_excluded_count=0,
        ),
        routing=ReviewRouting(
            decision=ReviewDecision.ABSTAIN_INSUFFICIENT_EVIDENCE,
            coverage_status=ReviewCoverageStatus.PARTIAL,
            abstention_reasons=MISSING_COVERAGE_REASONS,
            mechanism_hypotheses=(),
        ),
        evidence_ledger=evidence_entries,
    )


def build_document(
    capture_root: str | Path = DEFAULT_CAPTURE_ROOT,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
) -> dict[str, object]:
    """Return the immutable, hash-checked configuration document without writing it."""

    return review_cases_config_document((build_recorded_snapshot(capture_root, registry_path),))


def write_document(
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    capture_root: str | Path = DEFAULT_CAPTURE_ROOT,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
) -> Path:
    """Write the deterministic review-case configuration using stable JSON formatting."""

    destination = Path(output_path)
    document = build_document(capture_root, registry_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return destination


def _resolve_within(root: Path, value: Any, *, label: str) -> Path:
    """Resolve a manifest path only when it remains inside its capture root."""

    path = Path(_required_text(value, label=label)).resolve()
    allowed = root.resolve()
    try:
        path.relative_to(allowed)
    except ValueError as exc:
        raise ValueError(f"{label} must remain inside the capture root") from exc
    return path


def _receipt_for_manifest_entry(
    capture_root: Path,
    entry: Mapping[str, Any],
    *,
    expected_platform: str,
    expected_source: str | None = None,
) -> tuple[str, datetime]:
    """Verify one manifest reference against the immutable raw object/receipt."""

    digest = _required_text(entry.get("sha256"), label="manifest raw sha256")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("manifest raw SHA-256 is invalid")
    raw_uid = _required_text(entry.get("raw_artifact_uid"), label="manifest raw_artifact_uid")
    prefix = f"{expected_platform}:raw/"
    if raw_uid != f"{prefix}{digest}":
        raise ValueError("manifest raw artifact UID does not match the captured platform and SHA-256")
    object_path = _raw_path(capture_root, digest).resolve()
    declared_object = entry.get("object_path")
    if declared_object is not None and _resolve_within(capture_root, declared_object, label="manifest object_path") != object_path:
        raise ValueError("manifest object_path does not match the content-addressed raw object")
    try:
        raw_bytes = object_path.read_bytes()
    except OSError as exc:
        raise ValueError(f"manifest raw object is unavailable: {raw_uid}") from exc
    if hashlib.sha256(raw_bytes).hexdigest() != digest:
        raise ValueError(f"manifest raw object fails SHA-256 verification: {raw_uid}")
    receipt_path = _resolve_within(capture_root, entry.get("receipt_path"), label="manifest receipt_path")
    if not receipt_path.is_file() or capture_root.resolve() / "raw" / "receipts" not in receipt_path.parents:
        raise ValueError("manifest receipt_path must name a receipt beneath capture_root/raw/receipts")
    receipt = _as_mapping(_read_json(receipt_path, label="manifest raw receipt"), label="manifest raw receipt")
    if receipt.get("sha256") != digest or receipt.get("platform") != expected_platform:
        raise ValueError("manifest raw receipt conflicts with the referenced raw object")
    if expected_source is not None and receipt.get("source") != expected_source:
        raise ValueError("manifest raw receipt has an unexpected source")
    received_at = _utc(receipt.get("received_at"), label="manifest receipt received_at")
    declared_received_at = entry.get("receipt_received_at", entry.get("received_at"))
    if declared_received_at is not None and _utc(declared_received_at, label="manifest receipt_received_at") != received_at:
        raise ValueError("manifest receipt timing conflicts with the immutable receipt")
    return raw_uid, received_at


def _context_manifest(
    capture_root: Path,
    *,
    market_uid: str,
    asset_ids: tuple[str, ...],
) -> tuple[Mapping[str, Any], datetime, tuple[RedactedEvidenceLedgerEntry, ...], dict[str, datetime]]:
    path = capture_root / DEFAULT_CASE_CONTEXT_FILENAME
    document = _as_mapping(_read_json(path, label="case context manifest"), label="case context manifest")
    if document.get("schema_version") != CASE_CONTEXT_SCHEMA_VERSION or document.get("status") != "collected":
        raise ValueError("case context manifest is not a collected fixed Polymarket context")
    availability = _as_mapping(document.get("availability"), label="case context availability")
    if availability.get("available") is not True:
        raise ValueError("case context manifest is unavailable")
    cutoff = _utc(availability.get("as_of"), label="case context as_of")
    watermark = _utc(availability.get("watermark"), label="case context watermark")
    if watermark > cutoff:
        raise ValueError("case context watermark is later than its as_of cutoff")
    market = _as_mapping(document.get("market"), label="case context market")
    if market.get("market_uid") != market_uid:
        raise ValueError("case context market UID conflicts with the selected recorded capture")
    if market.get("resolution_source", object()) is not None:
        raise ValueError("case context must preserve the target's explicit null resolutionSource")
    if market.get("resolution_source_status") not in {"null", "absent"}:
        raise ValueError("case context must record whether Gamma resolutionSource was null or absent")
    question = _required_text(market.get("question"), label="case context question")
    rule = _required_text(market.get("rule"), label="case context rule")
    outcomes = market.get("outcomes")
    expected_outcomes = [("Yes", asset_ids[0]), ("No", asset_ids[1])]
    if not isinstance(outcomes, list) or [
        (item.get("label"), item.get("token_id")) if isinstance(item, Mapping) else None for item in outcomes
    ] != expected_outcomes:
        raise ValueError("case context outcome/token mapping conflicts with the selected recorded capture")
    raw_lineage = document.get("raw_lineage")
    if not isinstance(raw_lineage, list) or len(raw_lineage) != 2:
        raise ValueError("case context requires exactly Gamma and CLOB raw lineage entries")
    expected_sources = ("polymarket:source/gamma-market", "polymarket:source/clob-market-info")
    entries: list[RedactedEvidenceLedgerEntry] = []
    watermarks: dict[str, datetime] = {}
    seen_sources: set[str] = set()
    for raw in raw_lineage:
        item = _as_mapping(raw, label="case context raw lineage")
        source_uid = _required_text(item.get("source_uid"), label="case context source_uid")
        if source_uid not in expected_sources or source_uid in seen_sources:
            raise ValueError("case context raw lineage must contain one Gamma and one CLOB source")
        seen_sources.add(source_uid)
        raw_uid, received_at = _receipt_for_manifest_entry(
            capture_root,
            item,
            expected_platform="polymarket",
            expected_source=source_uid,
        )
        if received_at > cutoff:
            raise ValueError("case context raw receipt is later than its as_of cutoff")
        digest = _raw_digest(raw_uid)
        entries.append(
            RedactedEvidenceLedgerEntry(
                evidence_uid=f"polymarket:case-context/{market_uid.rsplit('/', 1)[1]}/{source_uid.rsplit('/', 1)[1]}/{digest}",
                modality="market_context",
                event_time=received_at,
                available_at=received_at,
                source_uid=source_uid,
                raw_artifact_uid=raw_uid,
                content_hash=digest,
                reliability_tier="high",
            )
        )
        watermarks[source_uid] = received_at
    if seen_sources != set(expected_sources):
        raise ValueError("case context raw lineage is incomplete")
    # The exact rule text is represented in the frozen question/check summary,
    # while the raw Gamma artifact remains the authoritative complete text.
    return {"question": question, "rule": rule}, cutoff, tuple(entries), watermarks


def _reference_evidence(
    capture_root: Path,
    *,
    market_uid: str,
    market_window_starts_at: datetime,
    market_window_ends_at: datetime,
) -> tuple[RedactedEvidenceLedgerEntry | None, str, dict[str, datetime], int]:
    """Admit only the fixed Binance candle manifest; invalid input stays absent.

    A malformed, late, tampered, or non-final candle is *not* a builder error:
    it is a missing modality in the co-captured packet.  The context and market
    capture remain independently reviewable and the routing remains abstain.
    """

    path = capture_root / DEFAULT_BINANCE_REFERENCE_FILENAME
    if not path.is_file():
        return None, "BTC/USDT 1-minute final-candle reference is unavailable in this co-capture.", {}, 0
    try:
        document = _as_mapping(_read_json(path, label="Binance reference manifest"), label="Binance reference manifest")
        if document.get("schema_version") != BINANCE_REFERENCE_SCHEMA_VERSION or document.get("status") != "collected":
            raise ValueError("reference manifest was not collected")
        if document.get("market_uid") != market_uid:
            raise ValueError("reference manifest market UID does not match the case")
        manifest_as_of = _utc(document.get("as_of"), label="reference manifest as_of")
        admission = _as_mapping(document.get("admission"), label="reference admission")
        if admission.get("status") != "admitted" or admission.get("market_uid") != market_uid:
            raise ValueError("reference admission is not an admitted match for this market")
        mapping = _as_mapping(document.get("mapping"), label="reference mapping")
        if (
            mapping.get("market_uid") != market_uid
            or mapping.get("asset_symbol") != "BTC"
            or mapping.get("quote_currency") != "USDT"
            or mapping.get("observation_kind") != "candle_high"
            or mapping.get("candle_interval") != "1m"
            or mapping.get("requires_closed_candle") is not True
        ):
            raise ValueError("reference mapping is not the documented BTC/USDT final candle-high contract")
        observation = _as_mapping(document.get("observation"), label="reference observation")
        if (
            observation.get("market_uid") != market_uid
            or observation.get("observation_kind") != "candle_high"
            or observation.get("source_symbol") != "BTCUSDT"
            or observation.get("candle_interval") != "1m"
            or observation.get("is_final") is not True
        ):
            raise ValueError("reference observation is not a final BTCUSDT 1-minute candle high")
        event_time = _utc(observation.get("event_time"), label="reference observation event_time")
        available_at = _utc(observation.get("ingested_at"), label="reference observation ingested_at")
        candle_start = _utc(observation.get("candle_start"), label="reference candle_start")
        candle_end = _utc(observation.get("candle_end"), label="reference candle_end")
        if (
            candle_start >= candle_end
            or event_time != candle_end
            or event_time < market_window_starts_at
            or event_time > market_window_ends_at
            or available_at > manifest_as_of
        ):
            raise ValueError("reference candle timing is not point-in-time admissible")
        source_mapping = _as_mapping(observation.get("source_mapping"), label="reference observation mapping")
        if source_mapping.get("mapping_uid") != mapping.get("mapping_uid"):
            raise ValueError("reference observation mapping conflicts with manifest mapping")
        provenance = _as_mapping(observation.get("provenance"), label="reference observation provenance")
        raw_uid = _required_text(provenance.get("raw_artifact_uid"), label="reference raw_artifact_uid")
        digest = _raw_digest_for_platform(raw_uid, platform="binance")
        raw_captures = document.get("raw_captures")
        if not isinstance(raw_captures, list) or not raw_captures:
            raise ValueError("reference manifest has no raw captures")
        lineage_by_digest = {str(item.get("sha256")): item for item in raw_captures if isinstance(item, Mapping)}
        manifest_raw = lineage_by_digest.get(digest)
        if manifest_raw is None:
            raise ValueError("reference provenance is absent from the raw capture manifest")
        # RawCapture JSON uses ``received_at`` rather than the context
        # manifest's ``receipt_received_at``.  Adapt the immutable contract
        # only at this boundary, never by substituting a source or a price.
        receipt_entry = dict(manifest_raw)
        receipt_entry["raw_artifact_uid"] = raw_uid
        raw_uid, receipt_at = _receipt_for_manifest_entry(
            capture_root,
            receipt_entry,
            expected_platform="binance",
        )
        if _utc(manifest_raw.get("received_at"), label="reference manifest received_at") != receipt_at:
            raise ValueError("reference manifest receipt time conflicts with immutable receipt")
        source_uid = _required_text(provenance.get("source_uid"), label="reference source_uid")
        if source_uid != mapping.get("primary_source_uid"):
            raise ValueError("reference provenance source conflicts with documented primary source")
        reliability = _as_mapping(observation.get("reliability"), label="reference reliability")
        tier = reliability.get("tier")
        if tier not in {"high", "medium", "low", "unassessed"}:
            raise ValueError("reference reliability tier is invalid")
        reference_uid = _required_text(observation.get("reference_price_uid"), label="reference_price_uid")
        return (
            RedactedEvidenceLedgerEntry(
                evidence_uid=reference_uid,
                modality="reference_price",
                event_time=event_time,
                available_at=max(available_at, receipt_at),
                source_uid=source_uid,
                raw_artifact_uid=raw_uid,
                content_hash=digest,
                reliability_tier=tier,
            ),
            "A raw-lineaged Binance BTC/USDT final 1-minute candle High was available at the co-capture cutoff.",
            {source_uid: receipt_at},
            len(raw_captures),
        )
    except (KeyError, TypeError, ValueError):
        return None, "BTC/USDT 1-minute final-candle reference is unavailable, mismatched, late, or failed raw-lineage verification.", {}, 0


def _raw_digest_for_platform(raw_artifact_uid: str, *, platform: str) -> str:
    prefix = f"{platform}:raw/"
    if not raw_artifact_uid.startswith(prefix):
        raise ValueError("raw_artifact_uid has an unexpected platform")
    digest = raw_artifact_uid.removeprefix(prefix)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("raw_artifact_uid has an invalid SHA-256 digest")
    return digest


def build_cocaptured_snapshot(
    capture_root: str | Path = DEFAULT_CAPTURE_ROOT,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
) -> FrozenReviewCase:
    """Build a new, separate review case from one bounded co-capture root.

    This never changes the historical ``recorded_snapshot`` case.  It derives
    its frozen cutoff from the latest admissible market/context/reference
    receipt rather than treating the context collection timestamp as a global
    cutoff.  A reference manifest can only add evidence if every declared raw
    object, receipt, mapping, and candle field is independently verified.
    """

    root = Path(capture_root)
    base = build_recorded_snapshot(root, registry_path)
    market_uid, asset_ids = _registry_target(Path(registry_path))
    context, _context_self_cutoff, context_entries, context_watermarks = _context_manifest(root, market_uid=market_uid, asset_ids=asset_ids)
    reference_entry, reference_summary, reference_watermarks, reference_raw_count = _reference_evidence(
        root,
        market_uid=market_uid,
        market_window_starts_at=base.trigger.window_starts_at,
        market_window_ends_at=base.trigger.window_ends_at,
    )

    market_entries = tuple(base.evidence_ledger)
    evidence_entries = market_entries + context_entries + (() if reference_entry is None else (reference_entry,))
    collection_evidence_uids = tuple(item.evidence_uid for item in market_entries)
    context_evidence_uids = tuple(item.evidence_uid for item in context_entries)
    watermarks = dict(base.coverage.source_high_watermarks)
    watermarks.update(context_watermarks)
    watermarks.update(reference_watermarks)
    cutoff = max(
        (base.trigger.available_at, *(entry.available_at for entry in context_entries), *(() if reference_entry is None else (reference_entry.available_at,))),
        default=base.trigger.available_at,
    )
    if any(timestamp > cutoff for timestamp in watermarks.values()):
        raise ValueError("co-captured source watermark is later than the derived case cutoff")
    reference_available = reference_entry is not None
    gaps = ["public_evidence_unavailable", "market_mechanics_partial"]
    if not reference_available:
        gaps.append("btc_reference_unavailable")
    coverage_status = ReviewCoverageStatus.PARTIAL
    checks = (
        OrdinaryExplanationCheck(
            check_uid="check:polymarket-btc65k-cocaptured-collection-health",
            kind="collection_health",
            status=ReviewCheckStatus.OBSERVED,
            coverage_status=ReviewCoverageStatus.COMPLETE,
            summary="Selected local market observations and their raw receipts are available within the co-captured cutoff.",
            evidence_uids=collection_evidence_uids,
        ),
        OrdinaryExplanationCheck(
            check_uid="check:polymarket-btc65k-cocaptured-market-context",
            kind="market_context",
            status=ReviewCheckStatus.OBSERVED,
            coverage_status=ReviewCoverageStatus.COMPLETE,
            summary="Fixed Gamma market context and CLOB Yes/No token mapping matched the co-captured target; resolutionSource was null or absent, recorded explicitly in the context manifest, and the captured rule text is retained in raw lineage.",
            evidence_uids=context_evidence_uids,
        ),
        OrdinaryExplanationCheck(
            check_uid="check:polymarket-btc65k-cocaptured-underlying-reference",
            kind="underlying_reference",
            status=ReviewCheckStatus.OBSERVED if reference_available else ReviewCheckStatus.UNAVAILABLE,
            coverage_status=ReviewCoverageStatus.COMPLETE if reference_available else ReviewCoverageStatus.UNAVAILABLE,
            summary=reference_summary,
            evidence_uids=() if reference_entry is None else (reference_entry.evidence_uid,),
        ),
        OrdinaryExplanationCheck(
            check_uid="check:polymarket-btc65k-cocaptured-public-evidence",
            kind="public_evidence",
            status=ReviewCheckStatus.UNAVAILABLE,
            coverage_status=ReviewCoverageStatus.UNAVAILABLE,
            summary="Point-in-time public evidence is not captured in this bounded co-capture.",
        ),
        OrdinaryExplanationCheck(
            check_uid="check:polymarket-btc65k-cocaptured-market-mechanics",
            kind="market_mechanics",
            status=ReviewCheckStatus.UNAVAILABLE,
            coverage_status=ReviewCoverageStatus.PARTIAL,
            summary="Depth and traded-notional coverage are incomplete; this case contains selected price observations only.",
        ),
    )
    modality_states = (
        CoverageModalityState(
            modality="market_state",
            status=ReviewCoverageStatus.COMPLETE,
            reason="Selected market observations and verified raw receipts are local.",
        ),
        CoverageModalityState(
            modality="market_context",
            status=ReviewCoverageStatus.COMPLETE,
            reason="Fixed Gamma/CLOB context and raw lineage matched the registered target.",
        ),
        CoverageModalityState(
            modality="reference_price",
            status=ReviewCoverageStatus.COMPLETE if reference_available else ReviewCoverageStatus.UNAVAILABLE,
            reason="Documented final BTC/USDT candle-high reference is available." if reference_available else reference_summary,
        ),
        CoverageModalityState(
            modality="public_evidence",
            status=ReviewCoverageStatus.UNAVAILABLE,
            reason="Point-in-time public evidence was not captured.",
        ),
    )
    return FrozenReviewCase(
        case_uid=COCAPTURED_CASE_UID,
        case_kind=ReviewCaseKind.RECORDED_SNAPSHOT,
        published_at=cutoff,
        as_of=cutoff,
        venue="polymarket",
        market_uid=market_uid,
        question=context["question"],
        outcome_uid=base.outcome_uid,
        trigger=ReviewTrigger(
            trigger_uid=COCAPTURED_TRIGGER_UID,
            event_uid=base.trigger.event_uid,
            window_starts_at=base.trigger.window_starts_at,
            window_ends_at=base.trigger.window_ends_at,
            observed_at=base.trigger.observed_at,
            available_at=base.trigger.available_at,
            price_open=base.trigger.price_open,
            price_close=base.trigger.price_close,
            price_change=base.trigger.price_change,
            observation_count=base.trigger.observation_count,
            fill_count=base.trigger.fill_count,
            orderbook_snapshot_count=base.trigger.orderbook_snapshot_count,
            trade_notional=base.trigger.trade_notional,
            raw_artifact_uids=base.trigger.raw_artifact_uids,
        ),
        ordinary_checks=checks,
        coverage=ReviewCoverage(
            overall_status=coverage_status,
            modality_states=modality_states,
            source_high_watermarks=watermarks,
            gap_reasons=tuple(gaps),
            raw_receipt_count=base.coverage.raw_receipt_count + len(context_entries) + reference_raw_count,
            late_excluded_count=0 if reference_available else 1,
        ),
        routing=ReviewRouting(
            decision=ReviewDecision.ABSTAIN_INSUFFICIENT_EVIDENCE,
            coverage_status=coverage_status,
            abstention_reasons=tuple(gaps),
            mechanism_hypotheses=(),
        ),
        evidence_ledger=evidence_entries,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an offline recorded-snapshot review case.")
    parser.add_argument("--capture-root", type=Path, default=DEFAULT_CAPTURE_ROOT)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    arguments = parser.parse_args()
    output = write_document(arguments.output, arguments.capture_root, arguments.registry)
    print(output)


if __name__ == "__main__":
    main()
