"""Bounded raw-first capture of one configured Polymarket case context.

This is intentionally narrower than :mod:`venue_metadata`.  Some Polymarket
markets describe their settlement rule only in the market description and
publish a null ``resolutionSource``.  A generic metadata collector that
requires a populated resolution-source URL would either invent a source or
discard a material fact.  This collector preserves that null explicitly,
captures the two official payloads before parsing, and admits the context only
when the configured question, rule, condition, and Yes/No token mapping match
exactly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
import json
from pathlib import Path
from typing import Any, Mapping

from marketleak.ingestion.config.phase15_registry import load_phase15_source_registry
from marketleak.ingestion.connectors.http import HttpResponse, HttpTransport, RequestsTransport
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture


POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"
GAMMA_SOURCE_UID = "polymarket:source/gamma-market"
CLOB_SOURCE_UID = "polymarket:source/clob-market-info"
CONTEXT_SCHEMA_VERSION = "phase15-polymarket-case-context-v1"
CONTEXT_FILENAME = "case-context.json"


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _timestamp(value: Any, *, field: str) -> datetime:
    text = _text(value, field=field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO-8601") from exc
    return _utc(parsed, field=field)


def _json_list(value: Any, *, field: str) -> list[Any]:
    decoded = json.loads(value) if isinstance(value, str) else value
    if not isinstance(decoded, list):
        raise ValueError(f"{field} must be a JSON list")
    return decoded


class PolymarketCaseContextStatus(str, Enum):
    COLLECTED = "collected"
    UNAVAILABLE_HTTP = "unavailable_http"
    UNAVAILABLE_INCOMPLETE = "unavailable_incomplete"
    UNAVAILABLE_LATE = "unavailable_late"
    REJECTED_MISMATCH = "rejected_mismatch"


@dataclass(frozen=True, slots=True)
class PolymarketCaseContextTarget:
    """One non-discovering Polymarket target and its immutable expectations."""

    gamma_market_id: str
    condition_id: str
    question: str
    rule: str
    yes_token_id: str
    no_token_id: str

    def __post_init__(self) -> None:
        for field in ("gamma_market_id", "condition_id", "question", "rule", "yes_token_id", "no_token_id"):
            object.__setattr__(self, field, _text(getattr(self, field), field=field))
        if self.yes_token_id == self.no_token_id:
            raise ValueError("Yes and No token IDs must be distinct")

    @property
    def market_uid(self) -> str:
        return f"polymarket:market/{self.condition_id}"


@dataclass(frozen=True, slots=True)
class PolymarketCaseContextResult:
    status: PolymarketCaseContextStatus
    reason: str
    document_path: Path
    raw_captures: tuple[RawCapture, ...]
    document: Mapping[str, Any]


def load_polymarket_case_context_target(path: str | Path) -> PolymarketCaseContextTarget:
    """Load exactly one declared context target; no target discovery is allowed."""

    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"case context config is unreadable: {path}") from exc
    if not isinstance(document, Mapping) or document.get("schema_version") != CONTEXT_SCHEMA_VERSION:
        raise ValueError("case context config has an unsupported schema_version")
    target = document.get("target")
    if not isinstance(target, Mapping):
        raise ValueError("case context config requires one target object")
    outcomes = target.get("outcomes")
    if not isinstance(outcomes, Mapping):
        raise ValueError("case context config requires Yes/No outcomes")
    return PolymarketCaseContextTarget(
        gamma_market_id=target.get("gamma_market_id"),
        condition_id=target.get("condition_id"),
        question=target.get("question"),
        rule=target.get("rule"),
        yes_token_id=outcomes.get("yes_token_id"),
        no_token_id=outcomes.get("no_token_id"),
    )


class PolymarketCaseContextCollector:
    """Capture a fixed Gamma + CLOB context pair and persist a frozen manifest."""

    def __init__(self, *, transport: HttpTransport, raw_store: RawArtifactStore | None = None) -> None:
        self.transport = transport
        self.raw_store = raw_store

    def collect(
        self,
        *,
        target: PolymarketCaseContextTarget,
        capture_root: str | Path,
        as_of: datetime,
        received_at: datetime,
    ) -> PolymarketCaseContextResult:
        root = Path(capture_root)
        cutoff = _utc(as_of, field="as_of")
        timestamp = _utc(received_at, field="received_at")
        store = self.raw_store or RawArtifactStore(root / "raw")
        if self.raw_store is not None and self.raw_store.root != root / "raw":
            raise ValueError("raw_store must be rooted at capture_root/raw")

        gamma_url = f"{POLYMARKET_GAMMA_API}/markets/{target.gamma_market_id}"
        clob_url = f"{POLYMARKET_CLOB_API}/clob-markets/{target.condition_id}"
        captures: list[RawCapture] = []
        gamma, gamma_capture, gamma_error = self._capture(store, GAMMA_SOURCE_UID, gamma_url, timestamp)
        if gamma_capture is not None:
            captures.append(gamma_capture)
        if gamma_error is not None:
            return self._write_failure(root, target, cutoff, timestamp, gamma_error, captures)
        assert gamma is not None and gamma_capture is not None
        try:
            gamma_values = self._validate_gamma(gamma, target)
        except (TypeError, ValueError, KeyError) as exc:
            return self._write_failure(
                root, target, cutoff, timestamp, f"Gamma metadata conflicts with the fixed target: {type(exc).__name__}", captures,
                status=PolymarketCaseContextStatus.REJECTED_MISMATCH,
            )
        if gamma_values["updated_at"] is not None and gamma_values["updated_at"] > gamma_capture.received_at:
            return self._write_failure(
                root,
                target,
                cutoff,
                timestamp,
                "Gamma updatedAt is later than its raw receipt and is not point-in-time admissible",
                captures,
                status=PolymarketCaseContextStatus.UNAVAILABLE_LATE,
            )

        clob, clob_capture, clob_error = self._capture(store, CLOB_SOURCE_UID, clob_url, timestamp)
        if clob_capture is not None:
            captures.append(clob_capture)
        if clob_error is not None:
            return self._write_failure(root, target, cutoff, timestamp, clob_error, captures)
        assert clob is not None and clob_capture is not None
        try:
            self._validate_clob(clob, target, gamma_values)
        except (TypeError, ValueError, KeyError) as exc:
            return self._write_failure(
                root, target, cutoff, timestamp, f"CLOB metadata conflicts with the fixed target: {type(exc).__name__}", captures,
                status=PolymarketCaseContextStatus.REJECTED_MISMATCH,
            )
        if timestamp > cutoff:
            return self._write_failure(
                root, target, cutoff, timestamp, "context receipts were not available by the requested as_of cutoff", captures,
                status=PolymarketCaseContextStatus.UNAVAILABLE_LATE,
            )

        document = self._document(
            target=target,
            status=PolymarketCaseContextStatus.COLLECTED,
            reason="Gamma market and CLOB token mapping matched the fixed target after raw capture.",
            as_of=cutoff,
            received_at=timestamp,
            captures=captures,
            gamma_values=gamma_values,
        )
        path = self._write_document(root, document)
        return PolymarketCaseContextResult(PolymarketCaseContextStatus.COLLECTED, str(document["reason"]), path, tuple(captures), document)

    def _capture(
        self,
        store: RawArtifactStore,
        source_uid: str,
        url: str,
        received_at: datetime,
    ) -> tuple[Mapping[str, Any] | None, RawCapture | None, str | None]:
        try:
            response: HttpResponse = self.transport.request("GET", url, params=None, timeout=10.0)
        except Exception as exc:
            return None, None, f"configured endpoint could not be reached: {type(exc).__name__}"
        capture = store.capture(
            response.body,
            platform="polymarket",
            source=source_uid,
            request={"method": "GET", "url": url, "params": {}},
            received_at=received_at,
            response_metadata={"status_code": response.status_code, "url": response.url, "headers": dict(response.headers)},
        )
        if not 200 <= response.status_code < 300:
            return None, capture, f"configured endpoint returned HTTP {response.status_code}"
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return None, capture, f"configured source body is incomplete or cannot be decoded: {type(exc).__name__}"
        if not isinstance(payload, Mapping):
            return None, capture, "configured source body is incomplete or cannot be decoded: payload is not an object"
        return payload, capture, None

    @staticmethod
    def _validate_gamma(payload: Mapping[str, Any], target: PolymarketCaseContextTarget) -> dict[str, Any]:
        if str(payload.get("id")) != target.gamma_market_id:
            raise ValueError("Gamma market id changed")
        if _text(payload.get("conditionId"), field="Gamma conditionId") != target.condition_id:
            raise ValueError("Gamma condition changed")
        if _text(payload.get("question"), field="Gamma question") != target.question:
            raise ValueError("Gamma question changed")
        if _text(payload.get("description"), field="Gamma description") != target.rule:
            raise ValueError("Gamma description/rule changed")
        if "resolutionSource" in payload and payload.get("resolutionSource") is not None:
            raise ValueError("Gamma resolutionSource must be absent or explicitly null for this target")
        outcomes = tuple(_text(item, field="Gamma outcome") for item in _json_list(payload.get("outcomes"), field="Gamma outcomes"))
        tokens = tuple(_text(item, field="Gamma token") for item in _json_list(payload.get("clobTokenIds"), field="Gamma token IDs"))
        if outcomes != ("Yes", "No") or tokens != (target.yes_token_id, target.no_token_id):
            raise ValueError("Gamma outcome/token mapping changed")
        return {
            "end_date": _timestamp(payload.get("endDate"), field="Gamma endDate"),
            "updated_at": _timestamp(payload.get("updatedAt"), field="Gamma updatedAt") if payload.get("updatedAt") else None,
            "resolution_source_status": "null" if "resolutionSource" in payload else "absent",
        }

    @staticmethod
    def _validate_clob(
        payload: Mapping[str, Any],
        target: PolymarketCaseContextTarget,
        gamma_values: Mapping[str, Any],
    ) -> None:
        if _text(payload.get("c"), field="CLOB condition") != target.condition_id:
            raise ValueError("CLOB condition changed")
        token_rows = payload.get("t")
        if not isinstance(token_rows, list):
            raise ValueError("CLOB tokens are missing")
        mapping: list[tuple[str, str]] = []
        for row in token_rows:
            if not isinstance(row, Mapping):
                raise ValueError("CLOB token row is invalid")
            mapping.append((_text(row.get("o"), field="CLOB outcome"), _text(row.get("t"), field="CLOB token")))
        if tuple(mapping) != (("Yes", target.yes_token_id), ("No", target.no_token_id)):
            raise ValueError("CLOB outcome/token mapping changed")
        if gamma_values.get("end_date") is None:
            raise ValueError("Gamma end date is missing")

    def _write_failure(
        self,
        root: Path,
        target: PolymarketCaseContextTarget,
        cutoff: datetime,
        timestamp: datetime,
        reason: str,
        captures: list[RawCapture],
        *,
        status: PolymarketCaseContextStatus = PolymarketCaseContextStatus.UNAVAILABLE_HTTP,
    ) -> PolymarketCaseContextResult:
        document = self._document(
            target=target,
            status=status,
            reason=reason,
            as_of=cutoff,
            received_at=timestamp,
            captures=captures,
            gamma_values=None,
        )
        path = self._write_document(root, document)
        return PolymarketCaseContextResult(status, reason, path, tuple(captures), document)

    @staticmethod
    def _document(
        *,
        target: PolymarketCaseContextTarget,
        status: PolymarketCaseContextStatus,
        reason: str,
        as_of: datetime,
        received_at: datetime,
        captures: list[RawCapture],
        gamma_values: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        raw_lineage = [
            {
                "source_uid": capture.source,
                "source_url": (
                    f"{POLYMARKET_GAMMA_API}/markets/{target.gamma_market_id}"
                    if capture.source == GAMMA_SOURCE_UID
                    else f"{POLYMARKET_CLOB_API}/clob-markets/{target.condition_id}"
                ),
                "raw_artifact_uid": f"polymarket:raw/{capture.sha256}",
                "sha256": capture.sha256,
                "receipt_received_at": capture.received_at.isoformat().replace("+00:00", "Z"),
                "receipt_path": str(capture.receipt_path),
            }
            for capture in captures
        ]
        return {
            "schema_version": CONTEXT_SCHEMA_VERSION,
            "status": status.value,
            "reason": reason,
            "availability": {
                "available": status == PolymarketCaseContextStatus.COLLECTED,
                "as_of": as_of.isoformat().replace("+00:00", "Z"),
                "watermark": received_at.isoformat().replace("+00:00", "Z"),
            },
            "market": {
                "market_uid": target.market_uid,
                "gamma_market_id": target.gamma_market_id,
                "condition_id": target.condition_id,
                "question": target.question,
                "rule": target.rule,
                "resolution_source": None,
                "resolution_source_status": None if gamma_values is None else gamma_values["resolution_source_status"],
                "outcomes": [
                    {"label": "Yes", "token_id": target.yes_token_id},
                    {"label": "No", "token_id": target.no_token_id},
                ],
                "end_date": None if gamma_values is None else gamma_values["end_date"].isoformat().replace("+00:00", "Z"),
                "updated_at": None if gamma_values is None or gamma_values["updated_at"] is None else gamma_values["updated_at"].isoformat().replace("+00:00", "Z"),
            },
            "raw_lineage": raw_lineage,
            "source_high_watermarks": {capture.source: capture.received_at.isoformat().replace("+00:00", "Z") for capture in captures},
            "raw_receipt_count": len(captures),
        }

    @staticmethod
    def _write_document(root: Path, document: Mapping[str, Any]) -> Path:
        root.mkdir(parents=True, exist_ok=True)
        destination = root / CONTEXT_FILENAME
        destination.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return destination


def run_approved_polymarket_case_context(
    registry_path: str | Path,
    target_uid: str,
    output_dir: str | Path,
    *,
    transport: HttpTransport | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Run the single approved case-context capture without arbitrary inputs.

    ``target_uid`` is resolved through the static source registry.  The runner
    accepts neither a URL nor a condition identifier from a caller and emits a
    compact summary; the raw Gamma/CLOB payloads remain only in the local raw
    ledger and the full context manifest.
    """

    registry_file = Path(registry_path)
    registry = load_phase15_source_registry(registry_file)
    target = registry.target(target_uid)
    if target.target_uid != "market:polymarket-btc65k-july" or target.venue != "polymarket":
        raise ValueError("target_uid is not approved for the fixed Polymarket case-context collector")
    expected_ref = "config:polymarket-btc65k-case-context-v1"
    if getattr(target, "case_context_config_ref", None) != expected_ref:
        raise ValueError("approved target does not name the fixed case-context configuration")
    config_path = registry_file.parent / "polymarket_btc65k_case_context.json"
    context_target = load_polymarket_case_context_target(config_path)
    if target.market_uid != f"polymarket:{context_target.condition_id}" or target.polymarket_asset_ids != (
        context_target.yes_token_id,
        context_target.no_token_id,
    ):
        raise ValueError("approved registry target conflicts with the fixed case-context configuration")
    timestamp = _utc(now or datetime.now(UTC), field="now")
    result = PolymarketCaseContextCollector(transport=transport or RequestsTransport()).collect(
        target=context_target,
        capture_root=output_dir,
        as_of=timestamp,
        received_at=timestamp,
    )
    return {
        "command": "collect-polymarket-case-context",
        "status": result.status.value,
        "available": bool(result.document["availability"]["available"]),
        "raw_receipt_count": len(result.raw_captures),
        "paths": {"case_context": str(result.document_path), "raw": str(Path(output_dir) / "raw")},
    }


__all__ = [
    "CONTEXT_FILENAME",
    "CONTEXT_SCHEMA_VERSION",
    "PolymarketCaseContextCollector",
    "PolymarketCaseContextResult",
    "PolymarketCaseContextStatus",
    "PolymarketCaseContextTarget",
    "load_polymarket_case_context_target",
    "run_approved_polymarket_case_context",
]
