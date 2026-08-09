"""Complete, bounded Polymarket market-population trade acquisition.

This module turns the Data API's bounded offset pagination into an explicit
coverage tree.  It never widens the requested market, never drops maker-role
trades (``takerOnly`` is always false), and never calls a source on import or
construction.  A saturated one-second interval is partitioned by the
documented BUY/SELL filter when the injected connector supports it; if that
partition is unavailable or itself saturates, the result is explicitly
partial.

The output is collection evidence, not an effectiveness or misconduct claim.
"""

from __future__ import annotations

import inspect
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, Literal, Mapping

from marketleak.domain import RawArtifact, TradeFill, TradeSide

from .connectors.http import EvidenceHttpClient
from .connectors.models import IngestionBatch
from .connectors.polymarket import PolymarketConnector
from .normalize import canonical_json_bytes, parse_json_decimal, require_text, utc_datetime
from .raw_store import RawArtifactStore, RawCapture
from .storage import NormalizedStore, WriteResult


_SCHEMA_VERSION = "polymarket-population-backfill-v1"
_LEAF_STATUSES = frozenset(
    {"complete", "split_required", "irreducibly_partial", "budget_exhausted"}
)
_SIDE_ORDER: Mapping[str | None, int] = {None: 0, "BUY": 1, "SELL": 2}
_CONDITION_ID_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OFFICIAL_CONTRACT_URI = (
    "https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets"
)
_TRADE_ORIGIN = "https://data-api.polymarket.com"
_CONTRACT_ORIGIN = "https://docs.polymarket.com"


def _time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _sha_payload(value: Any) -> str:
    return sha256(canonical_json_bytes(value)).hexdigest()


def _raw_digest(artifact: RawArtifact) -> str:
    content_hash = require_text(artifact.content_hash, "raw artifact content_hash")
    prefix = "sha256:"
    if not content_hash.startswith(prefix):
        raise ValueError("population raw artifact content_hash must use sha256")
    digest = content_hash[len(prefix) :].lower()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("population raw artifact content_hash must contain a valid SHA-256")
    expected_uid = f"polymarket:raw/{digest}"
    if artifact.raw_artifact_uid != expected_uid:
        raise ValueError("population raw artifact UID must match its content hash")
    return digest


def _semantic_fill_payload(fill: TradeFill) -> dict[str, Any]:
    """Return execution identity/content without receipt-envelope lineage."""

    payload = fill.model_dump(mode="python")
    payload.pop("ingested_at", None)
    payload.pop("raw_artifact_uid", None)
    return payload


def _full_fill_hash(fill: TradeFill) -> str:
    return _sha_payload(fill)


def _validate_sha256(value: str, name: str) -> str:
    digest = require_text(value, name).lower()
    if not _SHA256_RE.fullmatch(digest):
        raise ValueError(f"{name} must be 64 lowercase hexadecimal characters")
    return digest


@dataclass(frozen=True, slots=True)
class PolymarketReceiptBinding:
    """Authenticated raw-delivery receipt bound to its exact public query."""

    receipt_id: str
    receipt_sha256: str
    raw_sha256: str
    received_at: datetime
    request_parameters_json: str
    attempt: int
    status_code: int
    binding_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "receipt_id", require_text(self.receipt_id, "receipt_id"))
        object.__setattr__(
            self, "receipt_sha256", _validate_sha256(self.receipt_sha256, "receipt_sha256")
        )
        object.__setattr__(self, "raw_sha256", _validate_sha256(self.raw_sha256, "raw_sha256"))
        received = utc_datetime(self.received_at, "received_at")
        object.__setattr__(self, "received_at", received)
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int) or self.attempt < 1:
            raise ValueError("receipt attempt must be a positive integer")
        if (
            isinstance(self.status_code, bool)
            or not isinstance(self.status_code, int)
            or not 100 <= self.status_code <= 599
        ):
            raise ValueError("receipt status_code must be an HTTP status")
        try:
            parameters = json.loads(self.request_parameters_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("request_parameters_json must be valid JSON") from exc
        if not isinstance(parameters, Mapping):
            raise ValueError("request_parameters_json must encode an object")
        canonical = canonical_json_bytes(parameters).decode("utf-8")
        if canonical != self.request_parameters_json:
            raise ValueError("request_parameters_json must be canonical JSON")
        object.__setattr__(self, "binding_sha256", _sha_payload(self.to_payload(include_hash=False)))

    @property
    def request_parameters(self) -> dict[str, Any]:
        return json.loads(self.request_parameters_json)

    def to_payload(self, *, include_hash: bool = True) -> dict[str, Any]:
        payload = {
            "receipt_id": self.receipt_id,
            "receipt_sha256": self.receipt_sha256,
            "raw_sha256": self.raw_sha256,
            "received_at": _time(self.received_at),
            "request_parameters": self.request_parameters,
            "attempt": self.attempt,
            "status_code": self.status_code,
        }
        return {"binding_sha256": self.binding_sha256, **payload} if include_hash else payload


@dataclass(frozen=True, slots=True)
class PolymarketPopulationEvidenceBound:
    """Captured source contract plus a condition's activity lower bound.

    Gamma's ``acceptingOrdersTimestamp`` is evidence that the market was not
    accepting orders before that clock.  It is *not* evidence of the Data API's
    retention floor, which remains unknown/approximate for market queries.
    """

    condition_id: str
    gamma_market_id: str
    official_contract_uri: str
    official_contract_version: str
    official_contract_sha256: str
    contract_capture: RawCapture
    contract_receipt_id: str
    contract_receipt_sha256: str
    market_metadata_sha256: str
    market_metadata_capture: RawCapture
    market_metadata_receipt_id: str
    market_metadata_receipt_sha256: str
    market_activity_lower_bound_field: Literal["acceptingOrdersTimestamp"] = (
        "acceptingOrdersTimestamp"
    )
    market_activity_lower_bound: datetime = field(init=False)
    observed_at: datetime = field(init=False)
    bound_uid: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.condition_id) is not str or not _CONDITION_ID_RE.fullmatch(self.condition_id):
            raise ValueError("bound condition_id must be one exact 0x-prefixed 64-hex identifier")
        condition_id = self.condition_id.lower()
        gamma_market_id = require_text(self.gamma_market_id, "gamma_market_id")
        if not gamma_market_id.isdigit():
            raise ValueError("gamma_market_id must be an exact numeric string")
        if self.official_contract_uri != _OFFICIAL_CONTRACT_URI:
            raise ValueError("official_contract_uri must be the exact Polymarket trades contract")
        version = require_text(self.official_contract_version, "official_contract_version")
        if len(version) > 128:
            raise ValueError("official_contract_version is too long")
        if not isinstance(self.contract_capture, RawCapture) or not isinstance(
            self.market_metadata_capture, RawCapture
        ):
            raise ValueError("source contract and market metadata captures are required")
        contract_sha = _validate_sha256(
            self.official_contract_sha256, "official_contract_sha256"
        )
        receipt_sha = _validate_sha256(self.contract_receipt_sha256, "contract_receipt_sha256")
        receipt_id = require_text(self.contract_receipt_id, "contract_receipt_id")
        metadata_sha = _validate_sha256(self.market_metadata_sha256, "market_metadata_sha256")
        metadata_receipt_sha = _validate_sha256(
            self.market_metadata_receipt_sha256, "market_metadata_receipt_sha256"
        )
        metadata_receipt_id = require_text(
            self.market_metadata_receipt_id, "market_metadata_receipt_id"
        )
        if contract_sha != self.contract_capture.sha256:
            raise ValueError("official contract SHA-256 must match its raw capture")
        if metadata_sha != self.market_metadata_capture.sha256:
            raise ValueError("market metadata SHA-256 must match its raw capture")
        try:
            metadata = json.loads(self.market_metadata_capture.object_path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("market metadata capture is unavailable or malformed") from exc
        if not isinstance(metadata, Mapping):
            raise ValueError("market metadata capture must contain a JSON object")
        metadata_condition = metadata.get("conditionId")
        if type(metadata_condition) is not str or metadata_condition.lower() != condition_id:
            raise ValueError("market metadata conditionId does not match the bound condition")
        if str(metadata.get("id")) != gamma_market_id or type(metadata.get("id")) is not str:
            raise ValueError("market metadata market ID does not match the bound market")
        if self.market_activity_lower_bound_field != "acceptingOrdersTimestamp":
            raise ValueError(
                "market_activity_lower_bound_field must be acceptingOrdersTimestamp"
            )
        activity_lower_bound = utc_datetime(
            metadata.get(self.market_activity_lower_bound_field),
            self.market_activity_lower_bound_field,
        )
        if activity_lower_bound.microsecond or int(activity_lower_bound.timestamp()) <= 0:
            raise ValueError(
                "market activity lower bound must be a positive whole-second timestamp"
            )
        observed = max(
            self.contract_capture.received_at, self.market_metadata_capture.received_at
        )
        if observed < activity_lower_bound:
            raise ValueError("source observations cannot predate market inception")
        object.__setattr__(self, "condition_id", condition_id)
        object.__setattr__(self, "gamma_market_id", gamma_market_id)
        object.__setattr__(self, "official_contract_version", version)
        object.__setattr__(self, "official_contract_sha256", contract_sha)
        object.__setattr__(self, "contract_receipt_id", receipt_id)
        object.__setattr__(self, "contract_receipt_sha256", receipt_sha)
        object.__setattr__(self, "market_metadata_sha256", metadata_sha)
        object.__setattr__(self, "market_metadata_receipt_id", metadata_receipt_id)
        object.__setattr__(self, "market_metadata_receipt_sha256", metadata_receipt_sha)
        object.__setattr__(self, "market_activity_lower_bound", activity_lower_bound)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "bound_uid", _sha_payload(self.to_payload(include_uid=False)))

    @classmethod
    def from_capture(
        cls,
        *,
        condition_id: str,
        gamma_market_id: str,
        official_contract_version: str,
        contract_capture: RawCapture,
        market_metadata_capture: RawCapture,
        official_contract_uri: str = _OFFICIAL_CONTRACT_URI,
    ) -> "PolymarketPopulationEvidenceBound":
        receipt_bytes = contract_capture.receipt_path.read_bytes()
        try:
            receipt = json.loads(receipt_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("contract capture receipt is not valid JSON") from exc
        receipt_id = receipt.get("receipt_id") if isinstance(receipt, Mapping) else None
        metadata_receipt_bytes = market_metadata_capture.receipt_path.read_bytes()
        try:
            metadata_receipt = json.loads(metadata_receipt_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("market metadata receipt is not valid JSON") from exc
        metadata_receipt_id = (
            metadata_receipt.get("receipt_id") if isinstance(metadata_receipt, Mapping) else None
        )
        return cls(
            condition_id=condition_id,
            gamma_market_id=gamma_market_id,
            official_contract_uri=official_contract_uri,
            official_contract_version=official_contract_version,
            official_contract_sha256=contract_capture.sha256,
            contract_capture=contract_capture,
            contract_receipt_id=require_text(receipt_id, "contract receipt_id"),
            contract_receipt_sha256=sha256(receipt_bytes).hexdigest(),
            market_metadata_sha256=market_metadata_capture.sha256,
            market_metadata_capture=market_metadata_capture,
            market_metadata_receipt_id=require_text(
                metadata_receipt_id, "market metadata receipt_id"
            ),
            market_metadata_receipt_sha256=sha256(metadata_receipt_bytes).hexdigest(),
        )

    def to_payload(self, *, include_uid: bool = True) -> dict[str, Any]:
        payload = {
            "condition_id": self.condition_id,
            "gamma_market_id": self.gamma_market_id,
            "market_activity_lower_bound": _time(self.market_activity_lower_bound),
            "observed_at": _time(self.observed_at),
            "official_contract_uri": self.official_contract_uri,
            "official_contract_version": self.official_contract_version,
            "official_contract_sha256": self.official_contract_sha256,
            "contract_receipt_id": self.contract_receipt_id,
            "contract_receipt_sha256": self.contract_receipt_sha256,
            "market_metadata_sha256": self.market_metadata_sha256,
            "market_metadata_receipt_id": self.market_metadata_receipt_id,
            "market_metadata_receipt_sha256": self.market_metadata_receipt_sha256,
            "market_activity_lower_bound_field": self.market_activity_lower_bound_field,
        }
        return {"bound_uid": self.bound_uid, **payload} if include_uid else payload


@dataclass(frozen=True, slots=True)
class PolymarketPopulationRequest:
    """One immutable conditionId and inclusive, frozen second range."""

    condition_id: str
    interval_start: datetime
    interval_end: datetime
    source_bound: PolymarketPopulationEvidenceBound
    page_size: int = 1_000
    # A logical request is one page/filter call.  HTTP retries are separately
    # bounded so a source under pressure cannot multiply this budget silently.
    max_requests: int = 50_000
    max_http_attempts: int = 100_000
    max_leaves: int = 4_096
    query_uid: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.condition_id) is not str:
            raise ValueError("condition_id must be one exact string")
        condition_id = require_text(self.condition_id, "condition_id")
        if condition_id != self.condition_id or not _CONDITION_ID_RE.fullmatch(condition_id):
            raise ValueError("condition_id must be one exact 0x-prefixed 64-hex identifier")
        condition_id = condition_id.lower()
        start = utc_datetime(self.interval_start, "interval_start")
        end = utc_datetime(self.interval_end, "interval_end")
        if start.microsecond or end.microsecond:
            raise ValueError("population interval must use whole-second precision")
        if end < start:
            raise ValueError("interval_end must be at or after interval_start")
        if not isinstance(self.source_bound, PolymarketPopulationEvidenceBound):
            raise ValueError("source_bound is required")
        if self.source_bound.condition_id != condition_id:
            raise ValueError("source_bound must bind the exact condition_id")
        if int(start.timestamp()) <= 0 or start < self.source_bound.market_activity_lower_bound:
            raise ValueError("interval_start precedes the captured market activity lower bound")
        if end > self.source_bound.observed_at:
            raise ValueError("interval_end cannot be later than the source-contract observation")
        if isinstance(self.page_size, bool) or not isinstance(self.page_size, int):
            raise ValueError("page_size must be an integer")
        if not 1 <= self.page_size <= 10_000:
            raise ValueError("page_size must be in [1, 10000]")
        for name, value, maximum in (
            ("max_requests", self.max_requests, 100_000),
            ("max_http_attempts", self.max_http_attempts, 400_000),
            ("max_leaves", self.max_leaves, 65_536),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be an integer in [1, {maximum}]")
        object.__setattr__(self, "condition_id", condition_id)
        object.__setattr__(self, "interval_start", start)
        object.__setattr__(self, "interval_end", end)
        object.__setattr__(self, "query_uid", _sha_payload(self.to_payload(include_uid=False)))

    @property
    def query_filters(self) -> tuple[tuple[str, Any], ...]:
        return (
            ("end", int(self.interval_end.timestamp())),
            ("market", self.condition_id),
            ("start", int(self.interval_start.timestamp())),
            ("takerOnly", False),
        )

    def to_payload(self, *, include_uid: bool = True) -> dict[str, Any]:
        payload = {
            "schema_version": _SCHEMA_VERSION,
            "platform": "polymarket",
            "dataset": "public_market_trades",
            "condition_id": self.condition_id,
            "interval_start": _time(self.interval_start),
            "interval_end": _time(self.interval_end),
            "page_size": self.page_size,
            "max_requests": self.max_requests,
            "max_http_attempts": self.max_http_attempts,
            "max_leaves": self.max_leaves,
            "source_bound": self.source_bound.to_payload(),
            "query_filters": {name: value for name, value in self.query_filters},
        }
        return {"query_uid": self.query_uid, **payload} if include_uid else payload


@dataclass(frozen=True, slots=True)
class PolymarketPopulationLeaf:
    """One exact source query, including non-terminal split observations."""

    parent_query_uid: str
    condition_id: str
    interval_start: datetime
    interval_end: datetime
    side: Literal["BUY", "SELL"] | None
    status: Literal["complete", "split_required", "irreducibly_partial", "budget_exhausted"]
    exhausted: bool
    continuation: str | None
    retrieved_at: datetime | None
    raw_record_count: int
    fill_uids: tuple[str, ...]
    semantic_fill_sha256: tuple[str, ...]
    raw_sha256: tuple[str, ...]
    receipts: tuple[PolymarketReceiptBinding, ...]
    query_uid: str = field(init=False)
    leaf_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        parent = require_text(self.parent_query_uid, "parent_query_uid").lower()
        if len(parent) != 64 or any(character not in "0123456789abcdef" for character in parent):
            raise ValueError("parent_query_uid must be a SHA-256")
        condition_id = require_text(self.condition_id, "condition_id")
        start = utc_datetime(self.interval_start, "leaf interval_start")
        end = utc_datetime(self.interval_end, "leaf interval_end")
        retrieved = (
            utc_datetime(self.retrieved_at, "leaf retrieved_at")
            if self.retrieved_at is not None
            else None
        )
        if start.microsecond or end.microsecond or end < start:
            raise ValueError("leaf interval must be an ordered whole-second range")
        if self.side not in {None, "BUY", "SELL"}:
            raise ValueError("leaf side must be exactly BUY, SELL, or absent")
        if self.status not in _LEAF_STATUSES:
            raise ValueError("unsupported population leaf status")
        if type(self.exhausted) is not bool:
            raise ValueError("leaf exhausted must be a real boolean")
        if self.status == "complete":
            if not self.exhausted or self.continuation is not None:
                raise ValueError("complete leaf requires exhausted paging and no continuation")
        elif self.status in {"split_required", "irreducibly_partial"} and (
            self.exhausted or self.continuation is None
        ):
            raise ValueError("incomplete leaf requires a continuation and cannot be exhausted")
        elif self.status == "budget_exhausted" and self.exhausted:
            raise ValueError("budget-exhausted leaf cannot be exhausted")
        if isinstance(self.raw_record_count, bool) or not isinstance(self.raw_record_count, int):
            raise ValueError("raw_record_count must be an integer")
        if self.raw_record_count < 0:
            raise ValueError("raw_record_count must be non-negative")
        fill_uids = tuple(sorted(require_text(item, "fill_uid") for item in self.fill_uids))
        if len(fill_uids) != self.raw_record_count:
            raise ValueError("leaf fill_uids must preserve every raw normalized record")
        semantic_hashes = tuple(
            sorted(_validate_sha256(item, "semantic fill SHA-256") for item in self.semantic_fill_sha256)
        )
        if len(semantic_hashes) != self.raw_record_count:
            raise ValueError("semantic fill hashes must preserve every raw normalized record")
        hashes = tuple(sorted(require_text(item, "raw SHA-256").lower() for item in self.raw_sha256))
        if self.status != "budget_exhausted" and (not hashes or any(
            len(item) != 64 or any(character not in "0123456789abcdef" for character in item)
            for item in hashes
        )):
            raise ValueError("leaf requires valid raw SHA-256 lineage")
        receipts = tuple(
            sorted(
                self.receipts,
                key=lambda item: (
                    item.request_parameters_json,
                    item.attempt,
                    item.receipt_id,
                ),
            )
        )
        if self.status != "budget_exhausted" and not receipts:
            raise ValueError("retrieved leaf requires authenticated receipts")
        if self.status == "budget_exhausted" and not receipts and (
            hashes or fill_uids or semantic_hashes or self.raw_record_count or retrieved is not None
        ):
            raise ValueError("unqueried budget leaf cannot claim records or lineage")
        if self.status == "budget_exhausted" and receipts and (
            not hashes or retrieved is None or self.continuation is None
        ):
            raise ValueError("partially retrieved budget leaf requires lineage and continuation")
        if tuple(sorted({item.raw_sha256 for item in receipts})) != tuple(sorted(set(hashes))):
            raise ValueError("leaf raw hashes must equal its authenticated receipt union")
        object.__setattr__(self, "parent_query_uid", parent)
        object.__setattr__(self, "condition_id", condition_id)
        object.__setattr__(self, "interval_start", start)
        object.__setattr__(self, "interval_end", end)
        object.__setattr__(self, "retrieved_at", retrieved)
        object.__setattr__(self, "fill_uids", fill_uids)
        object.__setattr__(self, "semantic_fill_sha256", semantic_hashes)
        object.__setattr__(self, "raw_sha256", hashes)
        object.__setattr__(self, "receipts", receipts)
        object.__setattr__(self, "query_uid", _sha_payload(self.query_payload()))
        object.__setattr__(self, "leaf_sha256", _sha_payload(self.to_payload(include_hash=False)))

    @property
    def query_filters(self) -> tuple[tuple[str, Any], ...]:
        values: list[tuple[str, Any]] = [
            ("end", int(self.interval_end.timestamp())),
            ("market", self.condition_id),
        ]
        if self.side is not None:
            values.append(("side", self.side))
        values.extend(
            (
                ("start", int(self.interval_start.timestamp())),
                ("takerOnly", False),
            )
        )
        return tuple(sorted(values))

    def query_payload(self) -> dict[str, Any]:
        return {
            "parent_query_uid": self.parent_query_uid,
            "query_filters": {name: value for name, value in self.query_filters},
        }

    def to_payload(self, *, include_hash: bool = True) -> dict[str, Any]:
        payload = {
            "query_uid": self.query_uid,
            "parent_query_uid": self.parent_query_uid,
            "condition_id": self.condition_id,
            "interval_start": _time(self.interval_start),
            "interval_end": _time(self.interval_end),
            "side": self.side,
            "status": self.status,
            "exhausted": self.exhausted,
            "continuation": self.continuation,
            "retrieved_at": _time(self.retrieved_at) if self.retrieved_at is not None else None,
            "raw_record_count": self.raw_record_count,
            "fill_uids": list(self.fill_uids),
            "semantic_fill_sha256": list(self.semantic_fill_sha256),
            "raw_sha256": list(self.raw_sha256),
            "receipts": [item.to_payload() for item in self.receipts],
            "query_filters": {name: value for name, value in self.query_filters},
        }
        return {"leaf_sha256": self.leaf_sha256, **payload} if include_hash else payload


@dataclass(frozen=True, slots=True)
class PolymarketPopulationManifest:
    """Deterministic evidence binding for one complete or partial run."""

    request: PolymarketPopulationRequest
    complete: bool
    coverage_status: Literal["query_exhausted_coverage_limited", "partial"]
    limitation_reasons: tuple[str, ...]
    leaves: tuple[PolymarketPopulationLeaf, ...]
    raw_record_count: int
    canonical_record_count: int
    duplicate_record_count: int
    conflict_record_count: int
    canonical_fill_sha256: tuple[str, ...]
    raw_sha256: tuple[str, ...]
    receipt_ids: tuple[str, ...]
    receipt_sha256: tuple[str, ...]
    logical_request_count: int
    http_attempt_count: int
    retrieved_leaf_count: int
    source_consistent: bool
    budget_exhausted: bool
    query_exhausted: bool
    manifest_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.complete) is not bool:
            raise ValueError("manifest complete must be a real boolean")
        if self.complete:
            raise ValueError("market/event retention is approximate; manifest cannot claim complete")
        expected_status = (
            "query_exhausted_coverage_limited" if self.query_exhausted else "partial"
        )
        if self.coverage_status != expected_status:
            raise ValueError("manifest coverage_status must match query exhaustion")
        reasons = tuple(sorted({require_text(item, "limitation reason") for item in self.limitation_reasons}))
        if not reasons:
            raise ValueError("partial manifest requires an explicit limitation")
        leaves = tuple(sorted(self.leaves, key=_leaf_sort_key))
        if not leaves or any(item.parent_query_uid != self.request.query_uid for item in leaves):
            raise ValueError("manifest leaves must bind the exact parent query")
        terminal = tuple(item for item in leaves if item.status != "split_required")
        if not terminal:
            raise ValueError("manifest requires terminal coverage leaves")
        if self.complete and any(item.status != "complete" for item in terminal):
            raise ValueError("complete manifest requires every terminal leaf to be complete")
        if (
            type(self.source_consistent) is not bool
            or type(self.budget_exhausted) is not bool
            or type(self.query_exhausted) is not bool
        ):
            raise ValueError("manifest control flags must be real booleans")
        if self.query_exhausted and (
            not self.source_consistent
            or self.budget_exhausted
            or any(item.status != "complete" for item in terminal)
        ):
            raise ValueError("query exhaustion requires complete terminal leaves and consistency")
        if self.budget_exhausted != any(item.status == "budget_exhausted" for item in terminal):
            raise ValueError("budget_exhausted must match terminal budget leaves")
        counts = (
            self.raw_record_count,
            self.canonical_record_count,
            self.duplicate_record_count,
            self.conflict_record_count,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts):
            raise ValueError("manifest record counts must be non-negative integers")
        if self.raw_record_count != self.canonical_record_count + self.duplicate_record_count + self.conflict_record_count:
            raise ValueError("manifest raw record counts do not reconcile")
        if self.conflict_record_count:
            raise ValueError("a population manifest cannot authorize conflicting records")
        for name, value in (
            ("logical_request_count", self.logical_request_count),
            ("http_attempt_count", self.http_attempt_count),
            ("retrieved_leaf_count", self.retrieved_leaf_count),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"manifest {name} must be a non-negative integer")
        if self.http_attempt_count != sum(len(item.receipts) for item in leaves):
            raise ValueError("manifest http_attempt_count must equal authenticated receipt count")
        if self.logical_request_count > self.request.max_requests:
            raise ValueError("manifest logical request count exceeds its request budget")
        if self.http_attempt_count > self.request.max_http_attempts:
            raise ValueError("manifest HTTP attempt count exceeds its request budget")
        if self.retrieved_leaf_count != sum(item.status != "budget_exhausted" for item in leaves):
            raise ValueError("manifest retrieved_leaf_count does not match retrieved leaves")
        fill_hashes = tuple(sorted(require_text(item, "canonical fill SHA-256").lower() for item in self.canonical_fill_sha256))
        if len(fill_hashes) != self.canonical_record_count:
            raise ValueError("canonical fill hashes must match canonical_record_count")
        raw_hashes = tuple(sorted({require_text(item, "raw SHA-256").lower() for item in self.raw_sha256}))
        leaf_hashes = tuple(sorted({digest for item in leaves for digest in item.raw_sha256}))
        if raw_hashes != leaf_hashes:
            raise ValueError("manifest raw lineage must equal its leaf union")
        receipts = tuple(item for leaf in leaves for item in leaf.receipts)
        receipt_ids = tuple(sorted(item.receipt_id for item in receipts))
        receipt_hashes = tuple(sorted(item.receipt_sha256 for item in receipts))
        if tuple(sorted(self.receipt_ids)) != receipt_ids:
            raise ValueError("manifest receipt IDs must equal its leaf receipt union")
        if tuple(sorted(self.receipt_sha256)) != receipt_hashes:
            raise ValueError("manifest receipt hashes must equal its leaf receipt union")
        object.__setattr__(self, "limitation_reasons", reasons)
        object.__setattr__(self, "leaves", leaves)
        object.__setattr__(self, "canonical_fill_sha256", fill_hashes)
        object.__setattr__(self, "raw_sha256", raw_hashes)
        object.__setattr__(self, "receipt_ids", receipt_ids)
        object.__setattr__(self, "receipt_sha256", receipt_hashes)
        object.__setattr__(self, "manifest_sha256", _sha_payload(self.to_payload(include_hash=False)))

    def to_payload(self, *, include_hash: bool = True) -> dict[str, Any]:
        payload = {
            "schema_version": _SCHEMA_VERSION,
            "request": self.request.to_payload(),
            "complete": self.complete,
            "coverage_status": self.coverage_status,
            "limitation_reasons": list(self.limitation_reasons),
            "leaves": [item.to_payload() for item in self.leaves],
            "raw_record_count": self.raw_record_count,
            "canonical_record_count": self.canonical_record_count,
            "duplicate_record_count": self.duplicate_record_count,
            "conflict_record_count": self.conflict_record_count,
            "canonical_fill_sha256": list(self.canonical_fill_sha256),
            "raw_sha256": list(self.raw_sha256),
            "receipt_ids": list(self.receipt_ids),
            "receipt_sha256": list(self.receipt_sha256),
            "logical_request_count": self.logical_request_count,
            "http_attempt_count": self.http_attempt_count,
            "retrieved_leaf_count": self.retrieved_leaf_count,
            "source_consistent": self.source_consistent,
            "budget_exhausted": self.budget_exhausted,
            "query_exhausted": self.query_exhausted,
            "completeness_scope": (
                "only the requested post-market-inception interval; Data API retention "
                "coverage remains unknown/approximate and this is never all-time history"
            ),
        }
        return {"manifest_sha256": self.manifest_sha256, **payload} if include_hash else payload


@dataclass(frozen=True, slots=True)
class PolymarketPopulationStorageWrite:
    """Immutable summary of the append-only normalized-store operation."""

    inserted: int
    duplicates: int
    conflicts: int
    rejected: int
    paths: tuple[str, ...]
    quarantine_paths: tuple[str, ...]

    @classmethod
    def from_write_result(cls, result: WriteResult) -> "PolymarketPopulationStorageWrite":
        return cls(
            inserted=result.inserted,
            duplicates=result.duplicates,
            conflicts=result.conflicts,
            rejected=result.rejected,
            paths=tuple(sorted(str(item) for item in result.paths)),
            quarantine_paths=tuple(
                sorted(str(item.quarantine_path) for item in result.quarantined)
            ),
        )


@dataclass(frozen=True, slots=True)
class PolymarketPopulationResult:
    request: PolymarketPopulationRequest
    fills: tuple[TradeFill, ...]
    raw_artifacts: tuple[RawArtifact, ...]
    raw_captures: tuple[RawCapture, ...]
    leaves: tuple[PolymarketPopulationLeaf, ...]
    manifest: PolymarketPopulationManifest
    storage_write: PolymarketPopulationStorageWrite

    def __post_init__(self) -> None:
        fills = tuple(sorted(self.fills, key=lambda item: item.fill_uid))
        artifacts = tuple(sorted(self.raw_artifacts, key=_artifact_sort_key))
        captures = tuple(
            sorted(self.raw_captures, key=lambda item: (item.sha256, item.received_at, str(item.receipt_path)))
        )
        leaves = tuple(sorted(self.leaves, key=_leaf_sort_key))
        if self.request != self.manifest.request or leaves != self.manifest.leaves:
            raise ValueError("result request/leaves must match its manifest")
        if len(fills) != self.manifest.canonical_record_count:
            raise ValueError("result fills must match manifest canonical count")
        if tuple(sorted(_full_fill_hash(item) for item in fills)) != self.manifest.canonical_fill_sha256:
            raise ValueError("result fill content must match manifest")
        if tuple(sorted({_raw_digest(item) for item in artifacts})) != self.manifest.raw_sha256:
            raise ValueError("result raw artifacts must match manifest lineage")
        if tuple(sorted({item.sha256 for item in captures})) != self.manifest.raw_sha256:
            raise ValueError("result raw captures must match manifest lineage")
        try:
            capture_receipt_ids = tuple(sorted(item.receipt_path.stem for item in captures))
            capture_receipt_sha256 = tuple(
                sorted(sha256(item.receipt_path.read_bytes()).hexdigest() for item in captures)
            )
        except OSError as exc:
            raise ValueError("result raw-capture receipt lineage is unavailable") from exc
        if capture_receipt_ids != self.manifest.receipt_ids:
            raise ValueError("result raw captures must preserve every manifest receipt")
        if capture_receipt_sha256 != self.manifest.receipt_sha256:
            raise ValueError("result raw-capture receipt hashes must match the manifest")
        object.__setattr__(self, "fills", fills)
        object.__setattr__(self, "raw_artifacts", artifacts)
        object.__setattr__(self, "raw_captures", captures)
        object.__setattr__(self, "leaves", leaves)


class PolymarketPopulationError(RuntimeError):
    """Base class for fail-closed population acquisition errors."""


class PolymarketPopulationConflictError(PolymarketPopulationError):
    def __init__(self, fill_uids: tuple[str, ...], write_result: WriteResult):
        self.fill_uids = fill_uids
        self.write_result = write_result
        super().__init__(
            "conflicting normalized fill UID(s) were quarantined: " + ", ".join(fill_uids)
        )


def _leaf_sort_key(leaf: PolymarketPopulationLeaf) -> tuple[datetime, datetime, int, str]:
    return (
        leaf.interval_start,
        leaf.interval_end,
        _SIDE_ORDER[leaf.side],
        leaf.query_uid,
    )


def _artifact_sort_key(artifact: RawArtifact) -> tuple[str, datetime, str]:
    return (artifact.raw_artifact_uid, artifact.retrieved_at, artifact.storage_uri)


def _verify_capture_and_receipt(
    capture: RawCapture,
    *,
    expected_source: str,
    expected_origin: str,
    expected_parameters: Mapping[str, Any],
    allow_retryable_failure: bool = False,
) -> PolymarketReceiptBinding:
    """Authenticate immutable raw bytes and their canonical retrieval receipt."""

    if capture.platform != "polymarket" or capture.source != expected_source:
        raise PolymarketPopulationError("raw capture platform/source does not match the query")
    try:
        raw = capture.object_path.read_bytes()
        receipt_bytes = capture.receipt_path.read_bytes()
    except OSError as exc:
        raise PolymarketPopulationError("raw capture object or receipt is unavailable") from exc
    observed_raw_sha = sha256(raw).hexdigest()
    if observed_raw_sha != capture.sha256 or len(raw) != capture.byte_length:
        raise PolymarketPopulationError("stored raw object bytes fail hash/length verification")
    try:
        receipt = json.loads(receipt_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolymarketPopulationError("raw capture receipt is not valid JSON") from exc
    if not isinstance(receipt, Mapping) or canonical_json_bytes(receipt) != receipt_bytes:
        raise PolymarketPopulationError("raw capture receipt is not canonical JSON")
    request = receipt.get("request")
    response = receipt.get("response_metadata")
    if not isinstance(request, Mapping):
        raise PolymarketPopulationError("raw capture receipt lacks request provenance")
    status_code = response.get("status_code") if isinstance(response, Mapping) else None
    status_is_accepted = (
        isinstance(status_code, int)
        and not isinstance(status_code, bool)
        and (
            200 <= status_code < 300
            or (allow_retryable_failure and status_code in EvidenceHttpClient.RETRYABLE)
        )
    )
    if (
        not isinstance(response, Mapping)
        or not status_is_accepted
        or response.get("url") != expected_origin
    ):
        raise PolymarketPopulationError(
            "raw capture receipt does not prove a successful or retryable response"
        )
    expected = dict(sorted(expected_parameters.items()))
    if set(request) != {
        "method",
        "url",
        "parameter_names",
        "redacted_parameter_names",
        "headers",
        "secrets_redacted",
        "attempt",
        "public_parameters",
    }:
        raise PolymarketPopulationError("raw capture receipt request schema is not exact")
    if (
        request.get("method") != "GET"
        or request.get("url") != expected_origin
        or request.get("parameter_names") != sorted(expected)
        or request.get("redacted_parameter_names") != []
        or request.get("headers") != {}
        or request.get("secrets_redacted") is not False
        or request.get("public_parameters") != expected
        or isinstance(request.get("attempt"), bool)
        or not isinstance(request.get("attempt"), int)
        or request["attempt"] < 1
    ):
        raise PolymarketPopulationError("raw capture receipt does not bind the exact public request")
    try:
        received_at = utc_datetime(receipt.get("received_at"), "receipt received_at")
    except ValueError as exc:
        raise PolymarketPopulationError("raw capture receipt receive clock is invalid") from exc
    receipt_id = receipt.get("receipt_id")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("platform") != "polymarket"
        or receipt.get("source") != expected_source
        or receipt.get("sha256") != capture.sha256
        or receipt.get("byte_length") != capture.byte_length
        or received_at != capture.received_at
        or type(receipt_id) is not str
        or not receipt_id
        or capture.receipt_path.stem != receipt_id
    ):
        raise PolymarketPopulationError("raw capture receipt identity/clock does not match capture")
    return PolymarketReceiptBinding(
        receipt_id=receipt_id,
        receipt_sha256=sha256(receipt_bytes).hexdigest(),
        raw_sha256=capture.sha256,
        received_at=capture.received_at,
        request_parameters_json=canonical_json_bytes(expected).decode("utf-8"),
        attempt=request["attempt"],
        status_code=status_code,
    )


def _verify_population_evidence_bound(
    bound: PolymarketPopulationEvidenceBound,
    *,
    approved_contract_sha256: frozenset[str],
) -> None:
    if bound.official_contract_sha256 not in approved_contract_sha256:
        raise PolymarketPopulationError("official contract SHA-256 is not operator-approved")
    binding = _verify_capture_and_receipt(
        bound.contract_capture,
        expected_source="official-docs/data-api-trades",
        expected_origin=_CONTRACT_ORIGIN,
        expected_parameters={},
    )
    if (
        binding.receipt_id != bound.contract_receipt_id
        or binding.receipt_sha256 != bound.contract_receipt_sha256
        or binding.raw_sha256 != bound.official_contract_sha256
        or binding.received_at != bound.contract_capture.received_at
    ):
        raise PolymarketPopulationError("population source-contract binding is invalid")
    capture = bound.market_metadata_capture
    if (
        capture.platform != "polymarket"
        or capture.source != "polymarket:source/gamma-market-clock"
    ):
        raise PolymarketPopulationError("market metadata capture source is invalid")
    try:
        raw = capture.object_path.read_bytes()
        receipt_bytes = capture.receipt_path.read_bytes()
        receipt = json.loads(receipt_bytes)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolymarketPopulationError("market metadata capture/receipt is unavailable") from exc
    if (
        sha256(raw).hexdigest() != capture.sha256
        or len(raw) != capture.byte_length
        or not isinstance(receipt, Mapping)
        or canonical_json_bytes(receipt) != receipt_bytes
    ):
        raise PolymarketPopulationError("market metadata raw/receipt integrity failed")
    expected_url = f"https://gamma-api.polymarket.com/markets/{bound.gamma_market_id}"
    request = receipt.get("request")
    response = receipt.get("response_metadata")
    try:
        received_at = utc_datetime(receipt.get("received_at"), "metadata received_at")
    except ValueError as exc:
        raise PolymarketPopulationError("market metadata receipt clock is invalid") from exc
    if (
        receipt.get("schema_version") != 1
        or receipt.get("receipt_id") != bound.market_metadata_receipt_id
        or capture.receipt_path.stem != bound.market_metadata_receipt_id
        or sha256(receipt_bytes).hexdigest() != bound.market_metadata_receipt_sha256
        or receipt.get("platform") != "polymarket"
        or receipt.get("source") != "polymarket:source/gamma-market-clock"
        or receipt.get("sha256") != capture.sha256
        or receipt.get("byte_length") != capture.byte_length
        or received_at != capture.received_at
        or request != {"method": "GET", "url": expected_url, "params": {}}
        or not isinstance(response, Mapping)
        or isinstance(response.get("status_code"), bool)
        or not isinstance(response.get("status_code"), int)
        or not 200 <= response["status_code"] < 300
        or response.get("url") != expected_url
        or response.get("effective_url_matches_request") is not True
    ):
        raise PolymarketPopulationError("market metadata receipt does not bind the exact market")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolymarketPopulationError("market metadata body is malformed") from exc
    condition = payload.get("conditionId") if isinstance(payload, Mapping) else None
    inception = (
        payload.get(bound.market_activity_lower_bound_field)
        if isinstance(payload, Mapping)
        else None
    )
    if (
        not isinstance(payload, Mapping)
        or type(payload.get("id")) is not str
        or payload.get("id") != bound.gamma_market_id
        or type(condition) is not str
        or condition.lower() != bound.condition_id
    ):
        raise PolymarketPopulationError("market metadata body does not prove exact market identity")
    try:
        derived_lower_bound = utc_datetime(
            inception, bound.market_activity_lower_bound_field
        )
    except ValueError as exc:
        raise PolymarketPopulationError("market activity lower-bound clock is invalid") from exc
    if (
        derived_lower_bound != bound.market_activity_lower_bound
        or derived_lower_bound.microsecond
    ):
        raise PolymarketPopulationError(
            "market metadata body does not prove the market activity lower bound"
        )


class PolymarketPopulationBackfill:
    """Recursively acquire one exact conditionId into an append-only store."""

    def __init__(
        self,
        connector: Any,
        normalized_store: NormalizedStore,
        *,
        approved_contract_sha256: frozenset[str],
    ):
        if connector is None or not callable(getattr(connector, "fetch_trades", None)):
            raise ValueError("connector must expose fetch_trades")
        if not isinstance(normalized_store, NormalizedStore):
            raise ValueError("normalized_store must be a NormalizedStore")
        if not isinstance(approved_contract_sha256, frozenset) or not approved_contract_sha256:
            raise ValueError("approved_contract_sha256 must be a non-empty frozen policy set")
        approved = frozenset(
            _validate_sha256(item, "approved contract SHA-256")
            for item in approved_contract_sha256
        )
        self._connector = connector
        self._store = normalized_store
        self._approved_contract_sha256 = approved
        # Construction only inspects the callable contract; it performs no IO.
        # The production connector intentionally exposes ``fetch_trades`` as a
        # convenience ``**kwargs`` envelope.  Its typed ``iter_trade_pages``
        # contract is the authoritative capability surface.
        capability_callable = getattr(connector, "iter_trade_pages", connector.fetch_trades)
        parameters = inspect.signature(capability_callable).parameters
        self._supports_side = "side" in parameters
        self._supports_http_attempt_limit = "http_attempt_limit" in parameters

    def collect(self, request: PolymarketPopulationRequest) -> PolymarketPopulationResult:
        if not isinstance(request, PolymarketPopulationRequest):
            raise ValueError("request must be a PolymarketPopulationRequest")
        _verify_population_evidence_bound(
            request.source_bound,
            approved_contract_sha256=self._approved_contract_sha256,
        )
        leaves: list[PolymarketPopulationLeaf] = []
        observed_fills: list[TradeFill] = []
        raw_artifacts: list[RawArtifact] = []
        raw_captures: list[RawCapture] = []
        logical_request_count = 0
        http_attempt_count = 0

        def visit(start: datetime, end: datetime, side: Literal["BUY", "SELL"] | None) -> None:
            nonlocal logical_request_count, http_attempt_count
            retrieved_leaves = sum(item.status != "budget_exhausted" for item in leaves)
            if (
                logical_request_count >= request.max_requests
                or http_attempt_count >= request.max_http_attempts
                or retrieved_leaves >= request.max_leaves
            ):
                leaves.append(
                    self._leaf(
                        request=request,
                        start=start,
                        end=end,
                        side=side,
                        status="budget_exhausted",
                        fills=(),
                        artifacts=(),
                        receipts=(),
                        continuation=None,
                    )
                )
                return
            (
                fills,
                artifacts,
                captures,
                receipts,
                logical_requests,
                query_state,
                continuation,
            ) = self._terminal_query(
                request=request,
                start=start,
                end=end,
                side=side,
                remaining_requests=request.max_requests - logical_request_count,
                remaining_http_attempts=request.max_http_attempts - http_attempt_count,
            )
            logical_request_count += logical_requests
            http_attempt_count += len(receipts)
            observed_fills.extend(fills)
            raw_artifacts.extend(artifacts)
            raw_captures.extend(captures)
            status: Literal[
                "complete", "split_required", "irreducibly_partial", "budget_exhausted"
            ]
            if query_state == "complete":
                status = "complete"
            elif query_state == "budget_exhausted":
                status = "budget_exhausted"
            elif start < end or (side is None and self._supports_side):
                status = "split_required"
            else:
                status = "irreducibly_partial"
            leaf = self._leaf(
                request=request,
                start=start,
                end=end,
                side=side,
                status=status,
                fills=fills,
                artifacts=artifacts,
                receipts=receipts,
                continuation=continuation,
            )
            leaves.append(leaf)
            if status in {"complete", "budget_exhausted", "irreducibly_partial"}:
                return
            if start < end:
                middle_epoch = (int(start.timestamp()) + int(end.timestamp())) // 2
                middle = datetime.fromtimestamp(middle_epoch, tz=UTC)
                visit(start, middle, side)
                visit(middle + timedelta(seconds=1), end, side)
                return
            if side is None and self._supports_side:
                visit(start, end, "BUY")
                visit(start, end, "SELL")

        visit(request.interval_start, request.interval_end, None)

        ordered_observed = tuple(
            sorted(observed_fills, key=lambda item: (item.fill_uid, _full_fill_hash(item)))
        )
        write_result = self._store.write(ordered_observed, record_type="tradefill")
        canonical, duplicate_count, conflicting_uids = self._deduplicate(ordered_observed)
        if write_result.rejected:
            raise PolymarketPopulationError(
                f"normalized store rejected {write_result.rejected} population record(s)"
            )
        if write_result.conflicts or conflicting_uids:
            uids = tuple(
                sorted(
                    {
                        *conflicting_uids,
                        *(item.uid for item in write_result.quarantined),
                    }
                )
            )
            raise PolymarketPopulationConflictError(uids, write_result)

        ordered_leaves = tuple(sorted(leaves, key=_leaf_sort_key))
        terminal_leaves = tuple(item for item in ordered_leaves if item.status != "split_required")
        source_consistent = True
        for parent in (item for item in ordered_leaves if item.status == "split_required"):
            descendant_semantics = {
                digest
                for item in terminal_leaves
                if item.interval_start >= parent.interval_start
                and item.interval_end <= parent.interval_end
                and (parent.side is None or item.side == parent.side)
                for digest in item.semantic_fill_sha256
            }
            if not set(parent.semantic_fill_sha256).issubset(descendant_semantics):
                source_consistent = False
                break
        budget_exhausted = any(item.status == "budget_exhausted" for item in terminal_leaves)
        query_exhausted = (
            bool(terminal_leaves)
            and all(item.status == "complete" for item in terminal_leaves)
            and source_consistent
            and not budget_exhausted
        )
        # The official contract describes only an approximate market/event
        # retention horizon. Exhausting every requested page is therefore not
        # promoted into an all-time or source-retention completeness claim.
        complete = False
        reason_set: set[str] = set()
        reason_set.add("data_api_retention_floor_unknown_or_approximate")
        if budget_exhausted:
            reason_set.add("budget_exhausted")
            if http_attempt_count >= request.max_http_attempts:
                reason_set.add("http_attempt_budget_exhausted")
        if not source_consistent:
            reason_set.add("source_inconsistent")
        for item in terminal_leaves:
            if item.status == "irreducibly_partial":
                reason_set.add(
                    "one_second_side_partition_unavailable"
                    if item.side is None
                    else "one_second_side_partition_exhausted_at_offset_ceiling"
                )
        reasons = tuple(sorted(reason_set))
        ordered_artifacts = tuple(sorted(raw_artifacts, key=_artifact_sort_key))
        manifest = PolymarketPopulationManifest(
            request=request,
            complete=complete,
            coverage_status=(
                "query_exhausted_coverage_limited" if query_exhausted else "partial"
            ),
            limitation_reasons=reasons,
            leaves=ordered_leaves,
            raw_record_count=len(ordered_observed),
            canonical_record_count=len(canonical),
            duplicate_record_count=duplicate_count,
            conflict_record_count=0,
            canonical_fill_sha256=tuple(_full_fill_hash(item) for item in canonical),
            raw_sha256=tuple(_raw_digest(item) for item in ordered_artifacts),
            receipt_ids=tuple(
                binding.receipt_id for leaf in ordered_leaves for binding in leaf.receipts
            ),
            receipt_sha256=tuple(
                binding.receipt_sha256 for leaf in ordered_leaves for binding in leaf.receipts
            ),
            logical_request_count=logical_request_count,
            http_attempt_count=http_attempt_count,
            retrieved_leaf_count=sum(
                item.status != "budget_exhausted" for item in ordered_leaves
            ),
            source_consistent=source_consistent,
            budget_exhausted=budget_exhausted,
            query_exhausted=query_exhausted,
        )
        return PolymarketPopulationResult(
            request=request,
            fills=canonical,
            raw_artifacts=ordered_artifacts,
            raw_captures=tuple(raw_captures),
            leaves=ordered_leaves,
            manifest=manifest,
            storage_write=PolymarketPopulationStorageWrite.from_write_result(write_result),
        )

    def _terminal_query(
        self,
        *,
        request: PolymarketPopulationRequest,
        start: datetime,
        end: datetime,
        side: Literal["BUY", "SELL"] | None,
        remaining_requests: int,
        remaining_http_attempts: int,
    ) -> tuple[
        tuple[TradeFill, ...],
        tuple[RawArtifact, ...],
        tuple[RawCapture, ...],
        tuple[PolymarketReceiptBinding, ...],
        int,
        Literal["complete", "split_required", "budget_exhausted"],
        str | None,
    ]:
        fills: list[TradeFill] = []
        artifacts: list[RawArtifact] = []
        captures: list[RawCapture] = []
        receipts: list[PolymarketReceiptBinding] = []
        logical_requests = 0
        continuation: str | None = None
        seen_continuations: set[str] = set()
        ceiling = getattr(self._connector, "MAX_TRADE_OFFSET", 10_000)
        if isinstance(ceiling, bool) or not isinstance(ceiling, int) or ceiling < 0:
            raise PolymarketPopulationError("connector MAX_TRADE_OFFSET is invalid")
        while True:
            if (
                logical_requests >= remaining_requests
                or len(receipts) >= remaining_http_attempts
            ):
                return (
                    tuple(fills),
                    tuple(artifacts),
                    tuple(captures),
                    tuple(receipts),
                    logical_requests,
                    "budget_exhausted",
                    continuation,
                )
            offset = self._continuation_offset(continuation)
            kwargs: dict[str, Any] = {
                "market": request.condition_id,
                "start": start,
                "end": end,
                "taker_only": False,
                "page_size": request.page_size,
                # One page per call lets this orchestrator enforce its exact
                # request budget and authenticate one receipt at a time.
                "max_pages": 1,
                "continuation": continuation,
            }
            if side is not None:
                kwargs["side"] = side
            if self._supports_http_attempt_limit:
                configured_attempts = getattr(
                    getattr(self._connector, "http", None), "max_attempts", 1
                )
                if (
                    isinstance(configured_attempts, bool)
                    or not isinstance(configured_attempts, int)
                    or configured_attempts < 1
                ):
                    raise PolymarketPopulationError(
                        "connector HTTP max_attempts contract is invalid"
                    )
                kwargs["http_attempt_limit"] = min(
                    configured_attempts,
                    remaining_http_attempts - len(receipts),
                )
            batch = self._connector.fetch_trades(**kwargs)
            logical_requests += 1
            if not isinstance(batch, IngestionBatch):
                raise PolymarketPopulationError("connector returned an invalid ingestion batch")
            bindings = self._validate_batch(
                batch,
                condition_id=request.condition_id,
                start=start,
                end=end,
                side=side,
                page_size=request.page_size,
                offset=offset,
            )
            if len(receipts) + len(bindings) > remaining_http_attempts:
                raise PolymarketPopulationError(
                    "connector exceeded the explicit total HTTP-attempt budget"
                )
            fills.extend(batch.fills)
            artifacts.extend(batch.raw_artifacts)
            captures.extend(batch.raw_captures)
            receipts.extend(bindings)
            if batch.complete:
                if batch.continuation is not None:
                    raise PolymarketPopulationError(
                        "connector marked paging complete while returning a continuation"
                    )
                return (
                    tuple(fills),
                    tuple(artifacts),
                    tuple(captures),
                    tuple(receipts),
                    logical_requests,
                    "complete",
                    None,
                )
            state = self._continuation_state(batch.continuation)
            if state == "split_required":
                return (
                    tuple(fills),
                    tuple(artifacts),
                    tuple(captures),
                    tuple(receipts),
                    logical_requests,
                    "split_required",
                    batch.continuation,
                )
            if state != "page":
                raise PolymarketPopulationError("incomplete connector batch has no valid paging state")
            assert batch.continuation is not None
            if batch.continuation in seen_continuations:
                raise PolymarketPopulationError("connector repeated a continuation without progress")
            seen_continuations.add(batch.continuation)
            continuation = batch.continuation

    @staticmethod
    def _continuation_offset(continuation: str | None) -> int:
        if continuation is None:
            return 0
        try:
            payload = json.loads(continuation)
        except (TypeError, json.JSONDecodeError) as exc:
            raise PolymarketPopulationError("connector continuation is not valid JSON") from exc
        if not isinstance(payload, Mapping):
            raise PolymarketPopulationError("connector continuation must be an object")
        offset = payload.get("offset")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise PolymarketPopulationError("connector continuation offset is invalid")
        return offset

    @staticmethod
    def _continuation_state(continuation: str | None) -> str | None:
        if continuation is None:
            return None
        try:
            payload = json.loads(continuation)
        except (TypeError, json.JSONDecodeError) as exc:
            raise PolymarketPopulationError("connector continuation is not valid JSON") from exc
        if not isinstance(payload, Mapping) or payload.get("version") != 1:
            raise PolymarketPopulationError("connector continuation has an unsupported schema")
        state = payload.get("state")
        return state if isinstance(state, str) else None

    @staticmethod
    def _validate_batch(
        batch: IngestionBatch,
        *,
        condition_id: str,
        start: datetime,
        end: datetime,
        side: Literal["BUY", "SELL"] | None,
        page_size: int,
        offset: int,
    ) -> tuple[PolymarketReceiptBinding, ...]:
        if not batch.raw_artifacts:
            raise PolymarketPopulationError("connector batch lacks raw artifact lineage")
        if len(batch.raw_artifacts) != len(batch.raw_captures) or not batch.raw_captures:
            raise PolymarketPopulationError(
                "one-page population batch must preserve every HTTP attempt capture/artifact"
            )
        expected_parameters: dict[str, Any] = {
            "limit": page_size,
            "offset": offset,
            "market": condition_id,
            "start": int(start.timestamp()),
            "end": int(end.timestamp()),
            "takerOnly": False,
        }
        if side is not None:
            expected_parameters["side"] = side
        bindings: list[PolymarketReceiptBinding] = []
        for ordinal, (capture, artifact) in enumerate(
            zip(batch.raw_captures, batch.raw_artifacts, strict=True), start=1
        ):
            expected_artifact = RawArtifactStore.to_domain(
                capture,
                source_uid=PolymarketConnector.TRADE_SOURCE_UID,
                parser_version=PolymarketConnector.PARSER_VERSION,
            )
            if artifact != expected_artifact:
                raise PolymarketPopulationError(
                    "raw artifact domain lineage conflicts with its HTTP attempt capture"
                )
            binding = _verify_capture_and_receipt(
                capture,
                expected_source="data-api/trades",
                expected_origin=_TRADE_ORIGIN,
                expected_parameters=expected_parameters,
                allow_retryable_failure=ordinal < len(batch.raw_captures),
            )
            if binding.attempt != ordinal:
                raise PolymarketPopulationError(
                    "HTTP attempt receipts must be a contiguous ordered sequence"
                )
            if ordinal < len(batch.raw_captures) and binding.status_code not in EvidenceHttpClient.RETRYABLE:
                raise PolymarketPopulationError(
                    "a preceding HTTP attempt is not a documented retryable failure"
                )
            if ordinal == len(batch.raw_captures) and not 200 <= binding.status_code < 300:
                raise PolymarketPopulationError(
                    "the final HTTP attempt does not prove a successful response"
                )
            bindings.append(binding)
        capture = batch.raw_captures[-1]
        try:
            raw_rows = parse_json_decimal(capture.object_path.read_bytes())
        except (UnicodeDecodeError, ValueError) as exc:
            raise PolymarketPopulationError("verified raw trade page is not valid JSON") from exc
        if not isinstance(raw_rows, list) or len(raw_rows) != len(batch.fills):
            raise PolymarketPopulationError("raw trade page count does not match normalized fills")
        if any(not isinstance(item, Mapping) for item in raw_rows):
            raise PolymarketPopulationError("raw trade page contains a non-object row")
        try:
            reparsed_fills = tuple(
                PolymarketConnector.normalize_trade(item, capture) for item in raw_rows
            )
        except (TypeError, ValueError) as exc:
            raise PolymarketPopulationError(
                "verified raw trade row fails the production Polymarket parser"
            ) from exc
        if tuple(batch.fills) != reparsed_fills:
            raise PolymarketPopulationError(
                "connector fills do not exactly equal ordered production-parser output"
            )
        expected_market_uid = f"polymarket:market/{condition_id}"
        expected_side = TradeSide(side.lower()) if side is not None else None
        for fill in reparsed_fills:
            if fill.platform != "polymarket" or fill.market_uid != expected_market_uid:
                raise PolymarketPopulationError("connector returned a fill outside the exact conditionId")
            if not start <= fill.event_time <= end:
                raise PolymarketPopulationError("connector returned a fill outside the frozen interval")
            if expected_side is not None and fill.side != expected_side:
                raise PolymarketPopulationError("connector returned a fill outside the exact side partition")
            if fill.raw_artifact_uid != f"polymarket:raw/{capture.sha256}":
                raise PolymarketPopulationError("normalized fill lacks final-response raw lineage")
        return tuple(bindings)

    @staticmethod
    def _leaf(
        *,
        request: PolymarketPopulationRequest,
        start: datetime,
        end: datetime,
        side: Literal["BUY", "SELL"] | None,
        status: Literal[
            "complete", "split_required", "irreducibly_partial", "budget_exhausted"
        ],
        fills: tuple[TradeFill, ...],
        artifacts: tuple[RawArtifact, ...],
        receipts: tuple[PolymarketReceiptBinding, ...],
        continuation: str | None,
    ) -> PolymarketPopulationLeaf:
        if status != "budget_exhausted" and not artifacts:
            raise PolymarketPopulationError("population leaf lacks raw artifacts")
        return PolymarketPopulationLeaf(
            parent_query_uid=request.query_uid,
            condition_id=request.condition_id,
            interval_start=start,
            interval_end=end,
            side=side,
            status=status,
            exhausted=status == "complete",
            continuation=continuation,
            retrieved_at=max((item.retrieved_at for item in artifacts), default=None),
            raw_record_count=len(fills),
            fill_uids=tuple(item.fill_uid for item in fills),
            semantic_fill_sha256=tuple(
                _sha_payload(_semantic_fill_payload(item)) for item in fills
            ),
            raw_sha256=tuple(_raw_digest(item) for item in artifacts),
            receipts=receipts,
        )

    @staticmethod
    def _deduplicate(
        fills: tuple[TradeFill, ...],
    ) -> tuple[tuple[TradeFill, ...], int, tuple[str, ...]]:
        by_uid: dict[str, tuple[TradeFill, str]] = {}
        duplicates = 0
        conflicts: set[str] = set()
        for fill in fills:
            semantic_hash = _sha_payload(_semantic_fill_payload(fill))
            previous = by_uid.get(fill.fill_uid)
            if previous is None:
                by_uid[fill.fill_uid] = (fill, semantic_hash)
            elif previous[1] == semantic_hash:
                duplicates += 1
                if _full_fill_hash(fill) < _full_fill_hash(previous[0]):
                    by_uid[fill.fill_uid] = (fill, semantic_hash)
            else:
                conflicts.add(fill.fill_uid)
        canonical = tuple(sorted((item[0] for item in by_uid.values()), key=lambda item: item.fill_uid))
        return canonical, duplicates, tuple(sorted(conflicts))


__all__ = [
    "PolymarketPopulationBackfill",
    "PolymarketPopulationConflictError",
    "PolymarketPopulationError",
    "PolymarketPopulationLeaf",
    "PolymarketPopulationManifest",
    "PolymarketPopulationRequest",
    "PolymarketPopulationResult",
    "PolymarketPopulationStorageWrite",
    "PolymarketReceiptBinding",
    "PolymarketPopulationEvidenceBound",
]
