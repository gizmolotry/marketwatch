"""Bounded, raw-first Bitcoin address context from Mempool's public REST API.

The collector accepts only an explicit local watch registration and an injected
HTTP transport.  It records public address state, UTXOs, and transaction
timeline entries as observed data.  It does not infer an owner or a purpose
from an address or transaction.

API contract used here:
https://mempool.space/docs/api/rest
"""

from __future__ import annotations

import hashlib
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Mapping, Protocol, Sequence

from pydantic import Field, field_validator, model_validator

from marketleak.domain import CoverageStatus, RawArtifact, StrictDomainModel
from marketleak.domain.common import NonEmptyStr, StableUID
from marketleak.ingestion.normalize import canonical_json_bytes, stable_uid, utc_datetime
from marketleak.ingestion.raw_store import RawArtifactStore, RawCapture


MEMPOOL_API_BASE_URL = "https://mempool.space/api"
MEMPOOL_SOURCE_UID = "bitcoin:source/mempool-address-rest"
BITCOIN_CONTEXT_PARSER_VERSION = "bitcoin-address-context-v1.0.0"
CONFIRMED_PAGE_SIZE = 25
_TXID_RE = re.compile(r"^[0-9a-f]{64}$")
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_INDEX = {character: index for index, character in enumerate(_BASE58_ALPHABET)}
_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_BECH32_INDEX = {character: index for index, character in enumerate(_BECH32_CHARSET)}
_BECH32M_CONST = 0x2BC830A3


class BitcoinContextError(ValueError):
    """Raised when an explicit context request cannot be represented safely."""


class BitcoinAddressFormatError(BitcoinContextError):
    """Raised after local Bitcoin mainnet address validation fails."""


@dataclass(frozen=True, slots=True)
class MempoolHttpResponse:
    status_code: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)
    url: str = ""


class MempoolHttpTransport(Protocol):
    def get(self, url: str, *, timeout: float, headers: Mapping[str, str]) -> MempoolHttpResponse: ...


class UrllibMempoolTransport:
    """Explicit transport used only by the CLI collection command."""

    def get(self, url: str, *, timeout: float, headers: Mapping[str, str]) -> MempoolHttpResponse:
        request = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return MempoolHttpResponse(
                    status_code=int(response.status),
                    body=response.read(),
                    headers=dict(response.headers.items()),
                    url=response.geturl(),
                )
        except urllib.error.HTTPError as exc:
            return MempoolHttpResponse(
                status_code=int(exc.code),
                body=exc.read(),
                headers=dict(exc.headers.items()) if exc.headers is not None else {},
                url=exc.geturl(),
            )


def _bech32_polymod(values: Iterable[int]) -> int:
    checksum = 1
    for value in values:
        top = checksum >> 25
        checksum = (checksum & 0x1FFFFFF) << 5 ^ value
        for index, generator in enumerate((0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)):
            if (top >> index) & 1:
                checksum ^= generator
    return checksum


def _bech32_hrp_expand(hrp: str) -> list[int]:
    return [ord(character) >> 5 for character in hrp] + [0] + [ord(character) & 31 for character in hrp]


def _convert_bits(data: Sequence[int], *, from_bits: int, to_bits: int) -> list[int]:
    accumulator = 0
    bits = 0
    result: list[int] = []
    maximum = (1 << to_bits) - 1
    for value in data:
        if value < 0 or value >> from_bits:
            raise BitcoinAddressFormatError("invalid witness program encoding")
        accumulator = (accumulator << from_bits) | value
        bits += from_bits
        while bits >= to_bits:
            bits -= to_bits
            result.append((accumulator >> bits) & maximum)
    if bits >= from_bits or ((accumulator << (to_bits - bits)) & maximum):
        raise BitcoinAddressFormatError("invalid witness program padding")
    return result


def _validate_bech32_mainnet(address: str) -> str:
    if address.lower() != address and address.upper() != address:
        raise BitcoinAddressFormatError("Bech32 address cannot mix case")
    normalized = address.lower()
    if not 14 <= len(normalized) <= 90:
        raise BitcoinAddressFormatError("Bech32 address length is invalid")
    separator = normalized.rfind("1")
    if separator < 1 or separator + 7 > len(normalized):
        raise BitcoinAddressFormatError("Bech32 separator or checksum is invalid")
    if normalized[:separator] != "bc":
        raise BitcoinAddressFormatError("only Bitcoin mainnet Bech32 addresses are accepted")
    try:
        data = [_BECH32_INDEX[character] for character in normalized[separator + 1:]]
    except KeyError as exc:
        raise BitcoinAddressFormatError("Bech32 address contains an invalid character") from exc
    encoding = _bech32_polymod(_bech32_hrp_expand("bc") + data)
    if encoding not in {1, _BECH32M_CONST}:
        raise BitcoinAddressFormatError("Bech32 checksum is invalid")
    witness_version = data[0]
    if witness_version > 16:
        raise BitcoinAddressFormatError("witness version is invalid")
    program = _convert_bits(data[1:-6], from_bits=5, to_bits=8)
    if not 2 <= len(program) <= 40:
        raise BitcoinAddressFormatError("witness program length is invalid")
    if witness_version == 0 and (encoding != 1 or len(program) not in {20, 32}):
        raise BitcoinAddressFormatError("version-zero witness program is invalid")
    if witness_version != 0 and encoding != _BECH32M_CONST:
        raise BitcoinAddressFormatError("post-version-zero witness program requires Bech32m")
    return normalized


def _validate_base58_mainnet(address: str) -> str:
    if not 26 <= len(address) <= 35 or address[0] not in {"1", "3"}:
        raise BitcoinAddressFormatError("legacy Bitcoin mainnet address shape is invalid")
    number = 0
    try:
        for character in address:
            number = number * 58 + _BASE58_INDEX[character]
    except KeyError as exc:
        raise BitcoinAddressFormatError("legacy Bitcoin address contains an invalid character") from exc
    decoded = number.to_bytes((number.bit_length() + 7) // 8, byteorder="big")
    decoded = b"\x00" * (len(address) - len(address.lstrip("1"))) + decoded
    if len(decoded) != 25:
        raise BitcoinAddressFormatError("legacy Bitcoin address payload length is invalid")
    payload, checksum = decoded[:-4], decoded[-4:]
    if payload[0] not in {0x00, 0x05}:
        raise BitcoinAddressFormatError("legacy Bitcoin address version is invalid")
    if hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4] != checksum:
        raise BitcoinAddressFormatError("legacy Bitcoin address checksum is invalid")
    return address


def validate_bitcoin_mainnet_address(value: str) -> str:
    """Validate a mainnet address locally without making a network request."""

    if not isinstance(value, str) or value != value.strip() or not value:
        raise BitcoinAddressFormatError("Bitcoin address must be a non-empty trimmed string")
    if value.lower().startswith("bc1"):
        return _validate_bech32_mainnet(value)
    return _validate_base58_mainnet(value)


class BitcoinWatchRegistration(StrictDomainModel):
    """A deliberate local registration for one public Bitcoin address."""

    registration_uid: StableUID
    address: NonEmptyStr
    registered_at: datetime
    network: Literal["bitcoin-mainnet"] = "bitcoin-mainnet"

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        return validate_bitcoin_mainnet_address(value)

    @field_validator("registered_at")
    @classmethod
    def validate_registered_at(cls, value: datetime) -> datetime:
        return utc_datetime(value, "registered_at")


class BitcoinAddressStats(StrictDomainModel):
    tx_count: int = Field(strict=True, ge=0)
    funded_txo_count: int = Field(strict=True, ge=0)
    funded_txo_sum_sats: int = Field(strict=True, ge=0)
    spent_txo_count: int = Field(strict=True, ge=0)
    spent_txo_sum_sats: int = Field(strict=True, ge=0)


class BitcoinObservedFact(StrictDomainModel):
    """Shared raw and temporal lineage for neutral public Bitcoin observations."""

    fact_uid: StableUID
    registration_uid: StableUID
    address: NonEmptyStr
    observed_at: datetime
    retrieved_at: datetime
    as_of: datetime
    source_uid: StableUID
    raw_artifact_uid: StableUID
    parser_version: NonEmptyStr
    raw_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        return validate_bitcoin_mainnet_address(value)

    @field_validator("observed_at", "retrieved_at", "as_of")
    @classmethod
    def validate_times(cls, value: datetime, info) -> datetime:
        return utc_datetime(value, info.field_name)

    @model_validator(mode="after")
    def validate_time_order(self) -> "BitcoinObservedFact":
        if self.observed_at > self.retrieved_at or self.retrieved_at > self.as_of:
            raise ValueError("observed_at <= retrieved_at <= as_of is required")
        return self


class BitcoinAddressSummaryFact(BitcoinObservedFact):
    chain_stats: BitcoinAddressStats
    mempool_stats: BitcoinAddressStats


class BitcoinUtxoFact(BitcoinObservedFact):
    txid: str = Field(pattern=r"^[0-9a-f]{64}$")
    vout: int = Field(strict=True, ge=0)
    value_sats: int = Field(strict=True, ge=0)
    state: Literal["confirmed", "mempool"]
    block_height: int | None = Field(default=None, strict=True, ge=0)
    block_time: datetime | None = None

    @field_validator("block_time")
    @classmethod
    def validate_block_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else utc_datetime(value, "block_time")


class BitcoinTransactionTimelineFact(BitcoinObservedFact):
    txid: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: Literal["confirmed", "mempool"]
    version: int = Field(strict=True)
    locktime: int = Field(strict=True, ge=0)
    size_bytes: int = Field(strict=True, ge=0)
    weight_units: int = Field(strict=True, ge=0)
    fee_sats: int = Field(strict=True, ge=0)
    block_height: int | None = Field(default=None, strict=True, ge=0)
    block_time: datetime | None = None

    @field_validator("block_time")
    @classmethod
    def validate_block_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else utc_datetime(value, "block_time")


class BitcoinSourceCoverage(StrictDomainModel):
    coverage_uid: StableUID
    registration_uid: StableUID
    address: NonEmptyStr
    endpoint: Literal["address", "utxo", "confirmed_txs", "mempool_txs"]
    status: CoverageStatus
    observed_at: datetime
    retrieved_at: datetime
    as_of: datetime
    source_uid: StableUID
    parser_version: NonEmptyStr
    raw_artifact_uids: tuple[StableUID, ...]
    raw_content_sha256: tuple[str, ...]
    requested_max_pages: int = Field(strict=True, ge=1)
    pages_fetched: int = Field(strict=True, ge=0)
    reason_codes: tuple[NonEmptyStr, ...] = ()

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        return validate_bitcoin_mainnet_address(value)

    @field_validator("observed_at", "retrieved_at", "as_of")
    @classmethod
    def validate_times(cls, value: datetime, info) -> datetime:
        return utc_datetime(value, info.field_name)

    @model_validator(mode="after")
    def validate_coverage(self) -> "BitcoinSourceCoverage":
        if self.observed_at > self.retrieved_at or self.retrieved_at > self.as_of:
            raise ValueError("coverage timestamps must be causally ordered")
        if len(self.raw_artifact_uids) != len(self.raw_content_sha256):
            raise ValueError("coverage raw artifact and digest counts must match")
        if any(not _TXID_RE.fullmatch(value) for value in self.raw_content_sha256):
            raise ValueError("coverage raw digests must be SHA-256 values")
        return self


class BitcoinContextSnapshot(StrictDomainModel):
    snapshot_uid: StableUID
    as_of: datetime
    registrations: tuple[BitcoinWatchRegistration, ...]
    summaries: tuple[BitcoinAddressSummaryFact, ...]
    utxos: tuple[BitcoinUtxoFact, ...]
    timeline: tuple[BitcoinTransactionTimelineFact, ...]
    coverage: tuple[BitcoinSourceCoverage, ...]
    raw_artifacts: tuple[RawArtifact, ...]

    @field_validator("as_of")
    @classmethod
    def validate_as_of(cls, value: datetime) -> datetime:
        return utc_datetime(value, "as_of")

    @model_validator(mode="after")
    def validate_snapshot_boundary(self) -> "BitcoinContextSnapshot":
        for fact in (*self.summaries, *self.utxos, *self.timeline):
            if fact.as_of != self.as_of:
                raise ValueError("all facts must use the snapshot as_of boundary")
        for coverage in self.coverage:
            if coverage.as_of != self.as_of:
                raise ValueError("all coverage must use the snapshot as_of boundary")
        return self


@dataclass(frozen=True, slots=True)
class _EndpointFetch:
    endpoint: Literal["address", "utxo", "confirmed_txs", "mempool_txs"]
    page_index: int
    capture: RawCapture | None
    payload: Any | None
    error_code: str | None = None


def _raw_uid(capture: RawCapture) -> str:
    return f"bitcoin:raw/{capture.sha256}"


def _nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _txid(value: Any, field_name: str = "txid") -> str:
    if not isinstance(value, str) or not _TXID_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lowercase hexadecimal transaction id")
    return value


class BitcoinAddressContextCollector:
    """Collect one bounded Mempool REST context snapshot for explicit registrations."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        transport: MempoolHttpTransport,
        clock: Callable[[], datetime] | None = None,
        base_url: str = MEMPOOL_API_BASE_URL,
        timeout_seconds: float = 15.0,
        max_confirmed_pages: int = 2,
        max_items_per_response: int = 1_000,
        max_timeline_entries: int = 500,
        parser_version: str = BITCOIN_CONTEXT_PARSER_VERSION,
    ) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store must be a RawArtifactStore")
        if timeout_seconds <= 0 or timeout_seconds > 60:
            raise ValueError("timeout_seconds must be in (0, 60]")
        if not 1 <= max_confirmed_pages <= 20:
            raise ValueError("max_confirmed_pages must be in [1, 20]")
        if not 1 <= max_items_per_response <= 10_000:
            raise ValueError("max_items_per_response must be in [1, 10000]")
        if not 1 <= max_timeline_entries <= 10_000:
            raise ValueError("max_timeline_entries must be in [1, 10000]")
        normalized_base = str(base_url).rstrip("/")
        parsed_base = urllib.parse.urlsplit(normalized_base)
        if parsed_base.scheme not in {"http", "https"} or not parsed_base.netloc:
            raise ValueError("base_url must be an absolute HTTP(S) URL")
        self.raw_store = raw_store
        self.transport = transport
        self.clock = clock or (lambda: datetime.now(UTC))
        self.base_url = normalized_base
        self.timeout_seconds = timeout_seconds
        self.max_confirmed_pages = max_confirmed_pages
        self.max_items_per_response = max_items_per_response
        self.max_timeline_entries = max_timeline_entries
        self.parser_version = parser_version.strip()
        if not self.parser_version:
            raise ValueError("parser_version is required")

    def _url(self, registration: BitcoinWatchRegistration, suffix: str = "") -> str:
        address = urllib.parse.quote(registration.address, safe="")
        return f"{self.base_url}/address/{address}{suffix}"

    def _fetch(
        self,
        registration: BitcoinWatchRegistration,
        *,
        endpoint: Literal["address", "utxo", "confirmed_txs", "mempool_txs"],
        suffix: str,
        page_index: int,
    ) -> _EndpointFetch:
        url = self._url(registration, suffix)
        request = {
            "method": "GET",
            "url": url,
            "endpoint": endpoint,
            "page_index": page_index,
            "timeout_seconds": self.timeout_seconds,
        }
        try:
            response = self.transport.get(
                url,
                timeout=self.timeout_seconds,
                headers={"Accept": "application/json", "User-Agent": "MarketLeak-Bitcoin-Context/1"},
            )
        except Exception:
            return _EndpointFetch(endpoint=endpoint, page_index=page_index, capture=None, payload=None, error_code="request_failed")
        received_at = utc_datetime(self.clock(), "received_at")
        # Raw bytes and their receipt are persisted before response status or
        # JSON is inspected.  This preserves malformed and error responses.
        capture = self.raw_store.capture(
            response.body,
            platform="bitcoin",
            source=f"mempool-address-{endpoint}",
            request=request,
            received_at=received_at,
            response_metadata={
                "status_code": response.status_code,
                "url": response.url or url,
                "headers": dict(response.headers),
            },
        )
        if not 200 <= response.status_code < 300:
            return _EndpointFetch(endpoint=endpoint, page_index=page_index, capture=capture, payload=None, error_code="http_status")
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return _EndpointFetch(endpoint=endpoint, page_index=page_index, capture=capture, payload=None, error_code="invalid_json")
        return _EndpointFetch(endpoint=endpoint, page_index=page_index, capture=capture, payload=payload)

    def _fact_lineage(self, registration: BitcoinWatchRegistration, capture: RawCapture, *, as_of: datetime, observed_at: datetime) -> dict[str, Any]:
        return {
            "registration_uid": registration.registration_uid,
            "address": registration.address,
            "observed_at": observed_at,
            "retrieved_at": capture.received_at,
            "as_of": as_of,
            "source_uid": MEMPOOL_SOURCE_UID,
            "raw_artifact_uid": _raw_uid(capture),
            "parser_version": self.parser_version,
            "raw_content_sha256": capture.sha256,
        }

    @staticmethod
    def _stats(value: Any, field_name: str) -> BitcoinAddressStats:
        if not isinstance(value, Mapping):
            raise ValueError(f"{field_name} must be an object")
        return BitcoinAddressStats(
            tx_count=_nonnegative_int(value.get("tx_count"), f"{field_name}.tx_count"),
            funded_txo_count=_nonnegative_int(value.get("funded_txo_count"), f"{field_name}.funded_txo_count"),
            funded_txo_sum_sats=_nonnegative_int(value.get("funded_txo_sum"), f"{field_name}.funded_txo_sum"),
            spent_txo_count=_nonnegative_int(value.get("spent_txo_count"), f"{field_name}.spent_txo_count"),
            spent_txo_sum_sats=_nonnegative_int(value.get("spent_txo_sum"), f"{field_name}.spent_txo_sum"),
        )

    def _summary_fact(self, registration: BitcoinWatchRegistration, fetch: _EndpointFetch, *, as_of: datetime) -> BitcoinAddressSummaryFact:
        if fetch.capture is None or not isinstance(fetch.payload, Mapping):
            raise ValueError("address summary payload is unavailable")
        returned_address = validate_bitcoin_mainnet_address(fetch.payload.get("address"))
        if returned_address != registration.address:
            raise ValueError("address summary does not match its registration")
        lineage = self._fact_lineage(registration, fetch.capture, as_of=as_of, observed_at=fetch.capture.received_at)
        return BitcoinAddressSummaryFact(
            fact_uid=stable_uid("bitcoin", "address-summary", (registration.registration_uid, fetch.capture.sha256)),
            **lineage,
            chain_stats=self._stats(fetch.payload.get("chain_stats"), "chain_stats"),
            mempool_stats=self._stats(fetch.payload.get("mempool_stats"), "mempool_stats"),
        )

    @staticmethod
    def _state_and_observed(status: Any, *, retrieved_at: datetime, expected_state: Literal["confirmed", "mempool"] | None = None) -> tuple[Literal["confirmed", "mempool"], datetime, int | None, datetime | None]:
        if not isinstance(status, Mapping) or not isinstance(status.get("confirmed"), bool):
            raise ValueError("transaction status.confirmed must be a boolean")
        state: Literal["confirmed", "mempool"] = "confirmed" if status["confirmed"] else "mempool"
        if expected_state is not None and state != expected_state:
            raise ValueError("transaction state does not match the endpoint")
        if state == "mempool":
            return state, retrieved_at, None, None
        block_height = _nonnegative_int(status.get("block_height"), "status.block_height")
        block_time = utc_datetime(status.get("block_time"), "status.block_time")
        if block_time > retrieved_at:
            raise ValueError("confirmed block_time cannot be after local retrieval")
        return state, block_time, block_height, block_time

    def _utxo_fact(self, registration: BitcoinWatchRegistration, item: Any, capture: RawCapture, *, as_of: datetime) -> BitcoinUtxoFact:
        if not isinstance(item, Mapping):
            raise ValueError("UTXO item must be an object")
        state, observed_at, block_height, block_time = self._state_and_observed(
            item.get("status"), retrieved_at=capture.received_at
        )
        txid = _txid(item.get("txid"))
        vout = _nonnegative_int(item.get("vout"), "vout")
        lineage = self._fact_lineage(registration, capture, as_of=as_of, observed_at=observed_at)
        return BitcoinUtxoFact(
            fact_uid=stable_uid("bitcoin", "utxo", (registration.registration_uid, txid, vout, capture.sha256)),
            **lineage,
            txid=txid,
            vout=vout,
            value_sats=_nonnegative_int(item.get("value"), "value"),
            state=state,
            block_height=block_height,
            block_time=block_time,
        )

    def _timeline_fact(
        self,
        registration: BitcoinWatchRegistration,
        item: Any,
        capture: RawCapture,
        *,
        as_of: datetime,
        expected_state: Literal["confirmed", "mempool"],
    ) -> BitcoinTransactionTimelineFact:
        if not isinstance(item, Mapping):
            raise ValueError("transaction item must be an object")
        state, observed_at, block_height, block_time = self._state_and_observed(
            item.get("status"), retrieved_at=capture.received_at, expected_state=expected_state
        )
        txid = _txid(item.get("txid"))
        lineage = self._fact_lineage(registration, capture, as_of=as_of, observed_at=observed_at)
        return BitcoinTransactionTimelineFact(
            fact_uid=stable_uid("bitcoin", "timeline", (registration.registration_uid, txid, state, capture.sha256)),
            **lineage,
            txid=txid,
            state=state,
            version=item.get("version") if isinstance(item.get("version"), int) and not isinstance(item.get("version"), bool) else (_ for _ in ()).throw(ValueError("version must be an integer")),
            locktime=_nonnegative_int(item.get("locktime"), "locktime"),
            size_bytes=_nonnegative_int(item.get("size"), "size"),
            weight_units=_nonnegative_int(item.get("weight"), "weight"),
            fee_sats=_nonnegative_int(item.get("fee"), "fee"),
            block_height=block_height,
            block_time=block_time,
        )

    def _coverage(
        self,
        registration: BitcoinWatchRegistration,
        *,
        endpoint: Literal["address", "utxo", "confirmed_txs", "mempool_txs"],
        fetches: Sequence[_EndpointFetch],
        as_of: datetime,
        status: CoverageStatus,
        reasons: Iterable[str] = (),
    ) -> BitcoinSourceCoverage:
        captures = tuple(fetch.capture for fetch in fetches if fetch.capture is not None)
        observed_at = min((capture.received_at for capture in captures), default=as_of)
        retrieved_at = max((capture.received_at for capture in captures), default=as_of)
        final_reasons = tuple(dict.fromkeys((*reasons, *(fetch.error_code for fetch in fetches if fetch.error_code))))
        return BitcoinSourceCoverage(
            coverage_uid=stable_uid("bitcoin", "coverage", (registration.registration_uid, endpoint, tuple(capture.sha256 for capture in captures), as_of)),
            registration_uid=registration.registration_uid,
            address=registration.address,
            endpoint=endpoint,
            status=status,
            observed_at=observed_at,
            retrieved_at=retrieved_at,
            as_of=as_of,
            source_uid=MEMPOOL_SOURCE_UID,
            parser_version=self.parser_version,
            raw_artifact_uids=tuple(_raw_uid(capture) for capture in captures),
            raw_content_sha256=tuple(capture.sha256 for capture in captures),
            requested_max_pages=self.max_confirmed_pages if endpoint == "confirmed_txs" else 1,
            pages_fetched=len(captures),
            reason_codes=tuple(reason for reason in final_reasons if reason),
        )

    def collect(self, registrations: Iterable[BitcoinWatchRegistration]) -> BitcoinContextSnapshot:
        items = tuple(registrations)
        if not items:
            raise BitcoinContextError("at least one explicit BitcoinWatchRegistration is required")
        if any(not isinstance(item, BitcoinWatchRegistration) for item in items):
            raise TypeError("collector accepts only BitcoinWatchRegistration objects")
        if len({item.registration_uid for item in items}) != len(items):
            raise BitcoinContextError("registration_uid values must be unique")

        summary_fetches: list[tuple[BitcoinWatchRegistration, _EndpointFetch]] = []
        utxo_fetches: list[tuple[BitcoinWatchRegistration, _EndpointFetch]] = []
        confirmed_fetches: list[tuple[BitcoinWatchRegistration, list[_EndpointFetch]]] = []
        mempool_fetches: list[tuple[BitcoinWatchRegistration, _EndpointFetch]] = []
        all_captures: list[RawCapture] = []

        for registration in items:
            summary = self._fetch(registration, endpoint="address", suffix="", page_index=0)
            utxo = self._fetch(registration, endpoint="utxo", suffix="/utxo", page_index=0)
            mempool = self._fetch(registration, endpoint="mempool_txs", suffix="/txs/mempool", page_index=0)
            pages: list[_EndpointFetch] = []
            cursor: str | None = None
            for page_index in range(self.max_confirmed_pages):
                suffix = "/txs/chain" if cursor is None else f"/txs/chain/{cursor}"
                page = self._fetch(registration, endpoint="confirmed_txs", suffix=suffix, page_index=page_index)
                pages.append(page)
                if not isinstance(page.payload, list) or len(page.payload) < CONFIRMED_PAGE_SIZE:
                    break
                try:
                    cursor = _txid(page.payload[-1].get("txid") if isinstance(page.payload[-1], Mapping) else None, "last_seen_txid")
                except ValueError:
                    break
            summary_fetches.append((registration, summary))
            utxo_fetches.append((registration, utxo))
            mempool_fetches.append((registration, mempool))
            confirmed_fetches.append((registration, pages))
            all_captures.extend(fetch.capture for fetch in (summary, utxo, mempool, *pages) if fetch.capture is not None)

        as_of = max((capture.received_at for capture in all_captures), default=utc_datetime(self.clock(), "as_of"))
        summaries: list[BitcoinAddressSummaryFact] = []
        utxos: list[BitcoinUtxoFact] = []
        timeline: list[BitcoinTransactionTimelineFact] = []
        coverage: list[BitcoinSourceCoverage] = []

        for registration, fetch in summary_fetches:
            reasons: list[str] = []
            try:
                summaries.append(self._summary_fact(registration, fetch, as_of=as_of))
                status = CoverageStatus.COMPLETE
            except (TypeError, ValueError):
                status = CoverageStatus.UNAVAILABLE
                reasons.append("summary_not_admissible")
            coverage.append(self._coverage(registration, endpoint="address", fetches=(fetch,), as_of=as_of, status=status, reasons=reasons))

        for registration, fetch in utxo_fetches:
            reasons: list[str] = []
            if not isinstance(fetch.payload, list):
                status = CoverageStatus.UNAVAILABLE
                reasons.append("utxo_payload_not_list")
            else:
                accepted = fetch.payload[:self.max_items_per_response]
                if len(fetch.payload) > self.max_items_per_response:
                    reasons.append("response_item_bound_reached")
                for item in accepted:
                    try:
                        assert fetch.capture is not None
                        utxos.append(self._utxo_fact(registration, item, fetch.capture, as_of=as_of))
                    except (AssertionError, TypeError, ValueError):
                        reasons.append("utxo_item_not_admissible")
                status = CoverageStatus.PARTIAL if reasons else CoverageStatus.COMPLETE
            coverage.append(self._coverage(registration, endpoint="utxo", fetches=(fetch,), as_of=as_of, status=status, reasons=reasons))

        for registration, fetches in confirmed_fetches:
            reasons: list[str] = []
            count_before = len(timeline)
            for fetch in fetches:
                if not isinstance(fetch.payload, list):
                    reasons.append("confirmed_page_payload_not_list")
                    continue
                remaining = self.max_timeline_entries - (len(timeline) - count_before)
                if remaining <= 0:
                    reasons.append("timeline_entry_bound_reached")
                    break
                page_items = fetch.payload[:min(self.max_items_per_response, remaining)]
                if len(fetch.payload) > len(page_items):
                    reasons.append("timeline_entry_bound_reached")
                for item in page_items:
                    try:
                        assert fetch.capture is not None
                        timeline.append(self._timeline_fact(registration, item, fetch.capture, as_of=as_of, expected_state="confirmed"))
                    except (AssertionError, TypeError, ValueError):
                        reasons.append("confirmed_timeline_item_not_admissible")
            page_limit_reached = len(fetches) == self.max_confirmed_pages and isinstance(fetches[-1].payload, list) and len(fetches[-1].payload) >= CONFIRMED_PAGE_SIZE
            if page_limit_reached:
                reasons.append("confirmed_page_bound_reached")
            status = CoverageStatus.PARTIAL if reasons else CoverageStatus.COMPLETE
            coverage.append(self._coverage(registration, endpoint="confirmed_txs", fetches=fetches, as_of=as_of, status=status, reasons=reasons))

        for registration, fetch in mempool_fetches:
            reasons: list[str] = []
            count_before = len(timeline)
            if not isinstance(fetch.payload, list):
                status = CoverageStatus.UNAVAILABLE
                reasons.append("mempool_payload_not_list")
            else:
                remaining = self.max_timeline_entries - (len(timeline) - count_before)
                items_for_response = fetch.payload[:min(self.max_items_per_response, max(remaining, 0))]
                if len(fetch.payload) > len(items_for_response):
                    reasons.append("timeline_entry_bound_reached")
                for item in items_for_response:
                    try:
                        assert fetch.capture is not None
                        timeline.append(self._timeline_fact(registration, item, fetch.capture, as_of=as_of, expected_state="mempool"))
                    except (AssertionError, TypeError, ValueError):
                        reasons.append("mempool_timeline_item_not_admissible")
                status = CoverageStatus.PARTIAL if reasons else CoverageStatus.COMPLETE
            coverage.append(self._coverage(registration, endpoint="mempool_txs", fetches=(fetch,), as_of=as_of, status=status, reasons=reasons))

        raw_artifacts = tuple(
            RawArtifactStore.to_domain(capture, source_uid=MEMPOOL_SOURCE_UID, parser_version=self.parser_version)
            for capture in all_captures
        )
        payload = {
            "as_of": as_of,
            "registrations": [item.model_dump(mode="json") for item in items],
            "summary_fact_uids": [item.fact_uid for item in summaries],
            "utxo_fact_uids": [item.fact_uid for item in utxos],
            "timeline_fact_uids": [item.fact_uid for item in timeline],
            "coverage_uids": [item.coverage_uid for item in coverage],
            "raw_sha256": [item.content_hash for item in raw_artifacts],
        }
        return BitcoinContextSnapshot(
            snapshot_uid=f"bitcoin:context-snapshot/{hashlib.sha256(canonical_json_bytes(payload)).hexdigest()}",
            as_of=as_of,
            registrations=items,
            summaries=tuple(summaries),
            utxos=tuple(utxos),
            timeline=tuple(timeline),
            coverage=tuple(coverage),
            raw_artifacts=raw_artifacts,
        )


def load_bitcoin_watchlist(path: str | Path) -> tuple[BitcoinWatchRegistration, ...]:
    """Read only an explicit local JSON registration list."""

    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError("Bitcoin watchlist file was not found")
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping) or set(payload) != {"registrations"} or not isinstance(payload["registrations"], list):
        raise BitcoinContextError("watchlist must contain only a registrations array")
    registrations = tuple(
        BitcoinWatchRegistration.model_validate_json(json.dumps(item))
        for item in payload["registrations"]
    )
    if not registrations:
        raise BitcoinContextError("watchlist registrations cannot be empty")
    return registrations


def collect_bitcoin_context_once(
    *,
    watchlist_path: str | Path,
    output_dir: str | Path,
    max_confirmed_pages: int = 2,
    max_items_per_response: int = 1_000,
    max_timeline_entries: int = 500,
    timeout_seconds: float = 15.0,
    transport: MempoolHttpTransport | None = None,
    clock: Callable[[], datetime] | None = None,
) -> BitcoinContextSnapshot:
    """Collect a bounded snapshot and write its raw data and JSON locally."""

    registrations = load_bitcoin_watchlist(watchlist_path)
    root = Path(output_dir)
    raw_store = RawArtifactStore(root / "raw")
    collector = BitcoinAddressContextCollector(
        raw_store=raw_store,
        transport=transport or UrllibMempoolTransport(),
        clock=clock,
        max_confirmed_pages=max_confirmed_pages,
        max_items_per_response=max_items_per_response,
        max_timeline_entries=max_timeline_entries,
        timeout_seconds=timeout_seconds,
    )
    snapshot = collector.collect(registrations)
    root.mkdir(parents=True, exist_ok=True)
    destination = root / "bitcoin-context-snapshot.json"
    destination.write_bytes(canonical_json_bytes(snapshot.model_dump(mode="json")))
    return snapshot


__all__ = [
    "BITCOIN_CONTEXT_PARSER_VERSION",
    "MEMPOOL_API_BASE_URL",
    "MEMPOOL_SOURCE_UID",
    "BitcoinAddressContextCollector",
    "BitcoinAddressFormatError",
    "BitcoinAddressSummaryFact",
    "BitcoinAddressStats",
    "BitcoinContextError",
    "BitcoinContextSnapshot",
    "BitcoinSourceCoverage",
    "BitcoinTransactionTimelineFact",
    "BitcoinUtxoFact",
    "BitcoinWatchRegistration",
    "MempoolHttpResponse",
    "MempoolHttpTransport",
    "UrllibMempoolTransport",
    "collect_bitcoin_context_once",
    "load_bitcoin_watchlist",
    "validate_bitcoin_mainnet_address",
]
