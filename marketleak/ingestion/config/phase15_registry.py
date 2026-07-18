"""Strict JSON-only allowlist for Phase 15 market and reference sources.

The registry names fixed market identifiers and fixed, documented reference
sources.  It does not contain credentials, discover markets, accept user input,
or choose a generic BTC source.  Runtime collectors must receive an already
validated registry entry rather than a symbol, URL, or address supplied at
request time.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Literal, Mapping
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


REGISTRY_SCHEMA_VERSION = "phase15-source-registry-v1"
_UID = re.compile(r"^[a-z][a-z0-9_-]*:[A-Za-z0-9._/-]+$")
_KALSHI_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9_.-]*$")
_POLYMARKET_ASSET_ID = re.compile(r"^[0-9]{8,100}$")
_FORBIDDEN_TEXT = (
    "<",
    ">",
    "example",
    "placeholder",
    "replace",
    "changeme",
    "generic",
    "default",
    "fallback",
    "wallet",
    "secret",
    "password",
    "api_key",
    "apikey",
    "bearer ",
)
_NONRUNNABLE_HOSTS = (".example", ".invalid", ".test", "localhost")


def _forbid_placeholder(value: str, *, field_name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    lowered = normalized.lower()
    if any(item in lowered for item in _FORBIDDEN_TEXT):
        raise ValueError(f"{field_name} contains a placeholder, generic fallback, or prohibited secret/wallet text")
    return normalized


def _uid(value: str, *, field_name: str) -> str:
    normalized = _forbid_placeholder(value, field_name=field_name)
    if not _UID.fullmatch(normalized):
        raise ValueError(f"{field_name} must be an explicit stable UID")
    return normalized


def _https_url(value: str, *, field_name: str) -> str:
    normalized = _forbid_placeholder(value, field_name=field_name)
    parsed = urlsplit(normalized)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError(f"{field_name} must be an explicit credential-free HTTPS URL")
    if parsed.query or parsed.fragment or any(parsed.hostname == host[1:] or parsed.hostname.endswith(host) for host in _NONRUNNABLE_HOSTS):
        raise ValueError(f"{field_name} must not be a query-bearing, non-runnable, or arbitrary placeholder URL")
    return normalized


class RegistryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, str_strip_whitespace=True)


class BoundedStreamSettings(RegistryModel):
    """Per-target finite collection budget; unbounded streams are forbidden."""

    max_messages: int = Field(ge=1, le=10_000)
    duration_seconds: float = Field(gt=0, le=3_600)
    max_reconnects: int = Field(ge=0, le=10)


class ApprovedMarketTarget(RegistryModel):
    """A fixed venue target with exactly one venue-specific identifier family."""

    target_uid: str
    venue: Literal["polymarket", "kalshi"]
    market_uid: str
    polymarket_asset_ids: tuple[str, ...] | None = None
    kalshi_tickers: tuple[str, ...] | None = None
    metadata_ref: str
    stream_config_ref: str
    stream_settings: BoundedStreamSettings
    reference_mapping_uid: str | None = None
    case_context_config_ref: str | None = None

    @field_validator(
        "target_uid",
        "market_uid",
        "metadata_ref",
        "stream_config_ref",
        "reference_mapping_uid",
        "case_context_config_ref",
    )
    @classmethod
    def require_uid(cls, value: str | None, info) -> str | None:
        return None if value is None else _uid(value, field_name=info.field_name)

    @field_validator("polymarket_asset_ids")
    @classmethod
    def validate_polymarket_assets(cls, value: tuple[str, ...] | None) -> tuple[str, ...] | None:
        if value is None:
            return None
        if len(value) != 2 or len(set(value)) != 2:
            raise ValueError("polymarket_asset_ids must contain exactly two distinct explicit token IDs")
        if any(not _POLYMARKET_ASSET_ID.fullmatch(item) for item in value):
            raise ValueError("polymarket_asset_ids must be explicit numeric CLOB token IDs")
        return value

    @field_validator("kalshi_tickers")
    @classmethod
    def validate_kalshi_tickers(cls, value: tuple[str, ...] | None) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not value or len(value) > 100 or len(set(value)) != len(value):
            raise ValueError("kalshi_tickers must contain 1-100 distinct configured tickers")
        if any(not _KALSHI_TICKER.fullmatch(item) or "EXAMPLE" in item for item in value):
            raise ValueError("kalshi_tickers must be explicit uppercase venue tickers")
        return value

    @model_validator(mode="after")
    def require_venue_specific_target(self) -> "ApprovedMarketTarget":
        if self.venue == "polymarket":
            if self.polymarket_asset_ids is None or self.kalshi_tickers is not None:
                raise ValueError("polymarket target requires asset IDs and must not contain Kalshi tickers")
        elif self.kalshi_tickers is None or self.polymarket_asset_ids is not None:
            raise ValueError("kalshi target requires tickers and must not contain Polymarket asset IDs")
        return self

    @property
    def canonical_market_uid(self) -> str:
        """Return the durable identity used by cross-modal case evidence.

        The public Polymarket market-channel collector predates the normalized
        case identity and still subscribes with ``polymarket:<condition>``.
        Reference evidence, however, must join the review-case builder's
        durable ``polymarket:market/<condition>`` identity.  Preserve the
        configured subscription ID while exposing that canonical join key.
        """

        if self.venue != "polymarket":
            return self.market_uid
        prefix = "polymarket:"
        legacy = self.market_uid.removeprefix(prefix)
        return self.market_uid if legacy.startswith("market/") else f"{prefix}market/{legacy}"


class DocumentedBtcReferenceMapping(RegistryModel):
    """One documented market-to-primary-source BTC/USD mapping, with no fallback."""

    mapping_uid: str
    target_uid: str
    market_rule_source_uid: str
    market_rule_source_url: str
    settlement_source_uid: str
    settlement_source_url: str
    primary_source_uid: str
    primary_source_url: str
    instrument: Literal["BTC-USD", "BTC-USDT"]
    observation_kind: Literal["ticker_last", "candle_high"] = "ticker_last"
    candle_interval: Literal["1m"] | None = None
    requires_closed_candle: bool = False
    configured_endpoint_ref: str

    @field_validator(
        "mapping_uid",
        "target_uid",
        "market_rule_source_uid",
        "settlement_source_uid",
        "primary_source_uid",
        "configured_endpoint_ref",
    )
    @classmethod
    def require_uid(cls, value: str, info) -> str:
        return _uid(value, field_name=info.field_name)

    @field_validator("market_rule_source_url", "settlement_source_url", "primary_source_url")
    @classmethod
    def require_https_url(cls, value: str, info) -> str:
        return _https_url(value, field_name=info.field_name)

    @model_validator(mode="after")
    def require_documented_observation_contract(self) -> "DocumentedBtcReferenceMapping":
        """Keep legacy Coinbase mappings valid while making candle rules exact."""

        if self.observation_kind == "ticker_last":
            if (
                self.instrument != "BTC-USD"
                or self.candle_interval is not None
                or self.requires_closed_candle
            ):
                raise ValueError("ticker_last mappings require BTC-USD and no candle semantics")
            return self
        if (
            self.instrument != "BTC-USDT"
            or self.candle_interval != "1m"
            or not self.requires_closed_candle
        ):
            raise ValueError(
                "candle_high mappings require BTC-USDT, a 1m interval, and a closed-candle requirement"
            )
        return self


class Phase15SourceRegistry(RegistryModel):
    """Whole-registry integrity checks for static, server-side configuration."""

    registry_uid: str
    schema_version: Literal[REGISTRY_SCHEMA_VERSION]
    targets: tuple[ApprovedMarketTarget, ...]
    documented_btc_reference_mappings: tuple[DocumentedBtcReferenceMapping, ...] = ()

    @field_validator("registry_uid")
    @classmethod
    def require_registry_uid(cls, value: str) -> str:
        return _uid(value, field_name="registry_uid")

    @model_validator(mode="after")
    def require_one_to_one_configuration(self) -> "Phase15SourceRegistry":
        if not self.targets:
            raise ValueError("registry requires at least one explicitly approved market target")
        target_uids = tuple(target.target_uid for target in self.targets)
        if len(set(target_uids)) != len(target_uids):
            raise ValueError("approved target_uids must be unique")
        market_identities = tuple((target.venue, target.market_uid) for target in self.targets)
        if len(set(market_identities)) != len(market_identities):
            raise ValueError("each venue/market_uid may appear only once")
        mapping_uids = tuple(mapping.mapping_uid for mapping in self.documented_btc_reference_mappings)
        mapping_targets = tuple(mapping.target_uid for mapping in self.documented_btc_reference_mappings)
        if len(set(mapping_uids)) != len(mapping_uids) or len(set(mapping_targets)) != len(mapping_targets):
            raise ValueError("documented BTC mappings must be one-to-one by mapping_uid and target_uid")
        known_targets = set(target_uids)
        if any(target_uid not in known_targets for target_uid in mapping_targets):
            raise ValueError("documented BTC mapping names an unknown approved target")
        by_target = {mapping.target_uid: mapping.mapping_uid for mapping in self.documented_btc_reference_mappings}
        for target in self.targets:
            linked_mapping = by_target.get(target.target_uid)
            if target.reference_mapping_uid != linked_mapping:
                raise ValueError("target reference_mapping_uid must exactly match its one documented BTC mapping, if any")
        return self

    def target(self, target_uid: str) -> ApprovedMarketTarget:
        """Return an approved target only by its fixed registry UID."""

        stable_uid = _uid(target_uid, field_name="target_uid")
        for target in self.targets:
            if target.target_uid == stable_uid:
                return target
        raise KeyError("target UID is not approved by this registry")

    def reference_mapping(self, mapping_uid: str) -> DocumentedBtcReferenceMapping:
        """Return a documented BTC mapping only by its fixed registry UID."""

        stable_uid = _uid(mapping_uid, field_name="mapping_uid")
        for mapping in self.documented_btc_reference_mappings:
            if mapping.mapping_uid == stable_uid:
                return mapping
        raise KeyError("reference mapping UID is not approved by this registry")


def load_phase15_source_registry(path: str | Path) -> Phase15SourceRegistry:
    """Load one JSON object without interpolation, secret resolution, or network access."""

    source = Path(path)
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Phase15 source registry must be a readable JSON document") from exc
    if not isinstance(raw, Mapping):
        raise ValueError("Phase15 source registry must be a JSON object")
    return Phase15SourceRegistry.model_validate_json(json.dumps(raw))


__all__ = [
    "ApprovedMarketTarget",
    "BoundedStreamSettings",
    "DocumentedBtcReferenceMapping",
    "Phase15SourceRegistry",
    "REGISTRY_SCHEMA_VERSION",
    "load_phase15_source_registry",
]
