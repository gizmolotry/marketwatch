"""Fail-closed, published-context policy for observed Bitcoin addresses.

An address observation is isolated context.  It is not an identity, owner,
transaction intent, or market signal.  Attachment to an observed Polygon,
market, or event identifier is possible only through an explicitly approved,
pre-decision external-link record with immutable raw-evidence lineage.

The configuration repository is deliberately read-only: it reads a published
JSON snapshot only, performs no network calls, and never accepts an address
lookup as input.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
import json
from pathlib import Path
from typing import Mapping

from pydantic import field_validator, model_validator

from .schemas import Phase15Model, Provenance, canonical_hash


def _utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _sha256(value: str, *, field_name: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return normalized


class LinkApprovalStatus(str, Enum):
    APPROVED = "approved"
    UNAPPROVED = "unapproved"
    REJECTED = "rejected"


class BitcoinContextStatus(str, Enum):
    WATCHLIST_NOT_CONFIGURED = "watchlist_not_configured"
    UNAVAILABLE = "unavailable"
    AVAILABLE_PRECOMPUTED_CONTEXT = "available_precomputed_context"


class BitcoinAddressObservation(Phase15Model):
    """A redacted address UID and immutable observation lineage only."""

    address_uid: str
    observed_at: datetime
    first_seen_at: datetime
    retrieved_at: datetime
    ingested_at: datetime
    provenance: Provenance

    @field_validator("observed_at", "first_seen_at", "retrieved_at", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_observation(self) -> "BitcoinAddressObservation":
        if not self.address_uid.startswith("btcaddr:") or len(self.address_uid) <= len("btcaddr:"):
            raise ValueError("address_uid must use the redacted btcaddr: stable-UID namespace")
        if self.observed_at > self.first_seen_at:
            raise ValueError("address observation cannot be after first_seen_at")
        if self.first_seen_at > self.retrieved_at or self.retrieved_at > self.ingested_at:
            raise ValueError("address observation timing is not causal")
        if self.provenance.retrieved_at != self.retrieved_at:
            raise ValueError("address provenance.retrieved_at must equal retrieved_at")
        return self


class ExternalMarketLink(Phase15Model):
    """An external evidence record, not a statement about a person or owner."""

    link_uid: str
    bitcoin_address_uid: str
    polygon_address_uid: str | None = None
    market_address_uid: str | None = None
    event_uid: str | None = None
    approval_status: LinkApprovalStatus
    observed_at: datetime
    first_seen_at: datetime
    approved_at: datetime | None = None
    decision_as_of: datetime
    evidence: Provenance

    @field_validator("observed_at", "first_seen_at", "approved_at", "decision_as_of")
    @classmethod
    def require_utc_optional(cls, value: datetime | None, info) -> datetime | None:
        return None if value is None else _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_external_link(self) -> "ExternalMarketLink":
        if not str(self.link_uid).strip() or ":" not in self.link_uid:
            raise ValueError("link_uid must be a stable UID")
        if not self.bitcoin_address_uid.startswith("btcaddr:") or len(self.bitcoin_address_uid) <= len("btcaddr:"):
            raise ValueError("bitcoin_address_uid must use the redacted btcaddr: stable-UID namespace")
        if not any((self.polygon_address_uid, self.market_address_uid, self.event_uid)):
            raise ValueError("external link must reference an observed Polygon, market, or event identifier")
        if self.observed_at > self.first_seen_at or self.first_seen_at > self.evidence.retrieved_at:
            raise ValueError("external link timing is not causal")
        if self.evidence.retrieved_at > self.decision_as_of:
            raise ValueError("external link raw evidence is after the decision cutoff")
        if self.observed_at > self.decision_as_of or self.first_seen_at > self.decision_as_of:
            raise ValueError("external link was not observed before the decision cutoff")
        if self.approval_status == LinkApprovalStatus.APPROVED:
            if self.approved_at is None:
                raise ValueError("approved external links require approved_at")
            if self.evidence.retrieved_at > self.approved_at:
                raise ValueError("external link cannot be approved before its raw evidence is retrieved")
            if self.approved_at > self.decision_as_of:
                raise ValueError("external link approval is after the decision cutoff")
        return self

    @property
    def is_approved_pre_decision(self) -> bool:
        return self.approval_status == LinkApprovalStatus.APPROVED and self.approved_at is not None


class PublishedBitcoinContextSnapshot(Phase15Model):
    """A precomputed snapshot whose linkage policy can be checked locally."""

    snapshot_uid: str
    decision_as_of: datetime
    published_at: datetime
    address_observations: tuple[BitcoinAddressObservation, ...]
    observed_polygon_address_uids: tuple[str, ...] = ()
    observed_market_address_uids: tuple[str, ...] = ()
    observed_event_uids: tuple[str, ...] = ()
    external_links: tuple[ExternalMarketLink, ...] = ()

    @field_validator("decision_as_of", "published_at")
    @classmethod
    def require_utc(cls, value: datetime, info) -> datetime:
        return _utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_snapshot(self) -> "PublishedBitcoinContextSnapshot":
        if not str(self.snapshot_uid).strip() or ":" not in self.snapshot_uid:
            raise ValueError("snapshot_uid must be a stable UID")
        if self.published_at < self.decision_as_of:
            raise ValueError("published snapshot cannot predate its decision cutoff")
        address_uids = tuple(item.address_uid for item in self.address_observations)
        if len(set(address_uids)) != len(address_uids):
            raise ValueError("address observations must have unique redacted UIDs")
        for field_name, values in (
            ("observed_polygon_address_uids", self.observed_polygon_address_uids),
            ("observed_market_address_uids", self.observed_market_address_uids),
            ("observed_event_uids", self.observed_event_uids),
        ):
            if len(set(values)) != len(values):
                raise ValueError(f"{field_name} must be unique")
        if len({item.link_uid for item in self.external_links}) != len(self.external_links):
            raise ValueError("external link UIDs must be unique")
        if any(item.bitcoin_address_uid not in set(address_uids) for item in self.external_links):
            raise ValueError("external links must reference an observed redacted Bitcoin address UID")
        return self

    def attachable_links(self) -> tuple[ExternalMarketLink, ...]:
        """Return only approved, observed-target links available by this cutoff."""

        polygon = set(self.observed_polygon_address_uids)
        market = set(self.observed_market_address_uids)
        events = set(self.observed_event_uids)
        attached: list[ExternalMarketLink] = []
        for link in self.external_links:
            target_present = any(
                (
                    link.polygon_address_uid is not None and link.polygon_address_uid in polygon,
                    link.market_address_uid is not None and link.market_address_uid in market,
                    link.event_uid is not None and link.event_uid in events,
                )
            )
            if not target_present or not link.is_approved_pre_decision:
                continue
            if any(
                timestamp > self.decision_as_of
                for timestamp in (link.observed_at, link.first_seen_at, link.evidence.retrieved_at, link.approved_at)
                if timestamp is not None
            ):
                continue
            attached.append(link)
        return tuple(sorted(attached, key=lambda item: item.link_uid))

    def safe_payload(self) -> dict[str, object]:
        """Expose redacted, neutral context with no raw addresses or evidence bodies."""

        attached_by_address: dict[str, list[ExternalMarketLink]] = {}
        for link in self.attachable_links():
            attached_by_address.setdefault(link.bitcoin_address_uid, []).append(link)
        contexts: list[dict[str, object]] = []
        for observed in sorted(self.address_observations, key=lambda item: item.address_uid):
            links = attached_by_address.get(observed.address_uid, [])
            contexts.append(
                {
                    "address_uid": observed.address_uid,
                    "attachment_status": "attached_precomputed_external_context" if links else "isolated_context",
                    "polygon_address_uids": sorted(
                        {link.polygon_address_uid for link in links if link.polygon_address_uid is not None}
                    ),
                    "market_address_uids": sorted(
                        {link.market_address_uid for link in links if link.market_address_uid is not None}
                    ),
                    "event_uids": sorted({link.event_uid for link in links if link.event_uid is not None}),
                }
            )
        return {
            "snapshot_uid": self.snapshot_uid,
            "decision_as_of": self.decision_as_of.isoformat().replace("+00:00", "Z"),
            "published_at": self.published_at.isoformat().replace("+00:00", "Z"),
            "contexts": contexts,
        }


@dataclass(frozen=True)
class BitcoinContextLoadState:
    status: BitcoinContextStatus
    reason: str
    snapshot: PublishedBitcoinContextSnapshot | None = None


class PublishedBitcoinContextRepository:
    """Read one configured, hash-checked snapshot without external retrieval."""

    def __init__(self, configuration_path: str | Path | None) -> None:
        self.configuration_path = None if configuration_path is None else Path(configuration_path)

    def load(self) -> BitcoinContextLoadState:
        if self.configuration_path is None:
            return BitcoinContextLoadState(
                BitcoinContextStatus.WATCHLIST_NOT_CONFIGURED,
                "no published Bitcoin-context configuration is configured",
            )
        try:
            document = json.loads(self.configuration_path.read_text(encoding="utf-8"))
            if not isinstance(document, Mapping) or set(document) != {"snapshot", "snapshot_sha256"}:
                raise ValueError("configuration must contain only snapshot and snapshot_sha256")
            snapshot_payload = document["snapshot"]
            if not isinstance(snapshot_payload, Mapping):
                raise ValueError("snapshot must be a JSON object")
            expected = _sha256(str(document["snapshot_sha256"]), field_name="snapshot_sha256")
            if canonical_hash(snapshot_payload) != expected:
                raise ValueError("published snapshot hash does not match content")
            snapshot = PublishedBitcoinContextSnapshot.model_validate_json(json.dumps(snapshot_payload))
            return BitcoinContextLoadState(
                BitcoinContextStatus.AVAILABLE_PRECOMPUTED_CONTEXT,
                "hash-checked precomputed published context is available",
                snapshot,
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return BitcoinContextLoadState(
                BitcoinContextStatus.UNAVAILABLE,
                "published Bitcoin-context configuration is unavailable or failed provenance validation",
            )

    def payload(self, *, as_of: datetime) -> dict[str, object]:
        """Return only redacted context that was published by the server cutoff."""

        cutoff = _utc(as_of, field_name="as_of")
        state = self.load()
        if state.snapshot is None:
            return {
                "status": state.status.value,
                "reason": state.reason,
                "read_only": True,
                "live_fetch": False,
                "contexts": [],
            }
        if state.snapshot.published_at > cutoff or state.snapshot.decision_as_of > cutoff:
            return {
                "status": BitcoinContextStatus.UNAVAILABLE.value,
                "reason": "published context is not available at the requested cutoff",
                "read_only": True,
                "live_fetch": False,
                "contexts": [],
            }
        return {
            "status": BitcoinContextStatus.AVAILABLE_PRECOMPUTED_CONTEXT.value,
            "reason": state.reason,
            "read_only": True,
            "live_fetch": False,
            **state.snapshot.safe_payload(),
        }


def snapshot_config_document(snapshot: PublishedBitcoinContextSnapshot) -> dict[str, object]:
    """Create the immutable configuration envelope used by the read-only repository."""

    payload = snapshot.model_dump(mode="json")
    return {"snapshot": payload, "snapshot_sha256": canonical_hash(payload)}


__all__ = [
    "BitcoinAddressObservation",
    "BitcoinContextLoadState",
    "BitcoinContextStatus",
    "ExternalMarketLink",
    "LinkApprovalStatus",
    "PublishedBitcoinContextRepository",
    "PublishedBitcoinContextSnapshot",
    "snapshot_config_document",
]
