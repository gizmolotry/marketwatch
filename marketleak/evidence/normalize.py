"""Deterministic normalization for point-in-time public evidence.

Publication timestamps supplied by a publisher are intentionally distinct from
the time at which MarketLeak first observed a document.  Historical backfills
must not turn a claimed publication timestamp into a fabricated first-seen
timestamp.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


UTC = timezone.utc


class TimestampParseStatus(str, Enum):
    VALID = "VALID"
    MISSING = "MISSING"
    INVALID = "INVALID"


def utc_datetime(value: Any, *, field_name: str, allow_none: bool = False) -> datetime | None:
    """Parse an explicit timestamp and return an aware UTC datetime."""
    if value is None or value == "":
        if allow_none:
            return None
        raise ValueError(f"{field_name} is required")

    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = datetime.fromtimestamp(float(value), tz=UTC)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            if allow_none:
                return None
            raise ValueError(f"{field_name} is required")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(text)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{field_name} is not a supported timestamp") from exc
    else:
        raise ValueError(f"{field_name} is not a supported timestamp")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return parsed.astimezone(UTC)


def _canonical_content(
    *,
    title: str,
    body: str,
    url: str,
    source_document_id: str,
) -> bytes:
    payload = {
        "body": body,
        "source_document_id": source_document_id,
        "title": title,
        "url": url,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


class NormalizedEvidence(BaseModel):
    """One immutable observation of a public document revision."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    evidence_uid: str = Field(min_length=1)
    document_uid: str = Field(min_length=1)
    revision_uid: str = Field(min_length=1)
    source: str = Field(min_length=1)
    source_document_id: str = Field(min_length=1)
    source_revision: str = Field(min_length=1)
    url: str = ""
    title: str = ""
    body: str = ""
    claimed_published_at: datetime | None = None
    first_seen_at: datetime
    retrieved_at: datetime
    modified_at: datetime | None = None
    backfill: bool = False
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_artifact_uid: str | None = None
    parser_version: str = Field(min_length=1)
    publication_timestamp_status: TimestampParseStatus = TimestampParseStatus.MISSING
    metadata: Mapping[str, Any] = Field(default_factory=dict)

    @field_validator(
        "claimed_published_at",
        "first_seen_at",
        "retrieved_at",
        "modified_at",
        mode="before",
    )
    @classmethod
    def _timestamps_are_utc(cls, value: Any, info):
        return utc_datetime(value, field_name=info.field_name, allow_none=info.field_name in {"claimed_published_at", "modified_at"})

    @model_validator(mode="after")
    def _validate_observation_clock(self):
        if self.first_seen_at > self.retrieved_at:
            raise ValueError("first_seen_at cannot be later than retrieved_at")
        return self

    @property
    def searchable_text(self) -> str:
        return " ".join(part for part in (self.title, self.body) if part).strip()


def normalize_evidence(
    *,
    source: str,
    source_document_id: str,
    retrieved_at: Any,
    title: str = "",
    body: str = "",
    url: str = "",
    claimed_published_at: Any = None,
    first_seen_at: Any = None,
    modified_at: Any = None,
    backfill: bool = False,
    content_bytes: bytes | None = None,
    source_revision: str | None = None,
    raw_artifact_uid: str | None = None,
    parser_version: str = "evidence-normalizer-v1",
    metadata: Mapping[str, Any] | None = None,
) -> NormalizedEvidence:
    """Normalize a public document without inferring historical availability."""
    source = str(source).strip()
    source_document_id = str(source_document_id).strip()
    if not source or not source_document_id:
        raise ValueError("source and source_document_id are required")

    retrieved = utc_datetime(retrieved_at, field_name="retrieved_at")
    # A backfill becomes observable when it is retrieved.  The publisher's
    # claimed timestamp remains separate and cannot override this clock.
    first_seen_value = retrieved if backfill or first_seen_at is None else first_seen_at
    first_seen = utc_datetime(
        first_seen_value,
        field_name="first_seen_at",
    )

    publication_status = TimestampParseStatus.MISSING
    claimed = None
    if claimed_published_at not in (None, ""):
        try:
            claimed = utc_datetime(
                claimed_published_at,
                field_name="claimed_published_at",
                allow_none=True,
            )
            publication_status = TimestampParseStatus.VALID
        except ValueError:
            publication_status = TimestampParseStatus.INVALID

    modified = utc_datetime(modified_at, field_name="modified_at", allow_none=True)
    canonical_bytes = (
        content_bytes
        if content_bytes is not None
        else _canonical_content(
            title=str(title),
            body=str(body),
            url=str(url),
            source_document_id=source_document_id,
        )
    )
    content_hash = hashlib.sha256(canonical_bytes).hexdigest()
    document_uid = f"{source}:document:{hashlib.sha256(source_document_id.encode('utf-8')).hexdigest()}"
    revision_value = str(source_revision or content_hash).strip()
    revision_uid = f"{document_uid}:revision:{hashlib.sha256(revision_value.encode('utf-8')).hexdigest()}"
    evidence_seed = f"{revision_uid}|{retrieved.isoformat()}|{content_hash}"
    evidence_uid = f"evidence:{hashlib.sha256(evidence_seed.encode('utf-8')).hexdigest()}"

    return NormalizedEvidence(
        evidence_uid=evidence_uid,
        document_uid=document_uid,
        revision_uid=revision_uid,
        source=source,
        source_document_id=source_document_id,
        source_revision=revision_value,
        url=str(url or ""),
        title=str(title or ""),
        body=str(body or ""),
        claimed_published_at=claimed,
        first_seen_at=first_seen,
        retrieved_at=retrieved,
        modified_at=modified,
        backfill=bool(backfill),
        content_hash=content_hash,
        raw_artifact_uid=raw_artifact_uid,
        parser_version=parser_version,
        publication_timestamp_status=publication_status,
        metadata=dict(metadata or {}),
    )
