"""Shared validation primitives for MarketLeak's canonical v2 records."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import re
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)


SCHEMA_VERSION = "2.0.0"

NonEmptyStr = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1),
]
StableUID = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=3,
        pattern=r"^[a-z][a-z0-9_-]*:[^\s:][^\s]*$",
    ),
]
Probability = Annotated[Decimal, Field(strict=True, ge=Decimal("0"), le=Decimal("1"))]
NonNegativeDecimal = Annotated[Decimal, Field(strict=True, ge=Decimal("0"))]

_PLATFORM_RE = re.compile(r"^[a-z][a-z0-9_-]*$")


def platform_qualified_uid(platform: str, source_id: str) -> str:
    """Return a stable platform-qualified identifier without guessing identity.

    The source identifier is treated as opaque. Callers must supply the durable
    identifier exposed by the source, rather than a display name or mutable slug.
    """

    normalized_platform = platform.strip().lower()
    normalized_source_id = source_id.strip()
    if not _PLATFORM_RE.fullmatch(normalized_platform):
        raise ValueError("platform must contain only lowercase letters, digits, '_' or '-'")
    if not normalized_source_id or any(char.isspace() for char in normalized_source_id):
        raise ValueError("source_id must be a non-empty opaque identifier without whitespace")
    return f"{normalized_platform}:{normalized_source_id}"


def assert_uid_matches_platform(uid: str, platform: str, field_name: str) -> str:
    expected_prefix = f"{platform}:"
    if not uid.startswith(expected_prefix):
        raise ValueError(f"{field_name} must be qualified by platform '{platform}'")
    return uid


class StrictDomainModel(BaseModel):
    """Immutable, strict base class used by every canonical record."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class LineageRecord(StrictDomainModel):
    """Minimum provenance required for a canonical or derived record."""

    schema_version: NonEmptyStr = SCHEMA_VERSION
    event_time: datetime
    ingested_at: datetime
    source_uid: StableUID
    raw_artifact_uid: StableUID
    parser_version: NonEmptyStr

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(timezone.utc)


def json_ready(value: BaseModel | dict[str, Any]) -> dict[str, Any]:
    """Serialize a strict model into JSON-compatible primitives."""

    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return dict(value)
