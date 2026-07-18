"""Normalization primitives shared by official-source connectors."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Iterable, Mapping


def require_text(value: Any, field: str) -> str:
    if value is None:
        raise ValueError(f"{field} is required")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def decimal_from(value: Any, field: str, *, minimum: Decimal | None = None,
                 maximum: Decimal | None = None) -> Decimal:
    """Return a finite Decimal without routing string inputs through float."""

    if value is None or isinstance(value, bool):
        raise ValueError(f"{field} is required")
    try:
        if isinstance(value, Decimal):
            result = value
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"{field} must be finite")
            result = Decimal(str(value))
        else:
            result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    if minimum is not None and result < minimum:
        raise ValueError(f"{field} must be >= {minimum}")
    if maximum is not None and result > maximum:
        raise ValueError(f"{field} must be <= {maximum}")
    return result


def utc_datetime(value: Any, field: str = "timestamp") -> datetime:
    """Normalize ISO-8601 or Unix seconds/milliseconds to aware UTC."""

    if value is None or isinstance(value, bool):
        raise ValueError(f"{field} is required")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float, Decimal)) or str(value).strip().lstrip("-").isdigit():
        numeric = Decimal(str(value).strip())
        if abs(numeric) >= Decimal("100000000000"):
            numeric /= Decimal(1000)
        parsed = datetime.fromtimestamp(float(numeric), tz=UTC)
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} is not a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def qualified_id(platform: str, source_id: Any) -> str:
    return f"{require_text(platform, 'platform').lower()}:{require_text(source_id, 'source_id')}"


def stable_uid(platform: str, kind: str, components: Iterable[Any]) -> str:
    body = canonical_json_bytes(list(components))
    digest = hashlib.sha256(body).hexdigest()
    return f"{require_text(platform, 'platform').lower()}:{require_text(kind, 'kind').lower()}:{digest}"


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        aware = utc_datetime(value)
        return aware.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, Enum):
        return jsonable(value.value)
    if dataclasses.is_dataclass(value):
        return {field.name: jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if hasattr(value, "model_dump"):
        return jsonable(value.model_dump(mode="python"))
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats cannot be serialized")
        return str(value)
    return str(value)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        jsonable(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def parse_json_decimal(raw: bytes | str) -> Any:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw, parse_float=Decimal, parse_int=int)
