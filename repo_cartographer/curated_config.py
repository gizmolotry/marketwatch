"""Strict loading for versioned curated capability profiles."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .domain import CuratedCapabilityProfile, CuratedProfile, CuratedSelector, CuratedSelectorKind


_PROFILE_KEYS = {"format", "profile_id", "title", "capabilities"}
_CAPABILITY_KEYS = {"id", "title", "description", "selectors"}
_SELECTOR_KEYS = {"kind", "value", "required"}


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    observed = set(value)
    if observed != expected:
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unknown " + ", ".join(extra))
        raise ValueError(f"{label} has invalid keys: {'; '.join(details)}")


def curated_profile_from_dict(payload: Mapping[str, Any]) -> CuratedProfile:
    root = _object(payload, "curated profile")
    _keys(root, _PROFILE_KEYS, "curated profile")
    rows = root["capabilities"]
    if not isinstance(rows, list):
        raise ValueError("capabilities must be an array")
    capabilities: list[CuratedCapabilityProfile] = []
    for index, raw_capability in enumerate(rows):
        capability = _object(raw_capability, f"capabilities[{index}]")
        _keys(capability, _CAPABILITY_KEYS, f"capabilities[{index}]")
        raw_selectors = capability["selectors"]
        if not isinstance(raw_selectors, list):
            raise ValueError(f"capabilities[{index}].selectors must be an array")
        selectors: list[CuratedSelector] = []
        for selector_index, raw_selector in enumerate(raw_selectors):
            selector = _object(raw_selector, f"capabilities[{index}].selectors[{selector_index}]")
            _keys(selector, _SELECTOR_KEYS, f"capabilities[{index}].selectors[{selector_index}]")
            if not isinstance(selector["required"], bool):
                raise ValueError(f"capabilities[{index}].selectors[{selector_index}].required must be a boolean")
            try:
                kind = CuratedSelectorKind(selector["kind"])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"capabilities[{index}].selectors[{selector_index}].kind is unsupported") from exc
            selectors.append(CuratedSelector(kind=kind, value=selector["value"], required=selector["required"]))
        capabilities.append(
            CuratedCapabilityProfile(
                capability_id=capability["id"],
                title=capability["title"],
                description=capability["description"],
                selectors=tuple(selectors),
            )
        )
    return CuratedProfile(
        format=root["format"],
        profile_id=root["profile_id"],
        title=root["title"],
        capabilities=tuple(capabilities),
    )


def load_curated_profile(path: str | Path) -> CuratedProfile:
    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"curated profile is unreadable: {exc}") from exc
    return curated_profile_from_dict(_object(payload, "curated profile"))


__all__ = ["curated_profile_from_dict", "load_curated_profile"]
