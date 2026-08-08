"""Bundled pytest plugin emitting bounded structured execution facts."""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import tempfile
import tokenize
from typing import Any


_DEFAULT_MAX_EVENT_BYTES = 1_000_000
_MIN_MAX_EVENT_BYTES = 1_024
_MAX_MAX_EVENT_BYTES = 10_000_000
_RESERVED_ENVELOPE_BYTES = 768
_state: dict[str, Any] = {}
_definition_line_cache: dict[str, dict[tuple[int, str], tuple[int, ...]]] = {}


def _configured_limit() -> int:
    raw = os.environ.get("REPO_CARTOGRAPHER_PYTEST_EVENTS_MAX_BYTES", "")
    try:
        value = int(raw) if raw else _DEFAULT_MAX_EVENT_BYTES
    except ValueError:
        return _MIN_MAX_EVENT_BYTES
    return min(max(value, _MIN_MAX_EVENT_BYTES), _MAX_MAX_EVENT_BYTES)


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"


def _mark_overflow() -> None:
    _state["event_overflow"] = True
    _state["collected_nodeids"] = []
    _state["collected_items"] = []
    _state["markers"] = {}
    _state["phases"] = []
    _state["collection_errors"] = [{"nodeid": "plugin_event_file", "outcome": "overflow"}]
    _state["estimated_payload_bytes"] = 0


def _reserve(value: Any) -> bool:
    if _state.get("event_overflow"):
        return False
    increment = len(_json_bytes(value))
    if int(_state["estimated_payload_bytes"]) + increment + _RESERVED_ENVELOPE_BYTES > int(_state["max_event_bytes"]):
        _mark_overflow()
        return False
    _state["estimated_payload_bytes"] = int(_state["estimated_payload_bytes"]) + increment
    return True


def _append(key: str, value: dict[str, Any]) -> None:
    if _reserve(value):
        _state[key].append(value)


def _definition_lines(root: Path, relative_path: str) -> dict[tuple[int, str], tuple[int, ...]]:
    cached = _definition_line_cache.get(relative_path)
    if cached is not None:
        return cached
    candidates: dict[tuple[int, str], list[int]] = {}
    try:
        with tokenize.open(root / relative_path) as handle:
            tree = ast.parse(handle.read(), filename=relative_path)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            collection_line = min(
                (decorator.lineno for decorator in node.decorator_list),
                default=node.lineno,
            )
            candidates.setdefault((collection_line, node.name), []).append(node.lineno)
    except (OSError, UnicodeError, SyntaxError, ValueError):
        candidates = {}
    frozen = {key: tuple(sorted(values)) for key, values in candidates.items()}
    _definition_line_cache[relative_path] = frozen
    return frozen


def _relative_location(item) -> tuple[str, int, int, str]:
    relative_path, zero_based_line, _ = item.location
    path = Path(str(relative_path))
    if path.is_absolute():
        try:
            path = path.resolve().relative_to(Path(item.config.rootpath).resolve())
        except (OSError, ValueError):
            return "", 0, 0, ""
    normalized = path.as_posix().removeprefix("./")
    if not normalized or any(part in {"", ".", ".."} for part in normalized.split("/")):
        return "", 0, 0, ""
    line = zero_based_line + 1 if isinstance(zero_based_line, int) and zero_based_line >= 0 else 0
    original_name = getattr(item, "originalname", None)
    if not isinstance(original_name, str) or not original_name:
        original_name = getattr(item, "name", "")
        if isinstance(original_name, str):
            original_name = original_name.split("[", 1)[0]
    if not isinstance(original_name, str):
        original_name = ""
    definitions = _definition_lines(Path(item.config.rootpath), normalized).get((line, original_name), ())
    definition_line = definitions[0] if len(definitions) == 1 else line
    return normalized, line, definition_line, original_name


def _atomic_write(destination: Path, payload: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def pytest_configure(config):
    _state.clear()
    _definition_line_cache.clear()
    _state.update({
        "collected_nodeids": [],
        "collected_items": [],
        "markers": {},
        "phases": [],
        "collection_errors": [],
        "exit_code": None,
        "event_overflow": False,
        "max_event_bytes": _configured_limit(),
        "estimated_payload_bytes": 0,
    })


def pytest_collection_finish(session):
    for item in session.items:
        file_path, line, definition_line, original_name = _relative_location(item)
        if not file_path or line <= 0 or not original_name:
            _append("collection_errors", {"nodeid": item.nodeid, "outcome": "invalid_location"})
            continue
        collected = {
            "nodeid": item.nodeid,
            "file_path": file_path,
            "line": line,
            "definition_line": definition_line,
            "original_name": original_name,
        }
        if not _reserve({"collected_item": collected, "collected_nodeid": item.nodeid}):
            break
        _state["collected_items"].append(collected)
        _state["collected_nodeids"].append(item.nodeid)
        marker_names = sorted(
            name for name in ("skip", "skipif", "xfail")
            if item.get_closest_marker(name) is not None
        )
        if marker_names:
            marker_row = {item.nodeid: marker_names}
            if not _reserve(marker_row):
                break
            _state["markers"][item.nodeid] = marker_names


def pytest_collectreport(report):
    if report.failed:
        _append("collection_errors", {"nodeid": report.nodeid, "outcome": "failed"})


def pytest_runtest_logreport(report):
    if report.when not in {"setup", "call", "teardown"}:
        return
    _append("phases", {
        "nodeid": report.nodeid,
        "when": report.when,
        "outcome": report.outcome,
        "wasxfail": bool(getattr(report, "wasxfail", False)),
    })


def pytest_sessionfinish(session, exitstatus):
    _state["exit_code"] = int(exitstatus)
    destination = os.environ.get("REPO_CARTOGRAPHER_PYTEST_EVENTS")
    if not destination:
        return
    payload = {key: value for key, value in _state.items() if key != "estimated_payload_bytes"}
    encoded = _json_bytes(payload)
    if len(encoded) > int(_state["max_event_bytes"]):
        _mark_overflow()
        payload = {key: value for key, value in _state.items() if key != "estimated_payload_bytes"}
        encoded = _json_bytes(payload)
    _atomic_write(Path(destination), encoded)
