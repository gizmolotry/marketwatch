"""Bundled pytest plugin emitting structured execution facts without raw output."""

from __future__ import annotations

import json
import os
from pathlib import Path


_state: dict[str, object] = {}


def pytest_configure(config):
    _state.clear()
    _state.update({"collected_nodeids": [], "markers": {}, "phases": [], "collection_errors": [], "exit_code": None})


def pytest_collection_finish(session):
    _state["collected_nodeids"] = sorted(item.nodeid for item in session.items)
    _state["markers"] = {
        item.nodeid: sorted(name for name in ("skip", "skipif", "xfail") if item.get_closest_marker(name) is not None)
        for item in session.items
    }


def pytest_collectreport(report):
    if report.failed:
        _state["collection_errors"].append({"nodeid": report.nodeid, "outcome": "failed"})


def pytest_runtest_logreport(report):
    if report.when not in {"setup", "call", "teardown"}:
        return
    _state["phases"].append({
        "nodeid": report.nodeid,
        "when": report.when,
        "outcome": report.outcome,
        "wasxfail": bool(getattr(report, "wasxfail", False)),
    })


def pytest_sessionfinish(session, exitstatus):
    _state["exit_code"] = int(exitstatus)
    destination = os.environ.get("REPO_CARTOGRAPHER_PYTEST_EVENTS")
    if destination:
        Path(destination).write_text(json.dumps(_state, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
