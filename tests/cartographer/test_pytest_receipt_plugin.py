"""Bundled pytest event-plugin mechanics fixtures only."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


def _run_plugin(tmp_path: Path, source: str, *, max_bytes: int = 65_536) -> tuple[subprocess.CompletedProcess[bytes], bytes, dict]:
    repository = tmp_path / "repository"
    test_path = repository / "tests" / "test_plugin_cases.py"
    test_path.parent.mkdir(parents=True)
    test_path.write_text(source, encoding="utf-8")
    event_path = repository / "events.json"
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),
        "PYTHONPATH": str(Path(__file__).parents[2]),
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTEST_ADDOPTS": "",
        "REPO_CARTOGRAPHER_PYTEST_EVENTS": str(event_path),
        "REPO_CARTOGRAPHER_PYTEST_EVENTS_MAX_BYTES": str(max_bytes),
    }
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "--rootdir=.",
            "-p",
            "repo_cartographer.pytest_receipt_plugin",
            "tests/test_plugin_cases.py",
        ],
        cwd=repository,
        env=environment,
        shell=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=30,
    )
    raw = event_path.read_bytes()
    return completed, raw, json.loads(raw.decode("utf-8"))


def test_plugin_emits_only_the_live_location_for_a_shadowed_duplicate_declaration(tmp_path):
    completed, _, events = _run_plugin(
        tmp_path,
        "def test_shadowed():\n    assert False\n\ndef test_shadowed():\n    assert True\n",
    )

    assert completed.returncode == 0
    assert events["collected_items"] == [{
        "definition_line": 4,
        "file_path": "tests/test_plugin_cases.py",
        "line": 4,
        "nodeid": "tests/test_plugin_cases.py::test_shadowed",
        "original_name": "test_shadowed",
    }]


def test_plugin_preserves_native_and_definition_locations_for_parametrized_method_cases(tmp_path):
    completed, _, events = _run_plugin(
        tmp_path,
        "import pytest\n\nclass TestCases:\n    @pytest.mark.parametrize('value', [1, 2])\n    def test_method(self, value):\n        assert value > 0\n",
    )

    assert completed.returncode == 0
    items = events["collected_items"]
    assert len(items) == 2
    assert {item["nodeid"] for item in items} == {
        "tests/test_plugin_cases.py::TestCases::test_method[1]",
        "tests/test_plugin_cases.py::TestCases::test_method[2]",
    }
    assert all(item["file_path"] == "tests/test_plugin_cases.py" for item in items)
    assert all(item["line"] == 4 for item in items)
    assert all(item["definition_line"] == 5 for item in items)
    assert all(item["original_name"] == "test_method" for item in items)


def test_plugin_overflow_event_is_explicit_and_never_exceeds_the_configured_byte_bound(tmp_path):
    completed, raw, events = _run_plugin(
        tmp_path,
        "import pytest\n\n@pytest.mark.parametrize('value', range(200))\ndef test_many_cases(value):\n    assert value >= 0\n",
        max_bytes=1_024,
    )

    assert completed.returncode == 0
    assert len(raw) <= 1_024
    assert events["event_overflow"] is True
    assert events["collected_items"] == []
    assert events["phases"] == []
    assert events["collection_errors"] == [{"nodeid": "plugin_event_file", "outcome": "overflow"}]
