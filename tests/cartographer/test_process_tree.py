"""Process-tree timeout cleanup mechanics fixtures only."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import time

from repo_cartographer.process_tree import run_process_tree


def _pid_exists(pid: int) -> bool:
    if os.name == "nt":
        result = subprocess.run(
            [str(Path(os.environ.get("SYSTEMROOT", r"C:\Windows")) / "System32" / "tasklist.exe"), "/FI", f"PID eq {pid}", "/NH"],
            shell=False,
            capture_output=True,
            check=False,
            timeout=5,
        )
        return str(pid).encode("ascii") in result.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_timeout_terminates_descendant_before_it_can_survive_the_parent(tmp_path):
    child_script = tmp_path / "child.py"
    parent_script = tmp_path / "parent.py"
    child_pid_path = tmp_path / "child.pid"
    survived_path = tmp_path / "descendant-survived.txt"
    child_script.write_text(
        "import pathlib, sys, time\n"
        "time.sleep(5)\n"
        "pathlib.Path(sys.argv[1]).write_text('survived', encoding='ascii')\n"
        "time.sleep(60)\n",
        encoding="utf-8",
    )
    parent_script.write_text(
        "import pathlib, subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, sys.argv[1], sys.argv[3]])\n"
        "pathlib.Path(sys.argv[2]).write_text(str(child.pid), encoding='ascii')\n"
        "time.sleep(60)\n",
        encoding="utf-8",
    )

    result = run_process_tree(
        [sys.executable, str(parent_script), str(child_script), str(child_pid_path), str(survived_path)],
        cwd=tmp_path,
        env={"PATH": os.environ.get("PATH", ""), "SYSTEMROOT": os.environ.get("SYSTEMROOT", "")},
        timeout_seconds=3.0,
        grace_seconds=0.5,
    )

    assert result.timed_out is True
    assert result.returncode == 124
    assert child_pid_path.is_file()
    child_pid = int(child_pid_path.read_text(encoding="ascii"))
    deadline = time.monotonic() + 1.0
    while _pid_exists(child_pid) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not _pid_exists(child_pid)
    time.sleep(2.2)
    assert not survived_path.exists()
