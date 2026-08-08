"""Bounded subprocess execution with whole-process-tree timeout cleanup."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import signal
import subprocess
import time
from typing import Mapping, Sequence


_WINDOWS_TASKKILL_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class ProcessTreeResult:
    returncode: int
    timed_out: bool


def _validate_argv(argv: Sequence[str]) -> tuple[str, ...]:
    if not isinstance(argv, (list, tuple)) or not argv or any(not isinstance(value, str) or not value for value in argv):
        raise ValueError("argv must be a non-empty sequence of non-empty strings")
    return tuple(argv)


def _windows_taskkill_path() -> str:
    system_root = Path(os.environ.get("SYSTEMROOT", r"C:\Windows"))
    candidate = system_root / "System32" / "taskkill.exe"
    return str(candidate) if candidate.is_file() else "taskkill.exe"


def _terminate_windows_tree(process: subprocess.Popen[bytes], grace_seconds: float) -> None:
    if process.poll() is not None:
        return
    try:
        subprocess.run(
            [_windows_taskkill_path(), "/PID", str(process.pid), "/T", "/F"],
            shell=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=max(grace_seconds, _WINDOWS_TASKKILL_TIMEOUT_SECONDS),
        )
    except (OSError, subprocess.TimeoutExpired):
        process.kill()
    try:
        process.wait(timeout=max(grace_seconds, 0.1))
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _posix_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_posix_tree(process: subprocess.Popen[bytes], grace_seconds: float) -> None:
    process_group = process.pid
    try:
        os.killpg(process_group, signal.SIGTERM)
    except ProcessLookupError:
        process.wait()
        return
    deadline = time.monotonic() + grace_seconds
    while _posix_group_exists(process_group) and time.monotonic() < deadline:
        process.poll()
        time.sleep(0.01)
    if _posix_group_exists(process_group):
        try:
            os.killpg(process_group, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait()


def terminate_process_tree(process: subprocess.Popen[bytes], *, grace_seconds: float = 1.0) -> None:
    """Terminate a process and descendants created in its isolated process group."""

    if not isinstance(grace_seconds, (int, float)) or isinstance(grace_seconds, bool) or grace_seconds < 0:
        raise ValueError("grace_seconds must be a non-negative number")
    if os.name == "nt":
        _terminate_windows_tree(process, float(grace_seconds))
    else:
        _terminate_posix_tree(process, float(grace_seconds))


def run_process_tree(
    argv: Sequence[str],
    *,
    cwd: str | Path,
    env: Mapping[str, str],
    timeout_seconds: float,
    grace_seconds: float = 1.0,
) -> ProcessTreeResult:
    """Run fixed argv and return 124 after cleaning its entire isolated tree on timeout."""

    command = _validate_argv(argv)
    if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be a positive number")
    popen_kwargs: dict[str, object] = {}
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kwargs["start_new_session"] = True
    process = subprocess.Popen(
        command,
        cwd=Path(cwd),
        env=dict(env),
        shell=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        **popen_kwargs,
    )
    try:
        return ProcessTreeResult(process.wait(timeout=float(timeout_seconds)), False)
    except subprocess.TimeoutExpired:
        terminate_process_tree(process, grace_seconds=grace_seconds)
        return ProcessTreeResult(124, True)
    except BaseException:
        terminate_process_tree(process, grace_seconds=grace_seconds)
        raise


__all__ = ["ProcessTreeResult", "run_process_tree", "terminate_process_tree"]
