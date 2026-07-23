"""Deterministic Git and file snapshot collection."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path, PurePosixPath
import subprocess
from typing import Any, Iterable

from .canonical import canonical_sha256, normalize_repo_path, sha256_file
from .domain import FileFact, FileKind, GitState, RepositorySnapshot, TrackedState


SCANNER_VERSION = "repo-cartographer/0.1"


class SnapshotError(RuntimeError):
    """Raised when a repository snapshot cannot be collected safely."""


def _git(root: Path, *args: str, check: bool = True) -> bytes:
    completed = subprocess.run(
        ["git", "-c", "core.quotepath=false", *args],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and completed.returncode:
        message = completed.stderr.decode("utf-8", errors="replace").strip()
        raise SnapshotError(f"git {' '.join(args)} failed: {message}")
    return completed.stdout


def _decode_path(raw: bytes) -> str:
    return normalize_repo_path(raw.decode("utf-8", errors="surrogateescape"))


def _parse_porcelain(payload: bytes) -> dict[str, tuple[str, str]]:
    """Parse ``git status --porcelain=v1 -z`` into path -> (index, worktree)."""

    entries = payload.split(b"\0")
    result: dict[str, tuple[str, str]] = {}
    index = 0
    while index < len(entries):
        entry = entries[index]
        index += 1
        if not entry:
            continue
        if len(entry) < 4 or entry[2:3] != b" ":
            raise SnapshotError("unexpected git porcelain record")
        x = chr(entry[0])
        y = chr(entry[1])
        path = _decode_path(entry[3:])
        result[path] = (x, y)
        if x in {"R", "C"}:
            # Under -z, rename/copy records have a second NUL-delimited path.
            if index >= len(entries):
                raise SnapshotError("truncated git rename record")
            index += 1
    return result


def read_git_state(root: str | Path) -> GitState:
    """Read the native Git HEAD, index, and working-tree state."""

    repo = Path(root).resolve()
    if not repo.is_dir():
        raise SnapshotError(f"repository root is not a directory: {repo}")
    inside = _git(repo, "rev-parse", "--is-inside-work-tree").strip()
    if inside != b"true":
        raise SnapshotError(f"not a Git working tree: {repo}")

    head_raw = _git(repo, "rev-parse", "--verify", "HEAD", check=False).strip()
    head_commit = head_raw.decode("ascii") if head_raw else "UNBORN"
    branch_raw = _git(repo, "symbolic-ref", "--quiet", "--short", "HEAD", check=False).strip()
    branch = branch_raw.decode("utf-8", errors="replace") if branch_raw else "DETACHED"
    index_raw = _git(repo, "write-tree").strip()
    index_tree = index_raw.decode("ascii")

    statuses = _parse_porcelain(_git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=all"))
    staged = sorted(path for path, (x, _) in statuses.items() if x not in {" ", "?", "!"})
    modified = sorted(
        path
        for path, (x, y) in statuses.items()
        if (x not in {" ", "?", "!", "D"} or y not in {" ", "?", "!", "D"})
    )
    deleted = sorted(path for path, (x, y) in statuses.items() if x == "D" or y == "D")
    untracked = sorted(path for path, (x, y) in statuses.items() if x == "?" and y == "?")
    return GitState(
        head_commit=head_commit,
        branch=branch,
        index_tree=index_tree,
        dirty=bool(statuses),
        staged_paths=tuple(staged),
        modified_paths=tuple(modified),
        deleted_paths=tuple(deleted),
        untracked_paths=tuple(untracked),
    )


def _patterns(config: Any, *names: str) -> tuple[str, ...]:
    for name in names:
        value = getattr(config, name, None)
        if value is not None:
            return tuple(str(item) for item in value)
    return ()


def _matches(path: str, patterns: Iterable[str]) -> bool:
    candidate = PurePosixPath(path)
    return any(
        candidate.match(pattern) or (pattern.startswith("**/") and candidate.match(pattern[3:]))
        for pattern in patterns
    )


def _enum_member(enum: type[Any], name: str, fallback: str) -> Any:
    return getattr(enum, name, fallback)


def classify_file(path: str | Path, config: Any) -> FileKind:
    """Classify a normalized repository path with explicit, deterministic rules."""

    normalized = normalize_repo_path(path)
    lowered = normalized.casefold()
    name = PurePosixPath(lowered).name
    suffix = PurePosixPath(lowered).suffix
    parts = set(PurePosixPath(lowered).parts)

    if _matches(normalized, _patterns(config, "sensitive_globs", "sensitive_patterns")):
        return FileKind.CONFIG
    if parts & {"node_modules", ".venv", "venv", "vendor", "site-packages"}:
        return FileKind.VENDOR
    if parts & {"dist", "build", "generated", "__pycache__"} or suffix in {".pyc", ".pyo"}:
        return FileKind.GENERATED
    if suffix == ".feature":
        return FileKind.SPEC
    if suffix == ".py":
        if name.startswith("test_") or name == "conftest.py" or "/tests/" in f"/{lowered}" or lowered.startswith("tests/"):
            return FileKind.TEST
        return FileKind.SOURCE
    if suffix in {".md", ".rst", ".txt"}:
        return FileKind.DOC
    if suffix in {".json", ".jsonl", ".yaml", ".yml", ".toml", ".ini", ".cfg"}:
        return FileKind.CONFIG
    artifact_extensions = set(_patterns(config, "artifact_extensions"))
    if suffix in artifact_extensions or suffix in {".ckpt", ".pickle", ".faiss", ".npz"}:
        return FileKind.ARTIFACT
    if suffix in {".js", ".jsx", ".ts", ".tsx", ".go", ".rs", ".java", ".c", ".cc", ".cpp", ".h", ".hpp"}:
        return FileKind.SOURCE
    return FileKind.OTHER


def _language_for(path: str, kind: FileKind) -> str | None:
    suffix = PurePosixPath(path.casefold()).suffix
    languages = {
        ".py": "python",
        ".feature": "gherkin",
        ".js": "javascript",
        ".jsx": "javascript",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".go": "go",
        ".rs": "rust",
        ".java": "java",
        ".c": "c",
        ".cc": "cpp",
        ".cpp": "cpp",
    }
    return languages.get(suffix)


def _sensitive(path: str, config: Any) -> bool:
    conventional = (
        ".env",
        ".env.*",
        "**/.env",
        "**/.env.*",
        "**/secrets/**",
        "**/credentials/**",
        "**/*.pem",
        "**/*.key",
        "**/*.p12",
        "**/*.pfx",
    )
    return _matches(path, (*conventional, *_patterns(config, "sensitive_globs", "sensitive_patterns")))


def _config_hash(config: Any) -> str:
    if hasattr(config, "sha256"):
        return str(config.sha256)
    if hasattr(config, "fingerprint"):
        value = config.fingerprint()
        return str(value)
    if is_dataclass(config):
        return canonical_sha256(asdict(config))
    if hasattr(config, "to_dict"):
        return canonical_sha256(config.to_dict())
    return canonical_sha256({"config": repr(config)})


def _listed_paths(root: Path, config: Any) -> tuple[set[str], set[str], set[str]]:
    tracked = {_decode_path(item) for item in _git(root, "ls-files", "-z").split(b"\0") if item}
    untracked = set()
    if getattr(config, "include_untracked", True):
        untracked = {
            _decode_path(item)
            for item in _git(root, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
            if item
        }
    ignored = set()
    if getattr(config, "include_ignored", False):
        ignored = {
            _decode_path(item)
            for item in _git(root, "ls-files", "--others", "--ignored", "--exclude-standard", "-z").split(b"\0")
            if item
        }
    return tracked, untracked, ignored


def _safe_target(root: Path, relative: str) -> Path | None:
    candidate = root / relative
    if candidate.is_symlink():
        return None
    target = candidate.resolve()
    try:
        target.relative_to(root)
    except ValueError:
        return None
    if target.is_symlink() or not target.is_file():
        return None
    return target


def _stable_file_hash(target: Path) -> tuple[int, str]:
    """Hash a file only when its size and timestamps remain stable during reading."""

    before = target.stat()
    digest = sha256_file(target)
    after = target.stat()
    before_key = (before.st_size, before.st_mtime_ns, getattr(before, "st_ino", None))
    after_key = (after.st_size, after.st_mtime_ns, getattr(after, "st_ino", None))
    if before_key != after_key:
        raise SnapshotError(f"file changed while hashing: {target.name}")
    return after.st_size, digest


def snapshot_repository(root: str | Path, config: Any) -> tuple[RepositorySnapshot, tuple[FileFact, ...]]:
    """Freeze Git facts and byte hashes for tracked and nonignored untracked files."""

    repo = Path(root).resolve()
    git = read_git_state(repo)
    tracked, untracked, ignored = _listed_paths(repo, config)
    exclude = _patterns(config, "exclude_globs", "excluded_globs", "ignore_globs")
    include = _patterns(config, "include_globs") or ("**/*",)
    facts: list[FileFact] = []
    for relative in sorted(tracked | untracked | ignored):
        if relative in git.deleted_paths or _matches(relative, exclude) or not _matches(relative, include):
            continue
        target = _safe_target(repo, relative)
        if target is None:
            continue
        size, digest = _stable_file_hash(target)
        kind = classify_file(relative, config)
        sensitive = _sensitive(relative, config)
        tracked_state = (
            TrackedState.STAGED
            if relative in git.staged_paths
            else TrackedState.MODIFIED
            if relative in git.modified_paths
            else TrackedState.TRACKED
            if relative in tracked
            else TrackedState.IGNORED
            if relative in ignored
            else TrackedState.UNTRACKED
        )
        oversized = size > getattr(config, "max_file_bytes", 10_000_000)
        parse_status = "skipped" if sensitive or oversized else "pending"
        reason_codes = tuple(
            reason
            for condition, reason in (
                (sensitive, "sensitive_content_not_parsed"),
                (oversized, "max_file_bytes_exceeded"),
            )
            if condition
        )
        file_uid = canonical_sha256({"path": relative, "sha256": digest})
        facts.append(
            FileFact(
                file_uid=file_uid,
                path=relative,
                kind=kind,
                tracked_state=tracked_state,
                byte_size=size,
                sha256=digest,
                language=_language_for(relative, kind),
                sensitive=sensitive,
                parse_status=parse_status,
                reason_codes=reason_codes,
            )
        )

    final_git = read_git_state(repo)
    if final_git != git:
        raise SnapshotError("repository changed while snapshot was being collected")

    file_rows = [
        {
            "file_uid": item.file_uid,
            "path": item.path,
            "sha256": item.sha256,
            "byte_size": item.byte_size,
            "tracked_state": getattr(item.tracked_state, "value", item.tracked_state),
        }
        for item in facts
    ]
    files_root_sha256 = canonical_sha256(file_rows)
    config_sha256 = _config_hash(config)
    snapshot_payload = {
        "git": {
            "head_commit": git.head_commit,
            "index_tree": git.index_tree,
            "dirty": git.dirty,
            "staged_paths": git.staged_paths,
            "modified_paths": git.modified_paths,
            "deleted_paths": git.deleted_paths,
            "untracked_paths": git.untracked_paths,
        },
        "config_sha256": config_sha256,
        "scanner_version": SCANNER_VERSION,
        "files_root_sha256": files_root_sha256,
    }
    snapshot = RepositorySnapshot(
        snapshot_uid=canonical_sha256(snapshot_payload),
        root_display=repo.name,
        git=git,
        config_sha256=config_sha256,
        scanner_version=SCANNER_VERSION,
        files_root_sha256=files_root_sha256,
    )
    return snapshot, tuple(facts)
