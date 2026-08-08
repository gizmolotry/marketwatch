"""Frozen, evidence-oriented records emitted by the Repo Cartographer MVP."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
from types import MappingProxyType
from typing import Any, Mapping

from .canonical import canonical_sha256, normalize_repo_path, stable_sha256


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_OID = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")


class _LowercaseEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class TrackedState(_LowercaseEnum):
    TRACKED = "tracked"
    STAGED = "staged"
    MODIFIED = "modified"
    DELETED = "deleted"
    UNTRACKED = "untracked"
    IGNORED = "ignored"


class FileKind(_LowercaseEnum):
    SOURCE = "source"
    TEST = "test"
    SPEC = "spec"
    DOC = "doc"
    CONFIG = "config"
    ARTIFACT = "artifact"
    GENERATED = "generated"
    VENDOR = "vendor"
    OTHER = "other"


class SymbolKind(_LowercaseEnum):
    MODULE = "module"
    CLASS = "class"
    FUNCTION = "function"
    ASYNC_FUNCTION = "async_function"
    METHOD = "method"
    ASYNC_METHOD = "async_method"


class EntrypointKind(_LowercaseEnum):
    PYTHON_MAIN = "python_main"
    CONSOLE_CLI = "console_cli"
    ARGPARSE_COMMAND = "argparse_command"
    FASTAPI_ROUTE = "fastapi_route"
    TEST = "test"
    UNKNOWN = "unknown"


class EvidenceKind(_LowercaseEnum):
    FILE_BYTES = "file_bytes"
    GIT_STATE = "git_state"
    AST_SYMBOL = "ast_symbol"
    AST_IMPORT = "ast_import"
    AST_CALL = "ast_call"
    ENTRYPOINT = "entrypoint"
    TEST_DECLARATION = "test_declaration"
    GHERKIN_TAG = "gherkin_tag"
    CONFIG_VALUE = "config_value"
    ARTIFACT_BYTES = "artifact_bytes"
    ARTIFACT_MANIFEST = "artifact_manifest"
    DOC_CLAIM = "doc_claim"
    NEGATIVE_MARKER = "negative_marker"


class DefinitionState(_LowercaseEnum):
    ABSENT = "absent"
    DOCS_ONLY = "docs_only"
    DECLARATION = "declaration"
    SCAFFOLD = "scaffold"
    SUBSTANTIVE_IMPLEMENTATION = "substantive_implementation"


class IntegrationState(_LowercaseEnum):
    ISOLATED = "isolated"
    IMPORTABLE = "importable"
    STATICALLY_WIRED = "statically_wired"
    DYNAMICALLY_REACHABLE = "dynamically_reachable"


class VerificationState(_LowercaseEnum):
    NONE = "none"
    TEST_DECLARED = "test_declared"
    TEST_PASSED = "test_passed"


class ArtifactState(_LowercaseEnum):
    NONE = "none"
    MANIFEST_ONLY = "manifest_only"
    BYTES_PRESENT = "bytes_present"
    HASH_VERIFIED = "hash_verified"


class LearningState(_LowercaseEnum):
    NOT_APPLICABLE = "not_applicable"
    ARCHITECTURE_ONLY = "architecture_only"
    UNTRAINED = "untrained"
    TRAINED_UNVERIFIED = "trained_unverified"
    TRAINED_VERIFIED = "trained_verified"


class EvaluationState(_LowercaseEnum):
    NONE = "none"
    MECHANIC_FIXTURE = "mechanic_fixture"
    RETROSPECTIVE_CASE = "retrospective_case"
    FROZEN_HOLDOUT = "frozen_holdout"
    PROSPECTIVE = "prospective"


class RuntimeState(_LowercaseEnum):
    NOT_OBSERVED = "not_observed"
    UNAVAILABLE = "unavailable"
    RUNNING = "running"
    PROBE_FAILED = "probe_failed"


def _nonempty(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not (normalized := value.strip()):
        raise ValueError(f"{field_name} must be a non-empty string")
    return normalized


def _sha(value: str, field_name: str, *, allow_empty: bool = False) -> str:
    if allow_empty and value == "":
        return value
    normalized = _nonempty(value, field_name).lower()
    if not _SHA256.fullmatch(normalized):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return normalized


def _git_oid(value: str, field_name: str) -> str:
    normalized = _nonempty(value, field_name).lower()
    if not _GIT_OID.fullmatch(normalized):
        raise ValueError(f"{field_name} must be a 40- or 64-character Git object id")
    return normalized


def _uid(value: str, field_name: str) -> str:
    return _nonempty(value, field_name)


def _string_tuple(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise TypeError(f"{field_name} must be a tuple")
    return tuple(sorted({_nonempty(value, field_name) for value in values}))


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class ScanConfig:
    include_globs: tuple[str, ...] = ("**/*",)
    exclude_globs: tuple[str, ...] = (".git/**", ".cartographer/**")
    artifact_extensions: tuple[str, ...] = (".faiss", ".joblib", ".onnx", ".pkl", ".pt", ".pth")
    max_file_bytes: int = 10_000_000
    include_untracked: bool = True
    include_ignored: bool = False
    sensitive_globs: tuple[str, ...] = (".env", ".env.*", "**/*secret*", "**/*token*")

    def __post_init__(self) -> None:
        object.__setattr__(self, "include_globs", _string_tuple(self.include_globs, "include_globs"))
        object.__setattr__(self, "exclude_globs", _string_tuple(self.exclude_globs, "exclude_globs"))
        extensions = _string_tuple(self.artifact_extensions, "artifact_extensions")
        if any(not extension.startswith(".") for extension in extensions):
            raise ValueError("artifact_extensions must start with '.'")
        object.__setattr__(self, "artifact_extensions", tuple(sorted(extension.lower() for extension in extensions)))
        object.__setattr__(self, "sensitive_globs", _string_tuple(self.sensitive_globs, "sensitive_globs"))
        if not isinstance(self.max_file_bytes, int) or isinstance(self.max_file_bytes, bool) or self.max_file_bytes <= 0:
            raise ValueError("max_file_bytes must be a positive integer")
        if not isinstance(self.include_untracked, bool) or not isinstance(self.include_ignored, bool):
            raise TypeError("include_untracked and include_ignored must be booleans")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class GitState:
    head_commit: str
    branch: str
    index_tree: str
    dirty: bool
    staged_paths: tuple[str, ...] = ()
    modified_paths: tuple[str, ...] = ()
    deleted_paths: tuple[str, ...] = ()
    untracked_paths: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("head_commit", "index_tree"):
            value = getattr(self, name)
            if value not in {"UNBORN", "UNTRACKED"}:
                object.__setattr__(self, name, _git_oid(value, name))
        object.__setattr__(self, "branch", _nonempty(self.branch, "branch"))
        if not isinstance(self.dirty, bool):
            raise TypeError("dirty must be a bool")
        for name in ("staged_paths", "modified_paths", "deleted_paths", "untracked_paths"):
            paths = getattr(self, name)
            if not isinstance(paths, tuple):
                raise TypeError(f"{name} must be a tuple")
            object.__setattr__(self, name, tuple(sorted({normalize_repo_path(path) for path in paths})))


@dataclass(frozen=True, slots=True)
class RepositorySnapshot:
    snapshot_uid: str
    root_display: str
    git: GitState
    config_sha256: str
    scanner_version: str
    files_root_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "snapshot_uid", _uid(self.snapshot_uid, "snapshot_uid"))
        object.__setattr__(self, "root_display", _nonempty(self.root_display, "root_display"))
        if not isinstance(self.git, GitState):
            raise TypeError("git must be a GitState")
        object.__setattr__(self, "config_sha256", _sha(self.config_sha256, "config_sha256"))
        object.__setattr__(self, "files_root_sha256", _sha(self.files_root_sha256, "files_root_sha256"))
        object.__setattr__(self, "scanner_version", _nonempty(self.scanner_version, "scanner_version"))

    @property
    def sha256(self) -> str:
        return stable_sha256(self)


@dataclass(frozen=True, slots=True)
class FileFact:
    file_uid: str
    path: str
    kind: FileKind
    tracked_state: TrackedState
    byte_size: int
    sha256: str
    language: str | None
    sensitive: bool
    parse_status: str
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "file_uid", _uid(self.file_uid, "file_uid"))
        object.__setattr__(self, "path", normalize_repo_path(self.path))
        if not isinstance(self.kind, FileKind) or not isinstance(self.tracked_state, TrackedState):
            raise TypeError("kind and tracked_state must use their respective enums")
        if not isinstance(self.byte_size, int) or isinstance(self.byte_size, bool) or self.byte_size < 0:
            raise ValueError("byte_size must be a non-negative integer")
        object.__setattr__(self, "sha256", _sha(self.sha256, "sha256"))
        if self.language is not None:
            object.__setattr__(self, "language", _nonempty(self.language, "language"))
        if not isinstance(self.sensitive, bool):
            raise TypeError("sensitive must be a bool")
        object.__setattr__(self, "parse_status", _nonempty(self.parse_status, "parse_status"))
        object.__setattr__(self, "reason_codes", _string_tuple(self.reason_codes, "reason_codes"))


@dataclass(frozen=True, slots=True)
class SymbolFact:
    symbol_uid: str
    file_uid: str
    qualified_name: str
    kind: SymbolKind
    line_start: int
    line_end: int
    signature: str | None
    decorators: tuple[str, ...] = ()
    body_classification: str = "unknown"

    def __post_init__(self) -> None:
        for name in ("symbol_uid", "file_uid", "qualified_name"):
            object.__setattr__(self, name, _uid(getattr(self, name), name))
        if not isinstance(self.kind, SymbolKind):
            raise TypeError("kind must be a SymbolKind")
        _lines(self.line_start, self.line_end)
        if self.signature is not None:
            object.__setattr__(self, "signature", _nonempty(self.signature, "signature"))
        object.__setattr__(self, "decorators", _string_tuple(self.decorators, "decorators"))
        object.__setattr__(self, "body_classification", _nonempty(self.body_classification, "body_classification"))


def _lines(start: int, end: int) -> None:
    if any(not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in (start, end)) or end < start:
        raise ValueError("line bounds must be positive integers with line_end >= line_start")


@dataclass(frozen=True, slots=True)
class ImportFact:
    import_uid: str
    file_uid: str
    module: str
    names: tuple[str, ...]
    level: int
    line: int

    def __post_init__(self) -> None:
        for name in ("import_uid", "file_uid", "module"):
            object.__setattr__(self, name, _uid(getattr(self, name), name))
        object.__setattr__(self, "names", _string_tuple(self.names, "names"))
        if not isinstance(self.level, int) or isinstance(self.level, bool) or self.level < 0:
            raise ValueError("level must be a non-negative integer")
        _lines(self.line, self.line)


@dataclass(frozen=True, slots=True)
class EntrypointFact:
    entrypoint_uid: str
    file_uid: str
    symbol_uid: str | None
    kind: EntrypointKind
    name: str
    method: str | None
    path: str | None
    line: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "entrypoint_uid", _uid(self.entrypoint_uid, "entrypoint_uid"))
        object.__setattr__(self, "file_uid", _uid(self.file_uid, "file_uid"))
        if self.symbol_uid is not None:
            object.__setattr__(self, "symbol_uid", _uid(self.symbol_uid, "symbol_uid"))
        if not isinstance(self.kind, EntrypointKind):
            raise TypeError("kind must be an EntrypointKind")
        object.__setattr__(self, "name", _nonempty(self.name, "name"))
        if self.method is not None:
            object.__setattr__(self, "method", _nonempty(self.method, "method").upper())
        if self.path is not None:
            object.__setattr__(self, "path", _nonempty(self.path, "path"))
        _lines(self.line, self.line)


@dataclass(frozen=True, slots=True)
class TestFact:
    test_uid: str
    file_uid: str
    symbol_uid: str | None
    framework: str
    target_hints: tuple[str, ...]
    fixture_only: bool
    line: int

    def __post_init__(self) -> None:
        for name in ("test_uid", "file_uid"):
            object.__setattr__(self, name, _uid(getattr(self, name), name))
        if self.symbol_uid is not None:
            object.__setattr__(self, "symbol_uid", _uid(self.symbol_uid, "symbol_uid"))
        object.__setattr__(self, "framework", _nonempty(self.framework, "framework"))
        object.__setattr__(self, "target_hints", _string_tuple(self.target_hints, "target_hints"))
        if not isinstance(self.fixture_only, bool):
            raise TypeError("fixture_only must be a bool")
        _lines(self.line, self.line)


@dataclass(frozen=True, slots=True)
class SpecScenarioFact:
    scenario_uid: str
    file_uid: str
    feature: str
    rule: str | None
    scenario: str
    tags: tuple[str, ...]
    line: int

    def __post_init__(self) -> None:
        for name in ("scenario_uid", "file_uid", "feature", "scenario"):
            object.__setattr__(self, name, _uid(getattr(self, name), name))
        if self.rule is not None:
            object.__setattr__(self, "rule", _nonempty(self.rule, "rule"))
        object.__setattr__(self, "tags", _string_tuple(self.tags, "tags"))
        _lines(self.line, self.line)


@dataclass(frozen=True, slots=True)
class ArtifactFact:
    artifact_uid: str
    file_uid: str
    format: str
    sha256: str
    manifest_for: str | None
    expected_sha256: str | None
    verification: ArtifactState
    safe_metadata: Mapping[str, Any] = field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("artifact_uid", "file_uid", "format"):
            object.__setattr__(self, name, _uid(getattr(self, name), name))
        object.__setattr__(self, "sha256", _sha(self.sha256, "sha256"))
        if self.manifest_for is not None:
            object.__setattr__(self, "manifest_for", _uid(self.manifest_for, "manifest_for"))
        if self.expected_sha256 is not None:
            object.__setattr__(self, "expected_sha256", _sha(self.expected_sha256, "expected_sha256"))
        if not isinstance(self.verification, ArtifactState):
            raise TypeError("verification must be an ArtifactState")
        if not isinstance(self.safe_metadata, Mapping):
            raise TypeError("safe_metadata must be a mapping")
        object.__setattr__(self, "safe_metadata", _freeze(self.safe_metadata))
        object.__setattr__(self, "reason_codes", _string_tuple(self.reason_codes, "reason_codes"))


@dataclass(frozen=True, slots=True)
class EvidenceFact:
    evidence_uid: str
    subject_uid: str
    kind: EvidenceKind
    source_file_uid: str
    line_start: int | None
    line_end: int | None
    observed_value: Any
    source_sha256: str

    def __post_init__(self) -> None:
        for name in ("evidence_uid", "subject_uid", "source_file_uid"):
            object.__setattr__(self, name, _uid(getattr(self, name), name))
        if not isinstance(self.kind, EvidenceKind):
            raise TypeError("kind must be an EvidenceKind")
        if self.line_start is None or self.line_end is None:
            if self.line_start is not None or self.line_end is not None:
                raise ValueError("line_start and line_end must both be set or both be None")
        else:
            _lines(self.line_start, self.line_end)
        object.__setattr__(self, "observed_value", _freeze(self.observed_value))
        object.__setattr__(self, "source_sha256", _sha(self.source_sha256, "source_sha256"))


@dataclass(frozen=True, slots=True)
class AxisStates:
    definition: DefinitionState = DefinitionState.ABSENT
    integration: IntegrationState = IntegrationState.ISOLATED
    verification: VerificationState = VerificationState.NONE
    artifact: ArtifactState = ArtifactState.NONE
    learning: LearningState = LearningState.NOT_APPLICABLE
    evaluation: EvaluationState = EvaluationState.NONE
    runtime: RuntimeState = RuntimeState.NOT_OBSERVED

    def __post_init__(self) -> None:
        expected = {
            "definition": DefinitionState, "integration": IntegrationState, "verification": VerificationState,
            "artifact": ArtifactState, "learning": LearningState, "evaluation": EvaluationState, "runtime": RuntimeState,
        }
        for name, enum_type in expected.items():
            if not isinstance(getattr(self, name), enum_type):
                raise TypeError(f"{name} must be a {enum_type.__name__}")


@dataclass(frozen=True, slots=True)
class ComponentFact:
    component_uid: str
    component_kind: str
    name: str
    root_subject_uids: tuple[str, ...]
    axes: AxisStates
    supporting_evidence_uids: tuple[str, ...] = ()
    refuting_evidence_uids: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("component_uid", "component_kind", "name"):
            object.__setattr__(self, name, _uid(getattr(self, name), name))
        object.__setattr__(self, "root_subject_uids", _string_tuple(self.root_subject_uids, "root_subject_uids"))
        if not self.root_subject_uids:
            raise ValueError("root_subject_uids must not be empty")
        if not isinstance(self.axes, AxisStates):
            raise TypeError("axes must be an AxisStates")
        for name in ("supporting_evidence_uids", "refuting_evidence_uids", "reason_codes"):
            object.__setattr__(self, name, _string_tuple(getattr(self, name), name))


@dataclass(frozen=True, slots=True)
class CapabilityFact:
    capability_uid: str
    name: str
    component_uids: tuple[str, ...]
    axes: AxisStates
    supporting_evidence_uids: tuple[str, ...] = ()
    refuting_evidence_uids: tuple[str, ...] = ()
    contradiction_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "capability_uid", _uid(self.capability_uid, "capability_uid"))
        object.__setattr__(self, "name", _uid(self.name, "name"))
        object.__setattr__(self, "component_uids", _string_tuple(self.component_uids, "component_uids"))
        if not self.component_uids:
            raise ValueError("component_uids must not be empty")
        if not isinstance(self.axes, AxisStates):
            raise TypeError("axes must be an AxisStates")
        for name in ("supporting_evidence_uids", "refuting_evidence_uids", "contradiction_codes"):
            object.__setattr__(self, name, _string_tuple(getattr(self, name), name))


@dataclass(frozen=True, slots=True)
class Inventory:
    manifest: RepositorySnapshot
    files: tuple[FileFact, ...] = ()
    symbols: tuple[SymbolFact, ...] = ()
    imports: tuple[ImportFact, ...] = ()
    entrypoints: tuple[EntrypointFact, ...] = ()
    tests: tuple[TestFact, ...] = ()
    scenarios: tuple[SpecScenarioFact, ...] = ()
    artifacts: tuple[ArtifactFact, ...] = ()
    evidence: tuple[EvidenceFact, ...] = ()
    components: tuple[ComponentFact, ...] = ()
    capabilities: tuple[CapabilityFact, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, RepositorySnapshot):
            raise TypeError("manifest must be a RepositorySnapshot")
        groups = {
            "files": (FileFact, "file_uid"), "symbols": (SymbolFact, "symbol_uid"), "imports": (ImportFact, "import_uid"),
            "entrypoints": (EntrypointFact, "entrypoint_uid"), "tests": (TestFact, "test_uid"),
            "scenarios": (SpecScenarioFact, "scenario_uid"), "artifacts": (ArtifactFact, "artifact_uid"),
            "evidence": (EvidenceFact, "evidence_uid"), "components": (ComponentFact, "component_uid"),
            "capabilities": (CapabilityFact, "capability_uid"),
        }
        for name, (record_type, uid_name) in groups.items():
            values = getattr(self, name)
            if not isinstance(values, tuple) or any(not isinstance(value, record_type) for value in values):
                raise TypeError(f"{name} must be a tuple of {record_type.__name__}")
            ordered = tuple(sorted(values, key=lambda value: getattr(value, uid_name)))
            if len({getattr(value, uid_name) for value in ordered}) != len(ordered):
                raise ValueError(f"{name} contains duplicate identifiers")
            object.__setattr__(self, name, ordered)

    @property
    def sha256(self) -> str:
        return stable_sha256(self)


class CuratedSelectorKind(_LowercaseEnum):
    CAPABILITY = "capability"
    TEST_CAPABILITY = "test_capability"
    SCENARIO = "scenario"
    SCENARIO_TAG = "scenario_tag"
    ARTIFACT_PATH = "artifact_path"


@dataclass(frozen=True, slots=True)
class CuratedSelector:
    kind: CuratedSelectorKind
    value: str
    required: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.kind, CuratedSelectorKind):
            raise TypeError("kind must be a CuratedSelectorKind")
        object.__setattr__(self, "value", _nonempty(self.value, "value"))
        if not isinstance(self.required, bool):
            raise TypeError("required must be a bool")


@dataclass(frozen=True, slots=True)
class CuratedCapabilityProfile:
    capability_id: str
    title: str
    description: str
    selectors: tuple[CuratedSelector, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "capability_id", _nonempty(self.capability_id, "capability_id"))
        object.__setattr__(self, "title", _nonempty(self.title, "title"))
        object.__setattr__(self, "description", _nonempty(self.description, "description"))
        if not isinstance(self.selectors, tuple) or any(not isinstance(item, CuratedSelector) for item in self.selectors):
            raise TypeError("selectors must be a tuple of CuratedSelector")
        if not self.selectors or not any(item.required for item in self.selectors):
            raise ValueError("selectors must contain at least one required selector")


@dataclass(frozen=True, slots=True)
class CuratedProfile:
    format: str
    profile_id: str
    title: str
    capabilities: tuple[CuratedCapabilityProfile, ...]

    def __post_init__(self) -> None:
        if self.format != "repo-cartographer-curated-profile/v1":
            raise ValueError("unsupported curated profile format")
        object.__setattr__(self, "profile_id", _nonempty(self.profile_id, "profile_id"))
        object.__setattr__(self, "title", _nonempty(self.title, "title"))
        if not isinstance(self.capabilities, tuple) or any(not isinstance(item, CuratedCapabilityProfile) for item in self.capabilities):
            raise TypeError("capabilities must be a tuple of CuratedCapabilityProfile")
        ids = [item.capability_id for item in self.capabilities]
        if not ids or len(ids) != len(set(ids)):
            raise ValueError("capability identifiers must be present and unique")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class CuratedSelectorResult:
    selector: CuratedSelector
    matched_uids: tuple[str, ...]
    matched: bool

    def __post_init__(self) -> None:
        if not isinstance(self.selector, CuratedSelector):
            raise TypeError("selector must be a CuratedSelector")
        object.__setattr__(self, "matched_uids", _string_tuple(self.matched_uids, "matched_uids"))
        if not isinstance(self.matched, bool) or self.matched != bool(self.matched_uids):
            raise ValueError("matched must agree with matched_uids")


@dataclass(frozen=True, slots=True)
class CuratedCapabilityFact:
    capability_id: str
    title: str
    description: str
    axes: AxisStates
    selector_results: tuple[CuratedSelectorResult, ...]
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("capability_id", "title", "description"):
            object.__setattr__(self, name, _nonempty(getattr(self, name), name))
        if not isinstance(self.axes, AxisStates):
            raise TypeError("axes must be an AxisStates")
        if not isinstance(self.selector_results, tuple) or any(not isinstance(item, CuratedSelectorResult) for item in self.selector_results):
            raise TypeError("selector_results must be a tuple of CuratedSelectorResult")
        object.__setattr__(self, "reason_codes", _string_tuple(self.reason_codes, "reason_codes"))


@dataclass(frozen=True, slots=True)
class CuratedCapabilityMap:
    format: str
    profile_id: str
    profile_sha256: str
    source_inventory_root_sha256: str
    source_snapshot_uid: str
    capabilities: tuple[CuratedCapabilityFact, ...]
    test_receipt_sha256s: tuple[str, ...] = ()
    test_runner_policy_sha256s: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.format != "repo-cartographer-curated-map/v1":
            raise ValueError("unsupported curated map format")
        object.__setattr__(self, "profile_id", _nonempty(self.profile_id, "profile_id"))
        object.__setattr__(self, "profile_sha256", _sha(self.profile_sha256, "profile_sha256"))
        object.__setattr__(self, "source_inventory_root_sha256", _sha(self.source_inventory_root_sha256, "source_inventory_root_sha256"))
        object.__setattr__(self, "source_snapshot_uid", _nonempty(self.source_snapshot_uid, "source_snapshot_uid"))
        if not isinstance(self.capabilities, tuple) or any(not isinstance(item, CuratedCapabilityFact) for item in self.capabilities):
            raise TypeError("capabilities must be a tuple of CuratedCapabilityFact")
        ids = [item.capability_id for item in self.capabilities]
        if len(ids) != len(set(ids)):
            raise ValueError("curated map capability identifiers must be unique")
        if not isinstance(self.test_receipt_sha256s, tuple):
            raise TypeError("test_receipt_sha256s must be a tuple")
        object.__setattr__(
            self,
            "test_receipt_sha256s",
            tuple(sorted({_sha(value, "test_receipt_sha256s") for value in self.test_receipt_sha256s})),
        )
        if not isinstance(self.test_runner_policy_sha256s, tuple):
            raise TypeError("test_runner_policy_sha256s must be a tuple")
        object.__setattr__(
            self,
            "test_runner_policy_sha256s",
            tuple(sorted({_sha(value, "test_runner_policy_sha256s") for value in self.test_runner_policy_sha256s})),
        )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


__all__ = [
    "ArtifactFact", "ArtifactState", "AxisStates", "CapabilityFact", "ComponentFact", "DefinitionState",
    "EntrypointFact", "EntrypointKind", "EvaluationState", "EvidenceFact", "EvidenceKind", "FileFact",
    "FileKind", "GitState", "ImportFact", "IntegrationState", "Inventory", "LearningState", "RepositorySnapshot",
    "RuntimeState", "ScanConfig", "SpecScenarioFact", "SymbolFact", "SymbolKind", "TestFact", "TrackedState",
    "VerificationState", "CuratedCapabilityFact", "CuratedCapabilityMap", "CuratedCapabilityProfile",
    "CuratedProfile", "CuratedSelector", "CuratedSelectorKind", "CuratedSelectorResult",
]
