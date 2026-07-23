"""Conservative, evidence-backed component and capability status derivation."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import PurePosixPath
import re
from typing import Any

from .canonical import canonical_sha256
from .domain import (
    ArtifactFact,
    ArtifactState,
    AxisStates,
    CapabilityFact,
    ComponentFact,
    DefinitionState,
    EvaluationState,
    EvidenceFact,
    EvidenceKind,
    IntegrationState,
    LearningState,
    RuntimeState,
    VerificationState,
)


_DEFINITION_ORDER = (
    DefinitionState.ABSENT,
    DefinitionState.DOCS_ONLY,
    DefinitionState.DECLARATION,
    DefinitionState.SCAFFOLD,
    DefinitionState.SUBSTANTIVE_IMPLEMENTATION,
)
_INTEGRATION_ORDER = (
    IntegrationState.ISOLATED,
    IntegrationState.IMPORTABLE,
    IntegrationState.STATICALLY_WIRED,
    IntegrationState.DYNAMICALLY_REACHABLE,
)
_VERIFICATION_ORDER = (
    VerificationState.NONE,
    VerificationState.TEST_DECLARED,
    VerificationState.TEST_PASSED,
)
_ARTIFACT_ORDER = (
    ArtifactState.NONE,
    ArtifactState.MANIFEST_ONLY,
    ArtifactState.BYTES_PRESENT,
    ArtifactState.HASH_VERIFIED,
)
_LEARNING_ORDER = (
    LearningState.NOT_APPLICABLE,
    LearningState.ARCHITECTURE_ONLY,
    LearningState.UNTRAINED,
    LearningState.TRAINED_UNVERIFIED,
    LearningState.TRAINED_VERIFIED,
)
_EVALUATION_ORDER = (
    EvaluationState.NONE,
    EvaluationState.MECHANIC_FIXTURE,
    EvaluationState.RETROSPECTIVE_CASE,
    EvaluationState.FROZEN_HOLDOUT,
    EvaluationState.PROSPECTIVE,
)
_RUNTIME_ORDER = (
    RuntimeState.NOT_OBSERVED,
    RuntimeState.UNAVAILABLE,
    RuntimeState.PROBE_FAILED,
    RuntimeState.RUNNING,
)

_PLACEHOLDER_BODIES = {
    "abstract",
    "declaration",
    "ellipsis",
    "empty",
    "not_implemented",
    "pass",
    "placeholder",
    "scaffold",
    "stub",
    "todo",
}
_NEGATIVE_TAGS = {
    "docs_only",
    "effectiveness_unknown",
    "not_current",
    "not_implemented",
    "not_served",
    "not_trained",
    "not_validated",
    "placeholder",
    "planned",
    "proposal",
    "scaffold",
    "target_design",
    "unavailable",
    "unimplemented",
    "untrained",
}
_MODEL_FORMATS = {
    "ckpt",
    "joblib",
    "onnx",
    "pickle",
    "pkl",
    "pt",
    "pth",
    "safetensors",
}
_MODEL_BASE_PATTERNS = (
    re.compile(r"(?<![A-Za-z0-9_])(?:torch\.)?nn\.Module(?![A-Za-z0-9_])", re.IGNORECASE),
    re.compile(r"(?<![A-Za-z0-9_])(?:tensorflow\.|tf\.)?keras\.Model(?![A-Za-z0-9_])", re.IGNORECASE),
    re.compile(r"(?<![A-Za-z0-9_])(?:flax\.linen|linen)\.Module(?![A-Za-z0-9_])", re.IGNORECASE),
    re.compile(r"(?<![A-Za-z0-9_])(?:pytorch_lightning\.|pl\.)?LightningModule(?![A-Za-z0-9_])", re.IGNORECASE),
    re.compile(r"(?<![A-Za-z0-9_])BaseEstimator(?![A-Za-z0-9_])", re.IGNORECASE),
)


def _strongest(values: Iterable[Any], order: tuple[Any, ...]) -> Any:
    positions = {value: index for index, value in enumerate(order)}
    return max(values, key=lambda value: positions[value], default=order[0])


def merge_axes(items: Iterable[AxisStates | ComponentFact | CapabilityFact]) -> AxisStates:
    """Merge independent axis observations without collapsing them to one label."""

    axes = tuple(item if isinstance(item, AxisStates) else item.axes for item in items)
    if not axes:
        return AxisStates()
    return AxisStates(
        definition=_strongest((item.definition for item in axes), _DEFINITION_ORDER),
        integration=_strongest((item.integration for item in axes), _INTEGRATION_ORDER),
        verification=_strongest((item.verification for item in axes), _VERIFICATION_ORDER),
        artifact=_strongest((item.artifact for item in axes), _ARTIFACT_ORDER),
        learning=_strongest((item.learning for item in axes), _LEARNING_ORDER),
        evaluation=_strongest((item.evaluation for item in axes), _EVALUATION_ORDER),
        runtime=_strongest((item.runtime for item in axes), _RUNTIME_ORDER),
    )


def _uid(kind: str, value: object) -> str:
    return f"{kind}:{canonical_sha256(value)}"


def _kind_value(value: object) -> str:
    return str(getattr(value, "value", value)).casefold()


def _capability_key(name: str) -> str:
    words = re.findall(r"[a-z0-9]+", name.casefold().replace("_", " ").replace("-", " "))
    return " ".join(words) or name.casefold()


def _evidence(
    *,
    subject_uid: str,
    kind: EvidenceKind,
    file: Any,
    observed_value: object,
    line: int | None = None,
) -> EvidenceFact:
    payload = {
        "subject_uid": subject_uid,
        "kind": kind.value,
        "source_file_uid": file.file_uid,
        "line": line,
        "observed_value": observed_value,
        "source_sha256": file.sha256,
    }
    return EvidenceFact(
        evidence_uid=_uid("evidence", payload),
        subject_uid=subject_uid,
        kind=kind,
        source_file_uid=file.file_uid,
        line_start=line,
        line_end=line,
        observed_value=observed_value,
        source_sha256=file.sha256,
    )


def _existing_support(discovery: Any) -> dict[str, tuple[str, ...]]:
    by_subject: dict[str, list[str]] = defaultdict(list)
    for fact in getattr(discovery, "evidence", ()):
        by_subject[fact.subject_uid].append(fact.evidence_uid)
    return {subject: tuple(sorted(values)) for subject, values in by_subject.items()}


def _is_model_architecture(symbol: Any) -> bool:
    if _kind_value(symbol.kind) != "class" or not symbol.signature:
        return False
    return any(pattern.search(str(symbol.signature)) for pattern in _MODEL_BASE_PATTERNS)


def _negative_model_markers(value: object) -> tuple[str, ...]:
    """Return explicit, non-inferential model restraint markers."""

    markers: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized_key = str(key).casefold()
            if normalized_key == "weights_loaded" and child is False:
                markers.add("weights_loaded_false")
            elif normalized_key == "experimental_only" and child is True:
                markers.add("experimental_only_true")
            elif normalized_key in {"learning", "training_status", "status"} and str(child).casefold() == "untrained":
                markers.add("untrained")
            markers.update(_negative_model_markers(child))
        return tuple(sorted(markers))
    if isinstance(value, (tuple, list, set, frozenset)):
        for child in value:
            markers.update(_negative_model_markers(child))
        return tuple(sorted(markers))
    text = str(value).casefold()
    compact = re.sub(r"[\s'\"]+", "", text)
    if re.search(r"\bweights_loaded\b[^\n,;}]{0,80}(?:=|:)\s*false\b", text) or re.search(
        r"weights_loaded[:=]false\b", compact
    ):
        markers.add("weights_loaded_false")
    if re.search(r"\bexperimental_only\b[^\n,;}]{0,80}(?:=|:)\s*true\b", text) or re.search(
        r"experimental_only[:=]true\b", compact
    ):
        markers.add("experimental_only_true")
    if re.search(r"\buntrained\b", text):
        markers.add("untrained")
    return tuple(sorted(markers))


def _model_refutations_by_file(discovery: Any) -> dict[str, tuple[tuple[str, tuple[str, ...]], ...]]:
    by_file: dict[str, list[tuple[str, tuple[str, ...]]]] = defaultdict(list)
    for fact in getattr(discovery, "evidence", ()):
        markers = _negative_model_markers(fact.observed_value)
        if markers:
            by_file[fact.source_file_uid].append((fact.evidence_uid, markers))
    return {file_uid: tuple(sorted(values)) for file_uid, values in by_file.items()}


def _related_verified_artifacts(symbol: Any, artifacts: tuple[ArtifactFact, ...]) -> tuple[ArtifactFact, ...]:
    related: list[ArtifactFact] = []
    identity_values = {symbol.symbol_uid, symbol.qualified_name}
    for artifact in artifacts:
        metadata_values = {
            str(artifact.safe_metadata.get(key))
            for key in ("architecture_uid", "model_symbol_uid", "subject_uid", "qualified_name")
            if key in artifact.safe_metadata
        }
        if artifact.verification is ArtifactState.HASH_VERIFIED and (
            artifact.manifest_for in identity_values or bool(identity_values & metadata_values)
        ):
            related.append(artifact)
    return tuple(sorted(related, key=lambda item: item.artifact_uid))


def _file_module(path: str) -> str:
    pure = PurePosixPath(path)
    parts = list(pure.with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _imported_file_uids(files: tuple[Any, ...], imports: Iterable[Any]) -> set[str]:
    module_to_uid = {_file_module(file.path): file.file_uid for file in files if file.path.endswith(".py")}
    imported: set[str] = set()
    for fact in imports:
        module = str(fact.module).lstrip(".")
        for candidate, file_uid in module_to_uid.items():
            if module == candidate or module.startswith(candidate + "."):
                imported.add(file_uid)
    return imported


def _component(
    *,
    snapshot_uid: str,
    component_kind: str,
    name: str,
    roots: tuple[str, ...],
    axes: AxisStates,
    supports: Iterable[str] = (),
    refutes: Iterable[str] = (),
    reasons: Iterable[str] = (),
) -> ComponentFact:
    payload = {
        "snapshot_uid": snapshot_uid,
        "component_kind": component_kind,
        "name": name,
        "roots": tuple(sorted(roots)),
    }
    return ComponentFact(
        component_uid=_uid("component", payload),
        component_kind=component_kind,
        name=name,
        root_subject_uids=roots,
        axes=axes,
        supporting_evidence_uids=tuple(supports),
        refuting_evidence_uids=tuple(refutes),
        reason_codes=tuple(reasons),
    )


def derive_components(
    snapshot: Any,
    files: Iterable[Any],
    discovery: Any,
    artifacts: Iterable[ArtifactFact],
) -> tuple[tuple[ComponentFact, ...], tuple[EvidenceFact, ...]]:
    """Derive conservative components from static facts and opaque artifacts.

    This MVP can observe declarations and static wiring.  It does not execute
    tests, probe services, or load model files, so it never emits test-passed,
    dynamically-reachable, running, or trained-verified states.
    """

    file_tuple = tuple(files)
    files_by_uid = {item.file_uid: item for item in file_tuple}
    artifact_tuple = tuple(artifacts)
    symbol_tuple = tuple(getattr(discovery, "symbols", ()))
    symbols_by_uid = {item.symbol_uid: item for item in symbol_tuple}
    support_by_subject = _existing_support(discovery)
    model_refutations = _model_refutations_by_file(discovery)
    imported_uids = _imported_file_uids(file_tuple, getattr(discovery, "imports", ()))
    entrypoint_symbols = {
        item.symbol_uid for item in getattr(discovery, "entrypoints", ()) if item.symbol_uid is not None
    }
    snapshot_uid = str(snapshot.snapshot_uid)
    components: list[ComponentFact] = []
    generated_evidence: list[EvidenceFact] = []

    for symbol in symbol_tuple:
        file = files_by_uid.get(symbol.file_uid)
        if file is None:
            continue
        body = str(symbol.body_classification).casefold()
        if body in _PLACEHOLDER_BODIES or any(marker in body for marker in _PLACEHOLDER_BODIES):
            definition = DefinitionState.SCAFFOLD
            reasons = ("placeholder_body",)
        elif body in {"substantive", "implementation", "implemented"}:
            definition = DefinitionState.SUBSTANTIVE_IMPLEMENTATION
            reasons = ()
        else:
            definition = DefinitionState.DECLARATION
            reasons = ("body_substance_unknown",)
        integration = IntegrationState.IMPORTABLE
        if symbol.file_uid in imported_uids or symbol.symbol_uid in entrypoint_symbols:
            integration = IntegrationState.STATICALLY_WIRED
        is_model = _is_model_architecture(symbol)
        refuting_evidence: tuple[str, ...] = ()
        if is_model:
            related_artifacts = _related_verified_artifacts(symbol, artifact_tuple)
            file_refutations = model_refutations.get(symbol.file_uid, ())
            refuting_evidence = tuple(uid for uid, _ in file_refutations)
            negative_codes = tuple(
                f"negative_marker:{marker}"
                for _, markers in file_refutations
                for marker in markers
            )
            if body in _PLACEHOLDER_BODIES or any(marker in body for marker in _PLACEHOLDER_BODIES):
                learning = LearningState.ARCHITECTURE_ONLY
            else:
                learning = LearningState.UNTRAINED
            artifact_state = ArtifactState.NONE
            roots = (symbol.symbol_uid,)
            if related_artifacts:
                artifact_state = ArtifactState.HASH_VERIFIED
                learning = LearningState.TRAINED_UNVERIFIED
                roots = (symbol.symbol_uid, *(item.artifact_uid for item in related_artifacts))
                reasons = (*reasons, "checkpoint_without_training_provenance")
            if file_refutations:
                learning = LearningState.UNTRAINED
                reasons = (*reasons, *negative_codes)
        else:
            learning = LearningState.NOT_APPLICABLE
            artifact_state = ArtifactState.NONE
            roots = (symbol.symbol_uid,)
        components.append(
            _component(
                snapshot_uid=snapshot_uid,
                component_kind="model" if is_model else "symbol",
                name=symbol.qualified_name,
                roots=roots,
                axes=AxisStates(
                    definition=definition,
                    integration=integration,
                    artifact=artifact_state,
                    learning=learning,
                ),
                supports=support_by_subject.get(symbol.symbol_uid, ()),
                refutes=refuting_evidence,
                reasons=reasons,
            )
        )

    for entrypoint in getattr(discovery, "entrypoints", ()):
        components.append(
            _component(
                snapshot_uid=snapshot_uid,
                component_kind="entrypoint",
                name=entrypoint.name,
                roots=(entrypoint.entrypoint_uid,),
                axes=AxisStates(
                    definition=DefinitionState.DECLARATION,
                    integration=IntegrationState.STATICALLY_WIRED,
                ),
                supports=support_by_subject.get(entrypoint.entrypoint_uid, ()),
            )
        )

    for test in getattr(discovery, "tests", ()):
        # Gherkin scenarios are specifications, not executable test results.
        # They are represented below with their target/not-current tags intact.
        if test.framework.casefold() == "gherkin":
            continue
        file = files_by_uid.get(test.file_uid)
        symbol = symbols_by_uid.get(test.symbol_uid)
        test_name = symbol.qualified_name if symbol is not None else (
            f"test:{file.path}:{test.line}" if file is not None else f"test:unknown:{test.line}"
        )
        evaluation = EvaluationState.MECHANIC_FIXTURE if test.fixture_only else EvaluationState.NONE
        components.append(
            _component(
                snapshot_uid=snapshot_uid,
                component_kind="test",
                name=test_name,
                roots=(test.test_uid,),
                axes=AxisStates(
                    definition=DefinitionState.DECLARATION,
                    integration=IntegrationState.ISOLATED,
                    verification=VerificationState.TEST_DECLARED,
                    evaluation=evaluation,
                ),
                supports=support_by_subject.get(test.test_uid, ()),
                reasons=("fixture_mechanics_only",) if test.fixture_only else (),
            )
        )

    for scenario in getattr(discovery, "scenarios", ()):
        file = files_by_uid.get(scenario.file_uid)
        if file is None:
            continue
        tags = {str(tag).casefold().lstrip("@") for tag in scenario.tags}
        negative_tags = tuple(sorted(tags & _NEGATIVE_TAGS))
        refuting: list[str] = []
        reasons: list[str] = ["specification_is_not_implementation"]
        if negative_tags:
            marker = _evidence(
                subject_uid=scenario.scenario_uid,
                kind=EvidenceKind.NEGATIVE_MARKER,
                file=file,
                line=scenario.line,
                observed_value={"tags": negative_tags},
            )
            generated_evidence.append(marker)
            refuting.append(marker.evidence_uid)
            reasons.extend(f"negative_marker:{tag}" for tag in negative_tags)
        components.append(
            _component(
                snapshot_uid=snapshot_uid,
                component_kind="specification",
                name=scenario.feature,
                roots=(scenario.scenario_uid,),
                axes=AxisStates(
                    definition=DefinitionState.DOCS_ONLY,
                    verification=VerificationState.TEST_DECLARED,
                ),
                supports=support_by_subject.get(scenario.scenario_uid, ()),
                refutes=refuting,
                reasons=reasons,
            )
        )

    for artifact in artifact_tuple:
        file = files_by_uid.get(artifact.file_uid)
        if file is None:
            continue
        format_name = artifact.format.casefold().split("-", 1)[0]
        is_model = format_name in _MODEL_FORMATS or PurePosixPath(file.path).suffix.casefold().lstrip(".") in _MODEL_FORMATS
        if is_model and artifact.verification in {ArtifactState.BYTES_PRESENT, ArtifactState.HASH_VERIFIED}:
            learning = LearningState.TRAINED_UNVERIFIED
            learning_reason = "checkpoint_without_training_provenance"
        elif is_model:
            learning = LearningState.ARCHITECTURE_ONLY
            learning_reason = "model_artifact_bytes_not_verified"
        else:
            learning = LearningState.NOT_APPLICABLE
            learning_reason = ""
        observed = {
            "format": artifact.format,
            "sha256": artifact.sha256,
            "verification": artifact.verification.value,
            "manifest_for": artifact.manifest_for,
        }
        fact_evidence = _evidence(
            subject_uid=artifact.artifact_uid,
            kind=(
                EvidenceKind.ARTIFACT_MANIFEST
                if artifact.manifest_for is not None or artifact.verification is ArtifactState.MANIFEST_ONLY
                else EvidenceKind.ARTIFACT_BYTES
            ),
            file=file,
            observed_value=observed,
        )
        generated_evidence.append(fact_evidence)
        components.append(
            _component(
                snapshot_uid=snapshot_uid,
                component_kind="artifact",
                name=PurePosixPath(file.path).stem,
                roots=(artifact.artifact_uid,),
                axes=AxisStates(
                    definition=DefinitionState.DECLARATION,
                    artifact=artifact.verification,
                    learning=learning,
                ),
                supports=(fact_evidence.evidence_uid,),
                reasons=tuple((*artifact.reason_codes, *((learning_reason,) if learning_reason else ()))),
            )
        )

    # File-level explicit negative states are retained even when discovery could
    # not parse the file.  This keeps unavailable/untrained facts first-class.
    for file in file_tuple:
        negatives = tuple(sorted(code for code in file.reason_codes if any(tag in code.casefold() for tag in _NEGATIVE_TAGS)))
        if not negatives:
            continue
        marker = _evidence(
            subject_uid=file.file_uid,
            kind=EvidenceKind.NEGATIVE_MARKER,
            file=file,
            observed_value={"reason_codes": negatives},
        )
        generated_evidence.append(marker)
        components.append(
            _component(
                snapshot_uid=snapshot_uid,
                component_kind="negative_marker",
                name=PurePosixPath(file.path).stem,
                roots=(file.file_uid,),
                axes=AxisStates(definition=DefinitionState.DOCS_ONLY),
                refutes=(marker.evidence_uid,),
                reasons=negatives,
            )
        )

    return (
        tuple(sorted(components, key=lambda item: item.component_uid)),
        tuple(sorted(generated_evidence, key=lambda item: item.evidence_uid)),
    )


def _cap_negative_axes(axes: AxisStates) -> AxisStates:
    """Apply the conservative ceiling imposed by an explicit negative marker."""

    definition = axes.definition
    if _DEFINITION_ORDER.index(definition) > _DEFINITION_ORDER.index(DefinitionState.SCAFFOLD):
        definition = DefinitionState.SCAFFOLD
    verification = axes.verification
    if verification is VerificationState.TEST_PASSED:
        verification = VerificationState.TEST_DECLARED
    learning = axes.learning
    if learning in {LearningState.TRAINED_UNVERIFIED, LearningState.TRAINED_VERIFIED}:
        learning = LearningState.UNTRAINED
    return AxisStates(
        definition=definition,
        integration=IntegrationState.ISOLATED,
        verification=verification,
        artifact=axes.artifact,
        learning=learning,
        evaluation=axes.evaluation,
        runtime=RuntimeState.NOT_OBSERVED,
    )


def derive_capabilities(
    components: Iterable[ComponentFact],
    evidence: Iterable[EvidenceFact],
) -> tuple[CapabilityFact, ...]:
    """Group like-named components and retain support/refutation conflicts."""

    component_tuple = tuple(components)
    evidence_by_uid = {fact.evidence_uid: fact for fact in evidence}
    groups: dict[str, list[ComponentFact]] = defaultdict(list)
    for component in component_tuple:
        groups[_capability_key(component.name)].append(component)

    capabilities: list[CapabilityFact] = []
    for key, group in sorted(groups.items()):
        supports = tuple(sorted({uid for item in group for uid in item.supporting_evidence_uids}))
        refutes = tuple(sorted({uid for item in group for uid in item.refuting_evidence_uids}))
        axes = merge_axes(group)
        contradictions: set[str] = set()
        if refutes:
            axes = _cap_negative_axes(axes)
            contradictions.add("explicit_negative_marker")
        if supports and refutes:
            contradictions.add("support_and_refutation_coexist")
        if any("placeholder_body" in item.reason_codes for item in group):
            contradictions.add("placeholder_caps_definition")
        if any("artifact_hash_mismatch" in item.reason_codes for item in group):
            contradictions.add("artifact_hash_mismatch")
        if axes.learning is LearningState.TRAINED_UNVERIFIED and axes.artifact is not ArtifactState.HASH_VERIFIED:
            contradictions.add("training_provenance_unverified")
        # Dangling IDs are contradictions, not silently dropped evidence.
        if any(uid not in evidence_by_uid for uid in (*supports, *refutes)):
            contradictions.add("evidence_reference_missing")
        capabilities.append(
            CapabilityFact(
                capability_uid=_uid(
                    "capability",
                    {"name": key, "component_uids": tuple(sorted(item.component_uid for item in group))},
                ),
                name=key,
                component_uids=tuple(item.component_uid for item in group),
                axes=axes,
                supporting_evidence_uids=supports,
                refuting_evidence_uids=refutes,
                contradiction_codes=tuple(sorted(contradictions)),
            )
        )
    return tuple(sorted(capabilities, key=lambda item: item.capability_uid))


__all__ = ["derive_capabilities", "derive_components", "merge_axes"]
