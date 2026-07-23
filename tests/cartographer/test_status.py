"""Mechanics-only fixtures for conservative multi-axis status derivation."""

from __future__ import annotations

from types import SimpleNamespace

from repo_cartographer.domain import (
    ArtifactFact,
    ArtifactState,
    AxisStates,
    ComponentFact,
    DefinitionState,
    EvaluationState,
    EvidenceFact,
    EvidenceKind,
    FileFact,
    FileKind,
    GitState,
    IntegrationState,
    LearningState,
    RepositorySnapshot,
    RuntimeState,
    SpecScenarioFact,
    SymbolFact,
    SymbolKind,
    TestFact as DomainTestFact,
    TrackedState,
    VerificationState,
)
from repo_cartographer.status import derive_capabilities, derive_components, merge_axes


_HASH = "a" * 64


def _snapshot() -> RepositorySnapshot:
    return RepositorySnapshot(
        snapshot_uid="snapshot:fixture",
        root_display="fixture-repository",
        git=GitState(
            head_commit="1" * 40,
            branch="fixture",
            index_tree="2" * 40,
            dirty=False,
        ),
        config_sha256="b" * 64,
        scanner_version="fixture/1",
        files_root_sha256="c" * 64,
    )


def _file(uid: str, path: str, kind: FileKind, *, reasons=()) -> FileFact:
    return FileFact(
        file_uid=uid,
        path=path,
        kind=kind,
        tracked_state=TrackedState.TRACKED,
        byte_size=1,
        sha256=_HASH,
        language="python" if path.endswith(".py") else None,
        sensitive=False,
        parse_status="complete",
        reason_codes=tuple(reasons),
    )


def _discovery(**overrides):
    values = {
        "symbols": (),
        "imports": (),
        "entrypoints": (),
        "tests": (),
        "scenarios": (),
        "evidence": (),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_merge_axes_preserves_independent_strongest_observations():
    merged = merge_axes(
        (
            AxisStates(definition=DefinitionState.SCAFFOLD, artifact=ArtifactState.BYTES_PRESENT),
            AxisStates(
                definition=DefinitionState.SUBSTANTIVE_IMPLEMENTATION,
                verification=VerificationState.TEST_DECLARED,
                evaluation=EvaluationState.MECHANIC_FIXTURE,
            ),
        )
    )

    assert merged.definition is DefinitionState.SUBSTANTIVE_IMPLEMENTATION
    assert merged.artifact is ArtifactState.BYTES_PRESENT
    assert merged.verification is VerificationState.TEST_DECLARED
    assert merged.evaluation is EvaluationState.MECHANIC_FIXTURE


def test_placeholder_body_is_capped_at_scaffold():
    source = _file("file:source", "package/service.py", FileKind.SOURCE)
    symbol = SymbolFact(
        symbol_uid="symbol:service",
        file_uid=source.file_uid,
        qualified_name="package.service.run",
        kind=SymbolKind.FUNCTION,
        line_start=1,
        line_end=2,
        signature="()",
        body_classification="placeholder",
    )
    evidence = EvidenceFact(
        evidence_uid="evidence:symbol",
        subject_uid=symbol.symbol_uid,
        kind=EvidenceKind.AST_SYMBOL,
        source_file_uid=source.file_uid,
        line_start=1,
        line_end=2,
        observed_value="function declaration",
        source_sha256=source.sha256,
    )

    components, _ = derive_components(
        _snapshot(), (source,), _discovery(symbols=(symbol,), evidence=(evidence,)), ()
    )

    assert components[0].axes.definition is DefinitionState.SCAFFOLD
    assert "placeholder_body" in components[0].reason_codes
    assert components[0].axes.runtime is RuntimeState.NOT_OBSERVED


def test_target_design_spec_is_docs_only_and_refuting():
    spec = _file("file:spec", "specs/future.feature", FileKind.SPEC)
    scenario = SpecScenarioFact(
        scenario_uid="scenario:future",
        file_uid=spec.file_uid,
        feature="Future serving",
        rule=None,
        scenario="Serve a trained model",
        tags=("target_design", "not_current"),
        line=3,
    )

    components, evidence = derive_components(
        _snapshot(), (spec,), _discovery(scenarios=(scenario,)), ()
    )

    component = components[0]
    assert component.axes.definition is DefinitionState.DOCS_ONLY
    assert component.axes.verification is VerificationState.TEST_DECLARED
    assert component.refuting_evidence_uids
    assert evidence[0].kind is EvidenceKind.NEGATIVE_MARKER


def test_checkpoint_without_provenance_never_becomes_trained_verified_or_running():
    checkpoint_file = _file("file:model", "artifacts/candidate.pt", FileKind.ARTIFACT)
    artifact = ArtifactFact(
        artifact_uid="artifact:model",
        file_uid=checkpoint_file.file_uid,
        format="pt",
        sha256=checkpoint_file.sha256,
        manifest_for=None,
        expected_sha256=checkpoint_file.sha256,
        verification=ArtifactState.HASH_VERIFIED,
        safe_metadata={"byte_size": 1},
    )

    components, evidence = derive_components(
        _snapshot(), (checkpoint_file,), _discovery(), (artifact,)
    )

    assert components[0].axes.learning is LearningState.TRAINED_UNVERIFIED
    assert components[0].axes.runtime is RuntimeState.NOT_OBSERVED
    assert components[0].axes.evaluation is EvaluationState.NONE
    assert evidence[0].kind is EvidenceKind.ARTIFACT_BYTES


def test_capability_retains_contradiction_and_applies_negative_ceiling():
    support = EvidenceFact(
        evidence_uid="evidence:support",
        subject_uid="symbol:capability",
        kind=EvidenceKind.AST_SYMBOL,
        source_file_uid="file:source",
        line_start=1,
        line_end=2,
        observed_value="substantive body",
        source_sha256=_HASH,
    )
    negative = EvidenceFact(
        evidence_uid="evidence:negative",
        subject_uid="scenario:capability",
        kind=EvidenceKind.NEGATIVE_MARKER,
        source_file_uid="file:spec",
        line_start=3,
        line_end=3,
        observed_value={"tags": ("not_current",)},
        source_sha256=_HASH,
    )
    implemented = ComponentFact(
        component_uid="component:implemented",
        component_kind="symbol",
        name="Future serving",
        root_subject_uids=("symbol:capability",),
        axes=AxisStates(
            definition=DefinitionState.SUBSTANTIVE_IMPLEMENTATION,
            integration=IntegrationState.STATICALLY_WIRED,
            learning=LearningState.TRAINED_UNVERIFIED,
        ),
        supporting_evidence_uids=(support.evidence_uid,),
    )
    target = ComponentFact(
        component_uid="component:target",
        component_kind="specification",
        name="future_serving",
        root_subject_uids=("scenario:capability",),
        axes=AxisStates(definition=DefinitionState.DOCS_ONLY),
        refuting_evidence_uids=(negative.evidence_uid,),
    )

    capability = derive_capabilities((implemented, target), (support, negative))[0]

    assert capability.axes.definition is DefinitionState.SCAFFOLD
    assert capability.axes.integration is IntegrationState.ISOLATED
    assert capability.axes.learning is LearningState.UNTRAINED
    assert "explicit_negative_marker" in capability.contradiction_codes
    assert "support_and_refutation_coexist" in capability.contradiction_codes
    assert capability.supporting_evidence_uids == (support.evidence_uid,)
    assert capability.refuting_evidence_uids == (negative.evidence_uid,)


def test_marketwatch_neural_class_is_an_explicitly_untrained_model_architecture():
    source = _file("file:neural", "marketleak/multimodal/neural.py", FileKind.SOURCE)
    symbol = SymbolFact(
        symbol_uid="symbol:experimental-event-space",
        file_uid=source.file_uid,
        qualified_name="ExperimentalEventSpace",
        kind=SymbolKind.CLASS,
        line_start=182,
        line_end=312,
        signature="class ExperimentalEventSpace(nn.Module)",
        body_classification="substantive",
    )
    definition = EvidenceFact(
        evidence_uid="evidence:model-definition",
        subject_uid=symbol.symbol_uid,
        kind=EvidenceKind.AST_SYMBOL,
        source_file_uid=source.file_uid,
        line_start=182,
        line_end=312,
        observed_value="class ExperimentalEventSpace(nn.Module)",
        source_sha256=source.sha256,
    )
    restraint = EvidenceFact(
        evidence_uid="evidence:model-restraint",
        subject_uid=source.file_uid,
        kind=EvidenceKind.CONFIG_VALUE,
        source_file_uid=source.file_uid,
        line_start=120,
        line_end=121,
        observed_value={"weights_loaded": False, "experimental_only": True, "training_status": "untrained"},
        source_sha256=source.sha256,
    )

    components, _ = derive_components(
        _snapshot(),
        (source,),
        _discovery(symbols=(symbol,), evidence=(definition, restraint)),
        (),
    )

    model = next(item for item in components if item.root_subject_uids == (symbol.symbol_uid,))
    assert model.component_kind == "model"
    assert model.axes.definition is DefinitionState.SUBSTANTIVE_IMPLEMENTATION
    assert model.axes.learning is LearningState.UNTRAINED
    assert model.axes.learning is not LearningState.NOT_APPLICABLE
    assert model.axes.artifact is ArtifactState.NONE
    assert model.axes.runtime is RuntimeState.NOT_OBSERVED
    assert model.refuting_evidence_uids == (restraint.evidence_uid,)
    assert "negative_marker:weights_loaded_false" in model.reason_codes
    assert "negative_marker:experimental_only_true" in model.reason_codes
    assert "negative_marker:untrained" in model.reason_codes


def test_test_component_names_use_symbol_then_readable_path_fallback():
    test_file = _file("file:test-status", "tests/cartographer/test_status.py", FileKind.TEST)
    test_symbol = SymbolFact(
        symbol_uid="symbol:test-readable",
        file_uid=test_file.file_uid,
        qualified_name="TestStatus.test_readable_name",
        kind=SymbolKind.METHOD,
        line_start=40,
        line_end=42,
        signature="def test_readable_name(self)",
        body_classification="substantive",
    )
    mapped = DomainTestFact(
        test_uid="test:opaque-mapped-uid",
        file_uid=test_file.file_uid,
        symbol_uid=test_symbol.symbol_uid,
        framework="pytest",
        target_hints=(),
        fixture_only=False,
        line=40,
    )
    fallback = DomainTestFact(
        test_uid="test:opaque-fallback-uid",
        file_uid=test_file.file_uid,
        symbol_uid=None,
        framework="pytest",
        target_hints=(),
        fixture_only=False,
        line=55,
    )

    components, _ = derive_components(
        _snapshot(),
        (test_file,),
        _discovery(symbols=(test_symbol,), tests=(mapped, fallback)),
        (),
    )
    names = {item.name for item in components if item.component_kind == "test"}

    assert "TestStatus.test_readable_name" in names
    assert "test:tests/cartographer/test_status.py:55" in names
    assert mapped.test_uid not in names
    assert fallback.test_uid not in names
