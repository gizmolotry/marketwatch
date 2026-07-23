"""Repository-wide static discovery orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable

from .canonical import canonical_sha256
from .domain import (
    EntrypointFact,
    EvidenceFact,
    EvidenceKind,
    FileFact,
    FileKind,
    ImportFact,
    RepositorySnapshot,
    SpecScenarioFact,
    SymbolFact,
    TestFact,
)
from .python_ast import parse_python_file


@dataclass(frozen=True)
class DiscoveryResult:
    symbols: tuple[SymbolFact, ...] = ()
    imports: tuple[ImportFact, ...] = ()
    entrypoints: tuple[EntrypointFact, ...] = ()
    tests: tuple[TestFact, ...] = ()
    scenarios: tuple[SpecScenarioFact, ...] = ()
    evidence: tuple[EvidenceFact, ...] = ()


_TAG = re.compile(r"^\s*((?:@[^\s@]+(?:\s+|$))+)")
_HEADING = re.compile(
    r"^\s*(Feature|Rule|Scenario(?: Outline)?|Scenario Template)\s*:\s*(.*?)\s*$",
    re.IGNORECASE,
)


def _kind_value(value: object) -> str:
    return str(getattr(value, "value", value)).casefold()


def _definition_evidence(
    *,
    subject_uid: str,
    file: FileFact,
    line_start: int | None,
    line_end: int | None,
    observed_value: str,
    kind: EvidenceKind,
) -> EvidenceFact:
    payload = {
        "subject_uid": subject_uid,
        "kind": kind.value,
        "source_file_uid": file.file_uid,
        "line_start": line_start,
        "line_end": line_end,
        "observed_value": observed_value,
        "source_sha256": file.sha256,
    }
    return EvidenceFact(
        evidence_uid=canonical_sha256(payload),
        subject_uid=subject_uid,
        kind=kind,
        source_file_uid=file.file_uid,
        line_start=line_start,
        line_end=line_end,
        observed_value=observed_value,
        source_sha256=file.sha256,
    )


def parse_gherkin(path: str | Path, file_fact: FileFact) -> tuple[SpecScenarioFact, ...]:
    """Extract Gherkin feature/rule/scenario structure without a runtime parser."""

    if file_fact.sensitive:
        return ()
    target = Path(path)
    if target.is_symlink() or not target.is_file():
        return ()
    try:
        lines = target.read_text(encoding="utf-8-sig").splitlines()
    except (OSError, UnicodeError):
        return ()

    pending_tags: tuple[str, ...] = ()
    feature_tags: tuple[str, ...] = ()
    rule_tags: tuple[str, ...] = ()
    feature: str | None = None
    rule: str | None = None
    scenarios: list[SpecScenarioFact] = []
    for line_number, text in enumerate(lines, start=1):
        tag_match = _TAG.match(text)
        if tag_match:
            line_tags = tuple(part[1:] for part in tag_match.group(1).split() if part.startswith("@"))
            pending_tags = tuple(dict.fromkeys((*pending_tags, *line_tags)))
            continue
        heading = _HEADING.match(text)
        if not heading:
            if text.strip() and not text.lstrip().startswith("#"):
                # Tags only attach to the immediately following tagged heading.
                pending_tags = ()
            continue
        kind = heading.group(1).casefold()
        name = heading.group(2)
        if kind == "feature":
            feature = name
            rule = None
            feature_tags = pending_tags
            rule_tags = ()
        elif kind == "rule":
            rule = name
            rule_tags = pending_tags
        else:
            inherited = (*feature_tags, *rule_tags, *pending_tags)
            tags = tuple(dict.fromkeys(inherited))
            payload = {
                "file_uid": file_fact.file_uid,
                "feature": feature,
                "rule": rule,
                "scenario": name,
                "tags": tags,
                "line": line_number,
            }
            if feature is None:
                pending_tags = ()
                continue
            scenarios.append(
                SpecScenarioFact(
                    scenario_uid=canonical_sha256(payload),
                    file_uid=file_fact.file_uid,
                    feature=feature or "",
                    rule=rule,
                    scenario=name,
                    tags=tags,
                    line=line_number,
                )
            )
        pending_tags = ()
    return tuple(scenarios)


def _file_map(files: Iterable[FileFact]) -> dict[str, FileFact]:
    return {item.file_uid: item for item in files}


def inventory_tests(
    symbols: Iterable[SymbolFact],
    files: Iterable[FileFact],
    scenarios: Iterable[SpecScenarioFact],
) -> tuple[TestFact, ...]:
    """Build a mechanical inventory of test functions/classes and scenarios."""

    files_by_uid = _file_map(files)
    symbol_tuple = tuple(symbols)
    unittest_classes = {
        symbol.qualified_name
        for symbol in symbol_tuple
        if _kind_value(symbol.kind) == "class" and symbol.signature and "unittest.TestCase" in symbol.signature
    }
    discovered: list[TestFact] = []
    for symbol in symbol_tuple:
        file = files_by_uid.get(symbol.file_uid)
        if file is None:
            continue
        qualified_parts = symbol.qualified_name.split(".")
        leaf = qualified_parts[-1]
        in_test_file = _kind_value(file.kind) == "test" or Path(file.path).name.startswith("test_")
        symbol_kind = _kind_value(symbol.kind)
        is_test = leaf.startswith("test_") or (symbol_kind == "class" and leaf.startswith("Test"))
        decorators = tuple(str(item) for item in symbol.decorators)
        fixture_only = any(item.rsplit(".", 1)[-1] == "fixture" for item in decorators)
        if not (in_test_file and (is_test or fixture_only)):
            continue
        framework = "pytest"
        if (symbol.signature and "unittest.TestCase" in symbol.signature) or any(
            ".".join(qualified_parts[:index]) in unittest_classes for index in range(1, len(qualified_parts))
        ):
            framework = "unittest"
        target_hints = tuple(
            dict.fromkeys(
                part
                for part in (*decorators, *qualified_parts[:-1])
                if part and part not in {"pytest.fixture", "fixture"}
            )
        )
        payload = {
            "file_uid": file.file_uid,
            "symbol_uid": symbol.symbol_uid,
            "framework": framework,
            "target_hints": target_hints,
            "fixture_only": fixture_only,
            "line": symbol.line_start,
        }
        discovered.append(
            TestFact(
                test_uid=canonical_sha256(payload),
                file_uid=file.file_uid,
                symbol_uid=symbol.symbol_uid,
                framework=framework,
                target_hints=target_hints,
                fixture_only=fixture_only,
                line=symbol.line_start,
            )
        )

    for scenario in scenarios:
        payload = {
            "file_uid": scenario.file_uid,
            "symbol_uid": None,
            "framework": "gherkin",
            "target_hints": scenario.tags,
            "fixture_only": False,
            "line": scenario.line,
            "scenario_uid": scenario.scenario_uid,
        }
        discovered.append(
            TestFact(
                test_uid=canonical_sha256(payload),
                file_uid=scenario.file_uid,
                symbol_uid=None,
                framework="gherkin",
                target_hints=scenario.tags,
                fixture_only=False,
                line=scenario.line,
            )
        )
    return tuple(sorted(discovered, key=lambda item: (item.file_uid, item.line, item.test_uid)))


def _fact_evidence(
    files: dict[str, FileFact],
    symbols: Iterable[SymbolFact],
    imports: Iterable[ImportFact],
    entrypoints: Iterable[EntrypointFact],
    tests: Iterable[TestFact],
    scenarios: Iterable[SpecScenarioFact],
) -> tuple[EvidenceFact, ...]:
    evidence: list[EvidenceFact] = []
    for symbol in symbols:
        file = files[symbol.file_uid]
        evidence.append(
            _definition_evidence(
                subject_uid=symbol.symbol_uid,
                file=file,
                line_start=symbol.line_start,
                line_end=symbol.line_end,
                observed_value=f"{_kind_value(symbol.kind)} {symbol.qualified_name}",
                kind=EvidenceKind.AST_SYMBOL,
            )
        )
    for imported in imports:
        file = files[imported.file_uid]
        evidence.append(
            _definition_evidence(
                subject_uid=imported.import_uid,
                file=file,
                line_start=imported.line,
                line_end=imported.line,
                observed_value=f"import {imported.module}: {', '.join(imported.names)}",
                kind=EvidenceKind.AST_IMPORT,
            )
        )
    for entrypoint in entrypoints:
        file = files[entrypoint.file_uid]
        evidence.append(
            _definition_evidence(
                subject_uid=entrypoint.entrypoint_uid,
                file=file,
                line_start=entrypoint.line,
                line_end=entrypoint.line,
                observed_value=f"{_kind_value(entrypoint.kind)} {entrypoint.name}",
                kind=EvidenceKind.ENTRYPOINT,
            )
        )
    for test in tests:
        file = files[test.file_uid]
        evidence.append(
            _definition_evidence(
                subject_uid=test.test_uid,
                file=file,
                line_start=test.line,
                line_end=test.line,
                observed_value=f"{test.framework} test",
                kind=EvidenceKind.TEST_DECLARATION,
            )
        )
    for scenario in scenarios:
        file = files[scenario.file_uid]
        evidence.append(
            _definition_evidence(
                subject_uid=scenario.scenario_uid,
                file=file,
                line_start=scenario.line,
                line_end=scenario.line,
                observed_value=f"gherkin scenario {scenario.scenario}",
                kind=EvidenceKind.GHERKIN_TAG,
            )
        )
    return tuple(evidence)


def discover_repository(
    root: str | Path,
    snapshot: RepositorySnapshot,
    files: Iterable[FileFact],
) -> DiscoveryResult:
    """Discover Python and Gherkin repository structure deterministically."""

    del snapshot  # The caller binds this result to the supplied immutable snapshot.
    repo = Path(root).resolve()
    file_tuple = tuple(files)
    symbols: list[SymbolFact] = []
    imports: list[ImportFact] = []
    entrypoints: list[EntrypointFact] = []
    scenarios: list[SpecScenarioFact] = []
    parse_evidence: list[EvidenceFact] = []

    for file in sorted(file_tuple, key=lambda item: item.path):
        if file.sensitive:
            continue
        kind = _kind_value(file.kind)
        if kind in {"source", "test"} and file.path.casefold().endswith(".py"):
            file_symbols, file_imports, file_entrypoints, file_evidence = parse_python_file(repo, file)
            symbols.extend(file_symbols)
            imports.extend(file_imports)
            entrypoints.extend(file_entrypoints)
            parse_evidence.extend(file_evidence)
        elif kind == "spec" or file.path.casefold().endswith(".feature"):
            target = (repo / file.path).resolve()
            try:
                target.relative_to(repo)
            except ValueError:
                continue
            scenarios.extend(parse_gherkin(target, file))

    symbol_tuple = tuple(sorted(symbols, key=lambda item: (item.file_uid, item.line_start, item.qualified_name)))
    import_tuple = tuple(sorted(imports, key=lambda item: (item.file_uid, item.line, item.module, item.names)))
    entrypoint_tuple = tuple(sorted(entrypoints, key=lambda item: (item.file_uid, item.line, item.name)))
    scenario_tuple = tuple(sorted(scenarios, key=lambda item: (item.file_uid, item.line, item.scenario)))
    test_tuple = inventory_tests(symbol_tuple, file_tuple, scenario_tuple)
    files_by_uid = _file_map(file_tuple)
    generated_evidence = _fact_evidence(files_by_uid, symbol_tuple, import_tuple, entrypoint_tuple, test_tuple, scenario_tuple)
    evidence_tuple = tuple(
        sorted((*parse_evidence, *generated_evidence), key=lambda item: (item.source_file_uid, item.line_start or 0, item.evidence_uid))
    )
    return DiscoveryResult(
        symbols=symbol_tuple,
        imports=import_tuple,
        entrypoints=entrypoint_tuple,
        tests=test_tuple,
        scenarios=scenario_tuple,
        evidence=evidence_tuple,
    )


def discover_declared_entrypoints(*_: object, **__: object) -> tuple[EntrypointFact, ...]:
    """Reserved for declared packaging entry points; static AST entry points are returned today."""

    return ()
