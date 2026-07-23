"""Static Python discovery without importing repository code."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
import tokenize
from typing import Any

from .canonical import canonical_sha256
from .domain import (
    EntrypointFact,
    EntrypointKind,
    EvidenceFact,
    EvidenceKind,
    FileFact,
    ImportFact,
    SymbolFact,
    SymbolKind,
)


_SENSITIVE_NAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "credentials.json",
    "secrets.json",
    "service-account.json",
}
_SENSITIVE_SUFFIXES = {".key", ".pem", ".p12", ".pfx", ".jks", ".keystore"}


def is_sensitive_path(path: str | Path) -> bool:
    """Return whether *path* must not be opened by source discovery.

    This is intentionally conservative and independent of file contents: deciding
    whether a file is sensitive must never require reading that file first.
    """

    candidate = Path(str(path).replace("\\", "/"))
    lowered_parts = tuple(part.casefold() for part in candidate.parts)
    name = candidate.name.casefold()
    return (
        name in _SENSITIVE_NAMES
        or name.startswith(".env.")
        or candidate.suffix.casefold() in _SENSITIVE_SUFFIXES
        or any(part in {"secrets", "credentials", ".secrets"} for part in lowered_parts)
    )


def _qualified_name(expr: ast.AST | None) -> str | None:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        parent = _qualified_name(expr.value)
        return f"{parent}.{expr.attr}" if parent else expr.attr
    if isinstance(expr, ast.Call):
        return _qualified_name(expr.func)
    return None


def _literal_string(expr: ast.AST | None) -> str | None:
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return expr.value
    return None


@dataclass(frozen=True)
class _SymbolData:
    name: str
    qualified_name: str
    kind: str
    line: int
    end_line: int | None
    decorators: tuple[str, ...]
    signature: str | None
    body_classification: str


@dataclass(frozen=True)
class _ImportData:
    module: str
    names: tuple[str, ...]
    level: int
    line: int


@dataclass(frozen=True)
class _EntryData:
    kind: str
    target: str
    line: int
    method: str | None = None
    path: str | None = None


class _PythonVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.scope: list[str] = []
        self.scope_kinds: list[str] = []
        self.symbols: list[_SymbolData] = []
        self.imports: list[_ImportData] = []
        self.entrypoints: list[_EntryData] = []
        self.fastapi_apps: set[str] = set()
        self.argparse_parsers: set[str] = set()

    def _add_symbol(self, node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef, kind: str) -> None:
        qualified = ".".join((*self.scope, node.name))
        decorators = tuple(filter(None, (_qualified_name(item) for item in node.decorator_list)))
        self.symbols.append(
            _SymbolData(
                name=node.name,
                qualified_name=qualified,
                kind=kind,
                line=node.lineno,
                end_line=getattr(node, "end_lineno", None),
                decorators=decorators,
                signature=_signature(node),
                body_classification=_body_classification(node.body),
            )
        )

        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            route = _qualified_name(decorator.func)
            if not route or route.split(".", 1)[0] not in self.fastapi_apps or route.rsplit(".", 1)[-1].casefold() not in {
                "get", "post", "put", "patch", "delete", "options", "head", "trace", "route", "api_route", "websocket"
            }:
                continue
            route_path = _literal_string(decorator.args[0]) if decorator.args else None
            self.entrypoints.append(
                _EntryData("api_route", qualified, node.lineno, route.rsplit(".", 1)[-1].upper(), route_path)
            )

    def visit_ClassDef(self, node: ast.ClassDef) -> Any:
        self._add_symbol(node, "class")
        self.scope.append(node.name)
        self.scope_kinds.append("class")
        self.generic_visit(node)
        self.scope.pop()
        self.scope_kinds.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        self._visit_function(node, "function")

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        self._visit_function(node, "async_function")

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef, kind: str) -> None:
        effective_kind = (
            "async_method" if kind == "async_function" else "method"
        ) if self.scope_kinds and self.scope_kinds[-1] == "class" else kind
        self._add_symbol(node, effective_kind)
        self.scope.append(node.name)
        self.scope_kinds.append("function")
        self.generic_visit(node)
        self.scope.pop()
        self.scope_kinds.pop()

    def visit_Import(self, node: ast.Import) -> Any:
        for alias in node.names:
            name = f"{alias.name} as {alias.asname}" if alias.asname else alias.name
            self.imports.append(_ImportData(alias.name, (name,), 0, node.lineno))

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        module = node.module or "."
        names = tuple(f"{alias.name} as {alias.asname}" if alias.asname else alias.name for alias in node.names)
        self.imports.append(_ImportData(module, names, node.level, node.lineno))

    def visit_Assign(self, node: ast.Assign) -> Any:
        if isinstance(node.value, ast.Call):
            constructor = _qualified_name(node.value.func)
            if constructor and constructor.rsplit(".", 1)[-1] in {"FastAPI", "APIRouter"}:
                for target in node.targets:
                    target_name = _qualified_name(target)
                    if target_name:
                        self.fastapi_apps.add(target_name)
                        self.entrypoints.append(_EntryData("application", target_name, node.lineno))
            if constructor and constructor.rsplit(".", 1)[-1] == "ArgumentParser":
                for target in node.targets:
                    target_name = _qualified_name(target)
                    if target_name:
                        self.argparse_parsers.add(target_name)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> Any:
        call = _qualified_name(node.func)
        leaf = call.rsplit(".", 1)[-1] if call else ""
        if leaf == "ArgumentParser":
            self.entrypoints.append(_EntryData("cli", call or leaf, node.lineno))
        elif leaf in {"parse_args", "parse_known_args"} and (call or "").split(".", 1)[0] in self.argparse_parsers:
            self.entrypoints.append(_EntryData("cli", call or leaf, node.lineno))
        elif leaf == "add_argument" and (call or "").split(".", 1)[0] in self.argparse_parsers:
            option = _literal_string(node.args[0]) if node.args else None
            self.entrypoints.append(_EntryData("cli", option or call or leaf, node.lineno))
        self.generic_visit(node)

    def visit_If(self, node: ast.If) -> Any:
        if _is_main_guard(node.test):
            targets: list[str] = []
            for child in node.body:
                if isinstance(child, ast.Expr) and isinstance(child.value, ast.Call):
                    target = _qualified_name(child.value.func)
                    if target:
                        targets.append(target)
            self.entrypoints.append(
                _EntryData("script", targets[0] if targets else "__main__", node.lineno)
            )
        self.generic_visit(node)


def _is_main_guard(expr: ast.AST) -> bool:
    if not isinstance(expr, ast.Compare) or len(expr.ops) != 1 or len(expr.comparators) != 1:
        return False
    if not isinstance(expr.ops[0], ast.Eq):
        return False
    left, right = expr.left, expr.comparators[0]
    return (
        isinstance(left, ast.Name)
        and left.id == "__name__"
        and _literal_string(right) == "__main__"
    ) or (
        isinstance(right, ast.Name)
        and right.id == "__name__"
        and _literal_string(left) == "__main__"
    )


def _signature(node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    if isinstance(node, ast.ClassDef):
        bases = ", ".join(ast.unparse(base) for base in node.bases)
        return f"class {node.name}({bases})" if bases else f"class {node.name}"
    prefix = "async def" if isinstance(node, ast.AsyncFunctionDef) else "def"
    result = f"{prefix} {node.name}({ast.unparse(node.args)})"
    if node.returns is not None:
        result += f" -> {ast.unparse(node.returns)}"
    return result


def _body_classification(body: list[ast.stmt]) -> str:
    meaningful = [item for item in body if not (isinstance(item, ast.Expr) and isinstance(item.value, ast.Constant) and isinstance(item.value.value, str))]
    if not meaningful or all(isinstance(item, ast.Pass) for item in meaningful):
        return "scaffold"
    if all(
        isinstance(item, ast.Expr) and isinstance(item.value, ast.Constant) and item.value.value is Ellipsis
        or isinstance(item, ast.Raise) and _qualified_name(item.exc) in {"NotImplementedError", "builtins.NotImplementedError"}
        for item in meaningful
    ):
        return "scaffold"
    return "substantive"


def _evidence(fact: FileFact, line: int, message: str) -> EvidenceFact:
    payload = {
        "subject_uid": fact.file_uid,
        "kind": "definition",
        "source_file_uid": fact.file_uid,
        "line_start": line,
        "line_end": line,
        "observed_value": message,
        "source_sha256": fact.sha256,
    }
    return EvidenceFact(
        evidence_uid=canonical_sha256(payload),
        subject_uid=fact.file_uid,
        kind=EvidenceKind.NEGATIVE_MARKER,
        source_file_uid=fact.file_uid,
        line_start=line,
        line_end=line,
        observed_value=message,
        source_sha256=fact.sha256,
    )


def parse_python_file(
    root: str | Path,
    fact: FileFact,
) -> tuple[tuple[SymbolFact, ...], tuple[ImportFact, ...], tuple[EntrypointFact, ...], tuple[EvidenceFact, ...]]:
    """Parse one Python file statically.

    The target module is never imported or executed. Syntax/decoding failures are
    returned as evidence so one malformed file cannot abort a repository scan.
    """

    root_path = Path(root).resolve()
    relative = fact.path
    if fact.sensitive or is_sensitive_path(relative):
        return (), (), (), (_evidence(fact, 1, "sensitive file skipped without reading"),)

    candidate = root_path / relative
    if candidate.is_symlink():
        return (), (), (), (_evidence(fact, 1, "file unavailable or symlinked; skipped"),)
    target = candidate.resolve()
    try:
        target.relative_to(root_path)
    except ValueError:
        return (), (), (), (_evidence(fact, 1, "path resolves outside repository; skipped"),)
    if target.is_symlink() or not target.is_file():
        return (), (), (), (_evidence(fact, 1, "file unavailable or symlinked; skipped"),)

    try:
        with tokenize.open(target) as handle:
            source = handle.read()
        tree = ast.parse(source, filename=relative, type_comments=True)
    except (OSError, UnicodeError, SyntaxError) as exc:
        line = getattr(exc, "lineno", None)
        return (), (), (), (_evidence(fact, int(line or 1), f"python parse failed: {type(exc).__name__}"),)

    visitor = _PythonVisitor()
    visitor.visit(tree)

    module_name = relative.removesuffix(".py").replace("/", ".")
    if module_name.endswith(".__init__"):
        module_name = module_name.removesuffix(".__init__")
    module_name = module_name or "__init__"
    module_line_end = max((getattr(node, "end_lineno", 1) or 1 for node in tree.body), default=1)
    module = SymbolFact(
        symbol_uid=canonical_sha256({"file_uid": fact.file_uid, "qualified_name": module_name, "kind": "module", "line": 1}),
        file_uid=fact.file_uid,
        qualified_name=module_name,
        kind=SymbolKind.MODULE,
        line_start=1,
        line_end=module_line_end,
        signature=None,
        decorators=(),
        body_classification=_body_classification(tree.body),
    )
    defined_symbols = tuple(
        SymbolFact(
            symbol_uid=canonical_sha256(
                {"file_uid": fact.file_uid, "qualified_name": item.qualified_name, "kind": item.kind, "line": item.line}
            ),
            file_uid=fact.file_uid,
            qualified_name=item.qualified_name,
            kind=(
                SymbolKind.CLASS if item.kind == "class"
                else SymbolKind.ASYNC_METHOD if item.kind == "async_method"
                else SymbolKind.METHOD if item.kind == "method"
                else SymbolKind.ASYNC_FUNCTION if item.kind == "async_function"
                else SymbolKind.FUNCTION
            ),
            line_start=item.line,
            line_end=item.end_line or item.line,
            signature=item.signature,
            decorators=item.decorators,
            body_classification=item.body_classification,
        )
        for item in sorted(visitor.symbols, key=lambda value: (value.line, value.qualified_name))
    )
    symbols = (module, *defined_symbols)
    imports = tuple(
        ImportFact(
            import_uid=canonical_sha256(
                {"file_uid": fact.file_uid, "module": item.module, "names": item.names, "level": item.level, "line": item.line}
            ),
            file_uid=fact.file_uid,
            module=item.module,
            names=item.names,
            level=item.level,
            line=item.line,
        )
        for item in sorted(visitor.imports, key=lambda value: (value.line, value.module, value.names))
    )
    symbols_by_name = {item.qualified_name: item.symbol_uid for item in symbols}
    kind_map = {
        "api_route": EntrypointKind.FASTAPI_ROUTE,
        "application": EntrypointKind.UNKNOWN,
        "cli": EntrypointKind.ARGPARSE_COMMAND,
        "script": EntrypointKind.PYTHON_MAIN,
    }
    entrypoints = tuple(
        EntrypointFact(
            entrypoint_uid=canonical_sha256(
                {"file_uid": fact.file_uid, "kind": item.kind, "name": item.target, "method": item.method, "path": item.path, "line": item.line}
            ),
            file_uid=fact.file_uid,
            symbol_uid=symbols_by_name.get(item.target),
            kind=kind_map[item.kind],
            name=item.target,
            method=item.method,
            path=item.path,
            line=item.line,
        )
        for item in sorted(visitor.entrypoints, key=lambda value: (value.line, value.kind, value.target))
    )
    return symbols, imports, entrypoints, ()
