"""Static-parser fixtures; target modules are never imported or executed."""

from __future__ import annotations

from repo_cartographer.canonical import canonical_sha256, sha256_file
from repo_cartographer.domain import EntrypointKind, FileFact, FileKind, TrackedState
from repo_cartographer.python_ast import parse_python_file


def _fact(path, *, relative: str = "module.py", sensitive: bool = False) -> FileFact:
    return FileFact(
        file_uid=canonical_sha256({"path": relative}),
        path=relative,
        kind=FileKind.SOURCE,
        tracked_state=TrackedState.TRACKED,
        byte_size=path.stat().st_size,
        sha256=sha256_file(path),
        language="python",
        sensitive=sensitive,
        parse_status="skipped" if sensitive else "pending",
    )


def test_parse_python_file_discovers_symbols_imports_and_entrypoints_without_importing(tmp_path):
    source = tmp_path / "module.py"
    source.write_text(
        """import argparse as ap
from fastapi import FastAPI

raise RuntimeError("this code must never execute")
app = FastAPI()

class Service:
    def placeholder(self):
        pass

    async def run(self, value: int) -> str:
        return str(value)

@app.get("/health")
def health() -> dict:
    return {"ok": True}

def main():
    parser = ap.ArgumentParser()
    parser.add_argument("--fixture")
    return parser.parse_args()

if __name__ == "__main__":
    main()
""",
        encoding="utf-8",
    )

    symbols, imports, entrypoints, evidence = parse_python_file(tmp_path, _fact(source))
    by_name = {item.qualified_name: item for item in symbols}

    assert {item.module for item in imports} == {"argparse", "fastapi"}
    assert {"module", "Service", "Service.placeholder", "Service.run", "health", "main"} <= set(by_name)
    assert by_name["Service.placeholder"].body_classification == "scaffold"
    assert by_name["Service.run"].body_classification == "substantive"
    assert by_name["Service.run"].signature == "async def run(self, value: int) -> str"
    assert any(item.kind is EntrypointKind.FASTAPI_ROUTE and item.method == "GET" and item.path == "/health" for item in entrypoints)
    assert any(item.kind is EntrypointKind.ARGPARSE_COMMAND for item in entrypoints)
    assert any(item.kind is EntrypointKind.PYTHON_MAIN and item.name == "main" for item in entrypoints)
    assert evidence == ()


def test_syntax_failure_is_nonfatal(tmp_path):
    source = tmp_path / "module.py"
    source.write_text("def broken(:\n", encoding="utf-8")

    symbols, imports, entrypoints, evidence = parse_python_file(tmp_path, _fact(source))

    assert symbols == imports == entrypoints == ()
    assert len(evidence) == 1
    assert "SyntaxError" in evidence[0].observed_value


def test_sensitive_file_is_not_opened(tmp_path, monkeypatch):
    source = tmp_path / "module.py"
    source.write_text("FIXTURE_TOKEN = 'not-real'\n", encoding="utf-8")
    fact = _fact(source, sensitive=True)

    def forbidden_open(*args, **kwargs):
        raise AssertionError("sensitive source was opened")

    monkeypatch.setattr("tokenize.open", forbidden_open)
    symbols, imports, entrypoints, evidence = parse_python_file(tmp_path, fact)

    assert symbols == imports == entrypoints == ()
    assert evidence[0].observed_value == "sensitive file skipped without reading"
