"""Receipt-aware curated projection mechanics fixtures only."""

from __future__ import annotations

import json
import subprocess

from repo_cartographer.cli import scan_repository
from repo_cartographer.curated import project_curated_map, write_curated_map
from repo_cartographer.curated_config import curated_profile_from_dict
from repo_cartographer.domain import ScanConfig
from repo_cartographer.render import write_inventory
from repo_cartographer.test_receipts import run_pytest_receipt
from repo_cartographer.test_runner_config import PytestRunnerConfig


def _git(root, *args): subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def _fixture(tmp_path):
    root=tmp_path/"repository"; root.mkdir(); _git(root,"init","-b","main"); _git(root,"config","user.name","Receipt Fixture"); _git(root,"config","user.email","fixture@example.invalid")
    (root/"service.py").write_text("def report():\n    return 1\n",encoding="utf-8")
    (root/"test_service.py").write_text("from service import report\ndef test_report():\n    assert report() == 1\ndef test_other():\n    assert report() > 0\n",encoding="utf-8")
    _git(root,"add","."); _git(root,"commit","-m","curated receipt mechanics fixture")
    inventory=tmp_path/"inventory"; write_inventory(scan_repository(root,config=ScanConfig(include_untracked=False)),inventory)
    tests=[json.loads(line) for line in (inventory/"tests.jsonl").read_text(encoding="utf-8").splitlines()]
    symbols={row["symbol_uid"]:row for row in (json.loads(line) for line in (inventory/"symbols.jsonl").read_text(encoding="utf-8").splitlines())}
    executable=[row for row in tests if symbols.get(row.get("symbol_uid"),{}).get("kind") in {"function","method"}]
    config=PytestRunnerConfig("repo-cartographer-pytest-runner/v1",("test_service.py",),30,4096)
    receipt=tmp_path/"receipt.json"; run_pytest_receipt(inventory,config,receipt,repository_root=root,test_uids=[row["test_uid"] for row in executable])
    partial=tmp_path/"partial.json"; run_pytest_receipt(inventory,config,partial,repository_root=root,test_uids=[next(row["test_uid"] for row in executable if symbols[row["symbol_uid"]]["qualified_name"]=="test_report")])
    return inventory,receipt,partial


def _profile(required=True):
    return curated_profile_from_dict({"format":"repo-cartographer-curated-profile/v1","profile_id":"receipt-fixture-v1","title":"Receipt fixture","capabilities":[{"id":"fixture.service","title":"Fixture service","description":"Mechanics fixture only.","selectors":[{"kind":"capability","value":"service","required":True},{"kind":"test_capability","value":"test report","required":required}]}]})


def test_only_explicit_required_test_selector_consumes_verified_receipt_and_changes_only_verification(tmp_path):
    inventory,receipt,_=_fixture(tmp_path)
    before=project_curated_map(inventory,_profile())
    after=project_curated_map(inventory,_profile(),test_receipts=[receipt])
    old=before.capabilities[0]; new=after.capabilities[0]
    assert old.axes.verification.value == "test_declared"
    assert new.axes.verification.value == "test_passed"
    assert new.axes.definition == old.axes.definition
    assert new.axes.integration == old.axes.integration
    assert new.axes.artifact == old.axes.artifact
    assert new.axes.learning == old.axes.learning
    assert new.axes.evaluation == old.axes.evaluation
    assert new.axes.runtime == old.axes.runtime
    assert after.test_receipt_sha256s


def test_optional_test_selector_never_promotes_and_no_receipt_output_stays_compatible(tmp_path):
    inventory,receipt,_=_fixture(tmp_path)
    optional=project_curated_map(inventory,_profile(required=False),test_receipts=[receipt])
    assert optional.capabilities[0].axes.verification.value == "none"
    first=tmp_path/"first.json"; second=tmp_path/"second.json"
    write_curated_map(project_curated_map(inventory,_profile()),first)
    write_curated_map(project_curated_map(inventory,_profile(),test_receipts=[]),second)
    assert first.read_bytes() == second.read_bytes()


def test_file_level_test_capability_requires_every_eligible_declaration_in_exact_file(tmp_path):
    inventory,complete,partial=_fixture(tmp_path)
    module_profile=curated_profile_from_dict({"format":"repo-cartographer-curated-profile/v1","profile_id":"module-receipt-fixture-v1","title":"Module receipt fixture","capabilities":[{"id":"fixture.module","title":"Fixture module","description":"Mechanics fixture only.","selectors":[{"kind":"capability","value":"service","required":True},{"kind":"test_capability","value":"test service","required":True}]}]})
    complete_row=project_curated_map(inventory,module_profile,test_receipts=[complete]).capabilities[0]
    partial_row=project_curated_map(inventory,module_profile,test_receipts=[partial]).capabilities[0]
    assert complete_row.axes.verification.value == "test_passed"
    assert partial_row.axes.verification.value == "test_declared"
    assert "test_receipt_incomplete:test service" in partial_row.reason_codes


def test_test_selector_with_no_eligible_declarations_is_explicit_and_never_promotes(tmp_path):
    inventory,complete,_=_fixture(tmp_path)
    profile=curated_profile_from_dict({"format":"repo-cartographer-curated-profile/v1","profile_id":"empty-test-fixture-v1","title":"Empty test fixture","capabilities":[{"id":"fixture.empty","title":"Fixture empty","description":"Mechanics fixture only.","selectors":[{"kind":"test_capability","value":"service","required":True}]}]})
    row=project_curated_map(inventory,profile,test_receipts=[complete]).capabilities[0]
    assert row.axes.verification.value != "test_passed"
    assert "test_selector_has_no_eligible_declarations:service" in row.reason_codes
