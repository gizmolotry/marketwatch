"""Executed-test receipt CLI mechanics fixtures only."""

from __future__ import annotations

import json
import subprocess

from repo_cartographer.cli import EXIT_FAILED, EXIT_OK, main


def _git(root,*args): subprocess.run(["git",*args],cwd=root,check=True,capture_output=True)


def test_run_and_verify_receipt_cli(tmp_path,capsys):
    root=tmp_path/"repository"; root.mkdir(); _git(root,"init","-b","main"); _git(root,"config","user.name","Receipt Fixture"); _git(root,"config","user.email","fixture@example.invalid")
    (root/"tests").mkdir(); (root/"tests"/"test_ok.py").write_text("def test_ok():\n    assert True\n",encoding="utf-8")
    _git(root,"add","."); _git(root,"commit","-m","receipt CLI mechanics fixture")
    inventory=tmp_path/"inventory"
    assert main(["scan","--root",str(root),"--output",str(inventory),"--no-include-untracked"])==EXIT_OK; capsys.readouterr()
    config=tmp_path/"runner.json"; config.write_text(json.dumps({"format":"repo-cartographer-pytest-runner/v2","allowed_test_paths":["tests"],"timeout_seconds":30,"max_output_bytes":4096,"max_source_files":100,"max_source_bytes":1000000}),encoding="utf-8")
    key=tmp_path/"receipt.key"; key.write_bytes(b"receipt-cli-mechanics-key-material-001")
    receipt=tmp_path/"receipt.json"
    assert main(["run-tests","--root",str(root),"--inventory",str(inventory),"--config",str(config),"--output",str(receipt),"--test-path","tests","--attestation-key-id","fixture-cli-ci","--attestation-key-file",str(key)])==EXIT_OK
    result=json.loads(capsys.readouterr().out); assert result["promotable"] is True
    assert main(["verify-receipt","--receipt",str(receipt),"--inventory",str(inventory),"--config",str(config),"--trusted-key-id","fixture-cli-ci","--trusted-key-file",str(key)])==EXIT_OK
    assert json.loads(capsys.readouterr().out)["ok"] is True
    payload=json.loads(receipt.read_text(encoding="utf-8")); payload["mutation"]["frozen_copy_unchanged"]=False; receipt.write_text(json.dumps(payload),encoding="utf-8")
    assert main(["verify-receipt","--receipt",str(receipt),"--inventory",str(inventory)])==EXIT_FAILED
    assert "hash mismatch" in capsys.readouterr().err
