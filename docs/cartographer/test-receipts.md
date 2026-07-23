# Executed-test receipts

The test-receipt overlay is separate from the static inventory. Scanning still executes nothing and still reports test declarations only. An operator must explicitly run `run-tests` with a verified inventory, the source checkout, and a strict checked-in runner policy.

The runner accepts only inventoried test UIDs or bounded paths allowed by the policy. It verifies the source bytes, copies eligible files into a temporary directory, verifies the copy, and invokes `[sys.executable, -m, pytest]` with `shell=False`. Third-party plugin autoload and inherited `PYTEST_ADDOPTS` are disabled. The bundled plugin records structured native node IDs, collection errors, and setup/call/teardown outcomes. Standard output, standard error, arbitrary environment values, and exception text are not retained in the receipt. The plugin event file is size-checked before reading; overflow is explicit and non-promotable.

Each `repo-cartographer-pytest-receipt/v1` receipt binds the source inventory root, snapshot, Git head/index, file root, scan config, runner policy, interpreter and pytest versions, structured argv, declaration keys, selected source hashes, native cases and phases, exit status, and frozen-copy mutation result. The receipt SHA-256 protects the canonical payload. Verification against a different or tampered inventory or runner policy fails.

## Promotion rule

Only an explicit required `test_capability` selector can consume receipts. A matched module-level test declaration scopes the join to every eligible non-fixture function/method declaration in that exact file; a direct function/method component scopes only that declaration. An empty derived declaration set is explicit and cannot promote. Every scoped declaration must have all collected parameter cases pass setup, call, and teardown plainly. Any unmapped dynamic node, skip, xfail, xpass, failure, error, missing phase, collection error, event-file overflow, timeout, nonzero suite exit, or frozen-copy mutation prevents promotion. A nonzero suite exit cancels every promotion from that receipt.

Receipt evidence changes only `verification`, from `test_declared` to `test_passed`. It cannot change definition, integration, artifact, learning, evaluation, runtime, approval, or effectiveness. Optional selectors never promote. A map with no receipts retains the original v1 serialization and declaration-only behavior.
