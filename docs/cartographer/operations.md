# Repo Cartographer MVP operations

## Safe operating model

Run the cartographer against a deliberate Git worktree and an explicit JSON config, normally [default.json](../../configs/cartographer/default.json). Static scanning is a read-only inventory operation: it reads Git metadata and eligible repository bytes, then returns frozen in-memory facts. It does not collect network data, execute project code, import project modules, start services, load model objects, write a ledger, or modify MarketLeak data. Test execution occurs only through the separate explicit bounded `run-tests` receipt adapter described below.

Before comparing scans, record the repository commit and dirty state. A dirty snapshot is useful evidence, but it must not be described as a clean release inventory.

```powershell
git status --short
python -m pytest tests/cartographer -q --basetemp pytest-tmp-cartographer-<unique-name>
```

The test command verifies the cartographer's engineering mechanics only. It does not verify any MarketLeak detector, data source, model, or effectiveness claim.

## Command-line workflow

```powershell
python -m repo_cartographer scan --root D:\marketwatch --config D:\marketwatch\configs\cartographer\default.json --output D:\marketwatch\.cartographer\marketwatch-current --include-untracked
python -m repo_cartographer verify --inventory D:\marketwatch\.cartographer\marketwatch-current
python -m repo_cartographer explain --inventory D:\marketwatch\.cartographer\marketwatch-current --capability <UID_OR_EXACT_NAME>
python -m repo_cartographer map --inventory D:\marketwatch\.cartographer\marketwatch-current --profile D:\marketwatch\configs\cartographer\marketleak-capabilities.json --output D:\marketwatch\.cartographer\marketleak-map.json
python -m repo_cartographer map-explain --map D:\marketwatch\.cartographer\marketleak-map.json --capability model.experimental_neural_shell
python -m repo_cartographer map-diff --before D:\marketwatch\.cartographer\marketleak-map-before.json --after D:\marketwatch\.cartographer\marketleak-map-after.json
python -m repo_cartographer run-tests --root D:\marketwatch --inventory D:\marketwatch\.cartographer\marketwatch-current --config D:\marketwatch\configs\cartographer\pytest-runner.json --output D:\marketwatch\.cartographer\cartographer-tests.receipt.json --test-path tests/cartographer --attestation-key-id cartographer-ci --attestation-key-file D:\operator-secrets\cartographer-ci.key
python -m repo_cartographer verify-receipt --receipt D:\marketwatch\.cartographer\cartographer-tests.receipt.json --inventory D:\marketwatch\.cartographer\marketwatch-current --config D:\marketwatch\configs\cartographer\pytest-runner.json --trusted-key-id cartographer-ci --trusted-key-file D:\operator-secrets\cartographer-ci.key
python -m repo_cartographer map --inventory D:\marketwatch\.cartographer\marketwatch-current --profile D:\marketwatch\configs\cartographer\marketleak-capabilities.json --test-receipt D:\marketwatch\.cartographer\cartographer-tests.receipt.json --approved-runner-config D:\marketwatch\configs\cartographer\pytest-runner.json --trusted-key-id cartographer-ci --trusted-key-file D:\operator-secrets\cartographer-ci.key --output D:\marketwatch\.cartographer\marketleak-map.json
```

`scan` exits `0` after writing a frozen canonical inventory and `1` on an operational or consistency failure. `verify` exits `0` only when hashes, counts, canonical ordering, snapshot roots, and basic references agree; integrity failures exit `1`. `explain` exits `0` for one verified exact capability match and `1` for missing, ambiguous, or invalid inventory evidence. Argument-parser usage errors exit `2`. Positional repository, inventory, and capability arguments remain supported for compatibility. Use `--no-include-untracked` to override a config that includes untracked files.

`map` refuses an unverified inventory, binds the exact profile and inventory hashes, and atomically replaces one canonical JSON map. `map-explain` verifies the map hash before resolving an exact stable ID. `map-diff` rejects different profile hashes so taxonomy edits cannot masquerade as repository progress. The checked-in profile is [marketleak-capabilities.json](../../configs/cartographer/marketleak-capabilities.json); review a profile edit as a taxonomy change, separately from repository progress.

`run-tests` requires the checkout root because stable inventories intentionally do not retain absolute checkout paths. It also requires a stable key ID and key material from either an access-controlled file outside the repository or a named environment variable. Never place raw key material on argv. The command emits a signed receipt even when a completed suite fails; command success means the execution was captured, not that tests passed. Inspect `promotable`, `exit_code`, cases, phases, collection errors, and mutation fields. `verify-receipt` requires the exact runner config and a matching trusted key. `--legacy-integrity-only` is an explicit migration/audit mode for v1 receipts and never authorizes promotion. Receipt-aware `map` requires `--approved-runner-config` plus the trusted key source and binds the consumed policy hash. See [test-receipts.md](test-receipts.md).

## Library workflow

The library API remains composable beneath the command-line interface:

1. Load `ScanConfig` with `load_scan_config`.
2. Call `snapshot_repository(root, config)` to obtain `RepositorySnapshot` and `FileFact` values.
3. Call `discover_repository(root, snapshot, files)` for Python/Gherkin/test facts.
4. Call `inspect_artifacts(root, files, config)` for opaque artifact facts and manifest evidence.
5. Call `derive_components` and `derive_capabilities` to obtain conservative status facts.
6. Persist or render results only in a caller-owned, explicitly reviewed layer. The MVP itself does not create an authoritative ledger.

For reproducible comparisons, hold the Git revision, dirty path sets, scan config, scanner version, and eligible file bytes fixed. Compare stable IDs and source hashes, not absolute local paths or wall-clock scan times.

## Handling limitations

- Treat `sensitive`, `max_file_bytes_exceeded`, parse errors, missing artifact bytes, and hash conflicts as visible limitations.
- Do not use ignored or untracked content unless the explicit config includes it; record its tracked state when included.
- Do not add a permissive parser fallback that imports target code or deserializes model files.
- Do not convert a Gherkin tag, a documentation claim, or a fixture test into proof that behavior has been run.

## Change discipline

Changes to cartographer behavior must be additive to MarketLeak and preserve [specs/marketleak.feature](../../specs/marketleak.feature), [specs/phase15_multimodal.feature](../../specs/phase15_multimodal.feature), and the current-versus-target distinctions in [architecture-blueprint.md](../architecture-blueprint.md). Update the cartographer feature contract, focused tests, and this documentation whenever an observable cartographer behavior changes.

Use a unique pytest base temporary directory when parallel agents run. Do not commit generated inventories, caches, downloaded corpora, model weights, secrets, or temporary test directories.

## Future Antigravity integration

No Antigravity integration is implemented in the MVP. The existing token-efficient-routing skill still decides which model is appropriate for a task. A future adapter may attach a routing decision to a cartographer snapshot and record task inputs, changed paths, command hashes, and verification outputs. That adapter must be opt-in, append-only, provenance-retained, and unable to overwrite user changes or declare a capability proven merely because an agent selected or completed a task.
