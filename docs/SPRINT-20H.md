# MarketLeak 20-hour sprint

This is the visible clock and cut line for the current build. The deadline is **2026-07-19 00:06 America/Los_Angeles (PDT, UTC-07:00)**. The planning reference start is **2026-07-18 04:06 PDT**.

- Sprint tracker: [GitHub Issue #1](https://github.com/gizmolotry/marketwatch/issues/1)
- Working branch: `feature/20h-market-integrity-demo`

To see the live remaining time in VS Code, run **Terminal → Run Task → Sprint: countdown**. The deadline is fixed; if work starts later, do not slide it. Recalculate remaining time and cut scope.

## Definition of a successful submission

The sprint produces a defensible, runnable **market-integrity triage demonstration**. It shows point-in-time collection, a causal evidence packet, a mechanism/evidence assessment, explicit missingness, and abstention. It does not claim a validated fraud probability, identify a person from a wallet, or imply that passing tests proves effectiveness.

The architecture source of truth is [architecture-blueprint.md](architecture-blueprint.md). Evidence handling follows [data-and-evidence.md](data-and-evidence.md), and evaluation claims follow [validation-and-shadow.md](validation-and-shadow.md).

## Checkpoints and cut decisions

| Deadline (PDT) | Architecture focus | Deliverable and decision |
|---|---|---|
| H+0 — Jul 18, 04:06 | Phase A: contracts | Record baseline Git state; create the sprint branch; run Phase 15 tests; identify one demo market/case and freeze its `as_of` story. |
| H+4 — Jul 18, 08:06 | Phase B: core intake | One bounded, raw-first collection path has receipts, clocks, lineage, coverage/gap states, and a replayable real delivery. If this is not reliable, stop adding sources. |
| H+8 — Jul 18, 12:06 | Phase C: features | Produce the smallest causal multi-horizon feature/evidence packet needed by the demo. Missing inputs remain masks/reasons, never zero-filled activity. |
| H+12 — Jul 18, 16:06 | Phases D/F/G: context and fusion | Integrate only the highest-value public-information/SEC context that can be verified. Demonstrate an interpretable baseline or existing fusion/retrieval path; neural work remains experimental unless it has real training and evaluation evidence. |
| H+16 — Jul 18, 20:06 | Phases H/I: restraint and shadow demo | Freeze features and demo inputs. Show OOD/coverage/calibration gates, an abstention path, immutable evidence references, and a read-only review flow. Begin submission assets. |
| H+20 — Jul 19, 00:06 | Release cut | Full tests and UI build are green or failures are disclosed. Repository, README/demo instructions, architecture diagram, video, and contest submission are ready. No new features in the final two hours. |

At every checkpoint, use the rule: **preserve a working vertical slice before widening the system**. A late source, model, or feature is removed from the demo path rather than weakening causal or provenance safeguards.

## Scope priorities

### P0 — must be demonstrable

- One real, point-in-time market episode with immutable raw artifacts and receipts.
- A market/reference/public-context chronology whose clocks are visible.
- Causal market features with missingness and coverage status.
- A mechanism/evidence result that can abstain and links back to its evidence.
- A runnable API/UI or CLI path with concise reproduction instructions.
- Fresh Phase 15 tests, full tests, and UI build results.

### P1 — add only after P0 is stable

- One incremental official SEC Form 3/4/5 ingestion slice for issuer-linked context.
- A small, provenance-retained enforcement-case mapping example, clearly separated from model labels.
- Exact similar-event retrieval and one interpretable specialist baseline comparison.
- A polished analyst packet and contest-specific Qwen/Alibaba integration where required.

### P2 — cut first

- Training a production multimodal neural network.
- Broad SEC backfill, generalized case-graph automation, or multiple venue families.
- HNSW/IVF-PQ optimization, automated identity linkage, or unvalidated fraud scoring.
- Any modality that cannot be raw-captured, causally joined, and shown in the demo.

## Git and GitHub learning loop

Use one small branch and a sequence of understandable commits. Git is the local history; GitHub is the hosted copy and collaboration surface.

1. **Inspect:** run `git status --short --branch` and `git diff`. In VS Code, open **Source Control** with `Ctrl+Shift+G` and inspect each changed file.
2. **Branch:** create a focused branch, for example `git switch -c feature/20h-market-integrity-demo`. A branch is an independent line of work, not a copy of the folder.
3. **Stage intentionally:** use the `+` button beside only the files that belong together, or run `git add path/to/file`. Staging selects the exact snapshot for the next commit.
4. **Review:** inspect **Staged Changes** or run `git diff --staged`. Never commit secrets, downloaded corpora, model weights, caches, or runtime output.
5. **Commit:** write an outcome-oriented message such as `git commit -m "Add causal multi-horizon market features"`. Commit after a coherent, verified slice—not after every keystroke and not only at the end.
6. **Publish:** after confirming the GitHub remote and authentication, run `git push -u origin feature/20h-market-integrity-demo`. The first push connects the local branch to its GitHub branch.
7. **Pull request:** open the GitHub repository, create a pull request from the branch, summarize what changed, list exact test results, and disclose limitations. The PR becomes the reviewable narrative of the sprint.

Before every commit:

```powershell
git status --short --branch
git diff
git diff --staged
```

After every commit:

```powershell
git log -1 --oneline
git status --short --branch
```

Do not use `git reset --hard`, force-push, or bulk staging while learning. If a file appears that you do not recognize, stop and inspect it before staging.

## VS Code workflow

Open the repository as the workspace:

```powershell
code D:\marketwatch
```

Useful controls:

- `Ctrl+Shift+P`: Command Palette; search for any VS Code action.
- ``Ctrl+` ``: integrated terminal.
- `Ctrl+Shift+G`: Source Control view for diffs, staging, commits, branches, and sync.
- **Terminal → Run Task**: run the repository tasks in `.vscode/tasks.json`.
- **Problems** panel: review compiler/linter errors before committing.

The supplied tasks are deliberately local and safe: countdown, Git status, Phase 15 tests, full tests, capabilities, and UI build. They do not stage, commit, push, deploy, delete, or publish anything.

## Verification commands

Run the fast focused check during development:

```powershell
python -m pytest tests/phase15 -q --basetemp pytest-tmp-phase15-vscode
```

At the release cut, run:

```powershell
python -m pytest -q --basetemp pytest-tmp-full-vscode
python -m marketleak.cli_v2 capabilities
Set-Location ui
npm run build
```

Record the exact command, timestamp, pass/fail count, and any known limitation in the pull request. Engineering verification supports reproducibility; it does not change `effectiveness_unknown`.
