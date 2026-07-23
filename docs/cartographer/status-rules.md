# Repo Cartographer MVP status rules

## Principle

Each capability carries independent axes rather than one optimistic label. A declaration, a static import, a test name, a manifest, model bytes, and a running service answer different questions. The cartographer records the strongest state that its bounded evidence supports and retains contradictions and limitations.

| Axis | MVP state vocabulary | What the MVP can support |
|---|---|---|
| Definition | `absent`, `docs_only`, `declaration`, `scaffold`, `substantive_implementation` | Static AST body classification, specs, and explicit placeholder markers |
| Integration | `isolated`, `importable`, `statically_wired`, `dynamically_reachable` | Up to `statically_wired` from import/entrypoint evidence |
| Verification | `none`, `test_declared`, `test_passed` | Up to `test_declared`; the MVP does not execute tests |
| Artifact | `none`, `manifest_only`, `bytes_present`, `hash_verified` | Opaque byte and JSON-manifest checks |
| Learning | `not_applicable`, `architecture_only`, `untrained`, `trained_unverified`, `trained_verified` | At most `trained_unverified` when model-like bytes exist; never training proof |
| Evaluation | `none`, `mechanic_fixture`, `retrospective_case`, `frozen_holdout`, `prospective` | `mechanic_fixture` only when the discovered test marks a fixture-only mechanic |
| Runtime | `not_observed`, `unavailable`, `running`, `probe_failed` | `not_observed`; the MVP performs no service probing |

## Promotion rules

- Documentation and Gherkin are `docs_only` unless separate executable evidence supports a higher definition state.
- A placeholder-only Python body (`pass`, ellipsis, or a recognizably empty scaffold) is `scaffold`; a non-placeholder body is static evidence for `substantive_implementation`, not proof that it is correct.
- A file can be `importable`; an import or static entrypoint can make it `statically_wired`. Only a separately implemented and captured runtime probe may claim `dynamically_reachable`.
- A discovered `pytest`-style declaration is `test_declared`. Test execution, exit status, environment, and timestamp must be captured by a future verification layer before `test_passed` is permitted.
- Artifact bytes and matching explicit JSON-manifest SHA-256 bindings support artifact states only. They do not approve a bundle or prove that a model was trained, suitable, loaded, or served.
- A model-like artifact has `trained_unverified` at most. Training data, code/config, seed, metrics, provenance, evaluation, approval, and runtime are separate evidence requirements.
- A fixture-only test can establish only `mechanic_fixture`; fixtures are not real-world evidence and never establish MarketLeak effectiveness.

## Contradictions and negative markers

Explicit negative tags and reason codes are evidence, not cleanup noise. They cap promotion conservatively and remain linked to the capability. Examples include a placeholder body, `@not_current`, a missing artifact target, a hash mismatch, sensitive-content non-parsing, or unavailable provenance.

If support and refutation coexist, the capability retains both evidence sets and a contradiction code. The system must not pick the most favorable interpretation silently.

## Forbidden inferences

The following are outside the MVP and must not be derived from static inventory:

- a passing test, live service, or deployed runtime;
- a trained or approved production model;
- a valid fraud prediction, legal conclusion, actor identity, or effective surveillance capability;
- semantic equivalence from string or embedding similarity;
- causal confirmation from code layout, documentation, Git history, or an agent transcript.

Hash identity proves that bytes match a declared digest. It does not establish any of the conclusions above.

## Curated projection rules

The curated layer reads only a successfully verified rendered inventory. Selectors are exact: low-level capability name, test-capability name, Gherkin scenario name, Gherkin tag, or repository-relative artifact path. There is no fuzzy fallback.

- Required selectors aggregate independently by axis; the weakest applicable required state wins.
- A missing required selector contributes the weakest applicable state and an explicit reason code.
- Optional selectors are retained for explanation but never promote any state.
- Test-capability selectors may contribute only verification and evaluation.
- Scenario and tag selectors may contribute only `docs_only` definition and `test_declared` verification.
- Artifact-path selectors may contribute only the artifact axis.
- Static curated output caps `dynamically_reachable` to `statically_wired`, `test_passed` to `test_declared`, `trained_verified` to `trained_unverified`, and `running` to `not_observed`.

The projection never emits a composite done, readiness, health, approval, fraud, or effectiveness score. Its map hash proves canonical byte identity; the bound profile hash identifies the taxonomy; the bound source root identifies the inventory evidence.

## Executed-test receipt rule

Static inventory remains capped at `test_declared`. A separate verified receipt can promote an explicit required `test_capability` selector to `test_passed` only when every mapped non-fixture declaration has at least one collected native case and every parameter case has plain passed setup, call, and teardown phases. A module TestFact is a file-scope anchor requiring every eligible non-fixture function/method TestFact in that exact file; an empty derived set never promotes. Unmapped dynamic nodes, marked or dynamic skip, xfail, xpass, missing phases, failures, errors, collection errors, bounded event-file overflow, timeout, nonzero exit, and frozen-copy mutation fail closed. A nonzero exit cancels all promotions from that receipt.

Receipts affect only verification. They do not alter definition, integration, artifact, learning, evaluation, runtime, approval, or effectiveness. Optional selectors cannot consume a receipt for promotion, and test results never promote ordinary capability selectors implicitly.
