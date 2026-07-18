# MarketLeak repository contract

These instructions apply to the entire repository. Read them before changing code, data contracts, tests, documentation, model behavior, or user-facing claims. More specific `AGENTS.md` files may add constraints but must not weaken this contract. Also obey [.agents/AGENTS.md](.agents/AGENTS.md): **never use synthetic data**; tests may use minimal fixtures only to verify mechanics and must be described as fixtures, never evidence of effectiveness.

## Mission and non-claims

MarketLeak is a validation-first prediction-market surveillance research system. It records unusual activity, assembles point-in-time evidence, proposes non-personal observable mechanisms, and routes cases for human review.

It does **not** establish fraud, intent, account ownership, identity, access to nonpublic information, or legal liability. Do not call a score a fraud probability, call a wallet a person, or turn an alert into an accusation. The empirical status is **`effectiveness_unknown`** until frozen, independently adjudicated data and adequate prospective evaluation prove otherwise.

Keep these questions separate in schemas, features, labels, APIs, reports, and UI:

- **A - activity:** was market behavior unusual under causal market-state controls?
- **B - public explanation:** what matching public information was observable before the movement?
- **C - actor/access context:** what independently sourced actor or access facts exist?

No combination of A/B/C automatically proves misconduct. `unknown`, `unmapped`, missing, unavailable, partial, stale, and gap states are first-class outcomes, not negatives or zeroes.

## Current truth versus target architecture

Describe only code paths that exist and have fresh verification as "implemented." Current Phase 15 provides point-in-time event storage, causal feature assembly, human multi-axis labels, exact per-modality retrieval, baseline evaluation, an experimental shared/private neural shell, restraint primitives, immutable bundle metadata, and read-only v3 serving.

It does **not** currently provide a validated fraud predictor, trained production neural weights, a complete adjudicated corpus, automatic bundle approval, live v3 inference, or proof of effectiveness. `cli_v3 train-baseline` is an in-memory candidate evaluation compatibility command; it does not persist, approve, publish, or serve a model.

The target is a gated multimodal event-memory system with specialist encoders, calibrated late fusion, causal chronology, OOD/conformal abstention, exact retrieval benchmarks, and human adjudication. A target described in [docs/phase15-multimodal.md](docs/phase15-multimodal.md) must not be represented as current behavior.

## Causal time and provenance invariants

- Every decision is evaluated at an explicit timezone-aware UTC `as_of` cutoff.
- A fact is eligible only if both its event/publication time **and its availability/ingestion clock** are no later than `as_of`.
- Preserve distinct clocks: source event time, claimed publication time, first observed/seen, retrieved/received, ingested, and decision time. Never backfill later knowledge into an earlier decision.
- Capture each delivery before parsing. Store the immutable raw bytes/message, SHA-256 content address, and a separate retrieval/receive receipt. Replay creates a new receipt; semantic UID/content conflicts go to quarantine, never overwrite.
- Every canonical record and derived feature retains raw lineage, source, coverage state, and source watermark. No raw lineage means no evidentiary use.
- Absence claims require verified complete coverage for the relevant source, query, and interval. Partial or unknown coverage cannot support "no public explanation" or "no activity."
- Scheduled events are admissible only when their existence was observed before the cutoff. They are controls, not labels.
- An actor/graph edge discovered because an alert fired cannot independently corroborate that same alert.

See [docs/data-and-evidence.md](docs/data-and-evidence.md) and [docs/architecture-v2.md](docs/architecture-v2.md).

## Source and collector rules

- Use documented official/public endpoints and explicit, bounded source configurations. Collection performs no network work on import or construction.
- Respect source terms, authentication, rate limits, pagination, retries, reconnect bounds, and declared contact/user-agent requirements. Keep credentials server-side and out of URLs, browser bundles, receipts, raw artifacts, fixtures, logs, and version control.
- Store raw delivery and receipt before normalization. Record gaps, reconnects, stale books, unavailable pages, and high-watermarks explicitly.
- Do not infer unavailable fields: a price observation is not a fill; an unattributed trade has no invented actor or side; aggregate L2 is not owned-order data.
- Reference prices fail closed. Use only the per-market source documented by the contract's settlement/reference rules and observed before `as_of`; never substitute a convenient exchange price.
- On-chain facts must be raw-verified and joined deterministically. Compute paths and sums in code. Do not infer control, beneficial ownership, identity, intent, or cross-chain linkage.
- SEC Forms 3/4/5 are public-information context and a possible normal-behavior/pretraining corpus, not fraud labels. Preserve filing acceptance/first-observed time separately from transaction time and handle amendments and transaction codes explicitly.
- Never scrape or collect broadly merely because data exists. Declare the research purpose, eligible fields, retention boundary, and source coverage contract first.

## Features, models, and retrieval

- Build causal fixed-window features from facts eligible at the cutoff. Carry continuous price, volume/notional, spread, depth, imbalance, volatility, freshness, reliability, and explicit missingness where available.
- Missing modality is represented by an observed/missing mask and reason, never a numerical zero implying inactivity.
- The legacy v2 `thin_liquidity_threshold` is a diagnostic tag only. Do not promote it into a fraud rule, Phase 15 exclusion, or approval threshold.
- Start with interpretable specialist baselines and calibrated late fusion. A neural candidate must beat the frozen baseline on unseen time periods, markets/event clusters, actors/wallets, missing modalities, calibration, and selective risk.
- Shared representations support cross-modal comparison; private representations retain modality-specific information. Do not force every modality into one undifferentiated vector.
- Retrieval is contextual evidence, never a labeler. Keep modality-specific exact `IndexFlatIP` benchmarks. Add HNSW/IVF-PQ only after predeclared Recall@K, temporal, provenance, and selective-risk gates pass.
- OOD/kNN/energy values measure distributional support, not fraud probability. Modality disagreement, unavailable calibration, weak support, gaps, or insufficient reference history must be able to trigger abstention.
- Never silently replace an unavailable model/runtime/source with a heuristic. Return an explicit unavailable/not-ready/abstain reason.

## Labels and adjudication

Use append-only, independently human-authored multi-axis labels:

- observable mechanism (for example thin-liquidity artifact, public-information response, underlying-reference move, or unexplained activity);
- evidence strength (absent, conflicting, limited, corroborated, unknown, unmapped);
- human disposition (benign response, escalate for review, insufficient evidence, unresolved, unknown, unmapped);
- optional external legal outcome for audit only.

`unknown` and `unmapped` are never training-eligible negatives. An allegation is not a final judgment; preserve legal outcome stage and mapping confidence. Related contracts from one enforcement episode are one correlated case cluster, not independent positives. LLM output cannot create ground truth, resolve legal status, approve a bundle, or make a final decision.

## Evaluation and release gates

- Freeze the dataset snapshot, hashes, feature specification, code/config revision, random seed, label policy, partitions, calibration plan, and metrics before evaluation.
- Split forward in time and keep event clusters, markets, actors/wallets, issuers, and enforcement episodes disjoint where applicable. Prevent source documents or future amendments from leaking across the cutoff or split.
- Report class counts and label/mapping quality. Withhold calibration and effectiveness claims when counts or coverage are inadequate.
- Evaluate AUROC/AUPRC only as diagnostics. Operational metrics include precision@K, recall at fixed analyst budget, false escalations per analyst-day, Brier score/calibration error, coverage-versus-selective-risk, OOD behavior, missing-modality robustness, and prospective frozen performance.
- Compare against simple deterministic and unimodal baselines; run ablations for each modality. If a new modality or neural design adds no out-of-sample lift, do not promote it.
- A passing test, valid manifest, shadow run, or successful candidate evaluation proves engineering behavior only, not fraud-prediction effectiveness.

See [docs/validation-and-shadow.md](docs/validation-and-shadow.md).

## LLM and identity boundary

LLMs may extract claims/entities, normalize text, assist entity resolution, retrieve context, and format cited chronologies. Deterministic code must enforce timestamps, arithmetic, graph joins, provenance, coverage, policy gates, and bundle verification. LLMs are not primary classifiers, judges, labelers, or sources of facts.

Keep pseudonymous wallet/account identifiers pseudonymous. Do not name a person, infer common control, or claim an identity from transaction patterns, funding proximity, public handles, or model similarity. Only independently sourced, provenance-retained mappings may be displayed, with their exact confidence and legal status.

## Artifact and bundle governance

- Treat raw objects, receipts, normalized revisions, labels, ledgers, frozen snapshots, and manifests as append-only. Never mutate history to make a run pass.
- An immutable bundle manifest must bind SHA-256 hashes for dataset, feature spec, model, calibration, OOD, conformal controller, retrieval, code, and policy metadata.
- Hash verification proves byte identity only. It does not approve the model. Approval and operational attestation are separate, explicit, independently reviewable gates.
- Keep `MARKETLEAK_V3_BUNDLE_ROOT` and all artifact paths server-side. v3 endpoints remain read-only metadata/frozen-record surfaces unless a separately reviewed architecture change explicitly authorizes more.
- Do not commit downloaded corpora, secrets, generated caches, temporary pytest directories, local model weights, or large runtime outputs.

## Repository map

- `marketleak/domain/`: canonical typed records and invariants.
- `marketleak/ingestion/`: raw-first collectors, connectors, normalization, receipts, and coverage.
- `marketleak/evidence/`, `marketleak/onchain/`, `marketleak/graph/`: bounded contextual evidence and deterministic corroboration.
- `marketleak/detectors/`, `marketleak/evaluation/`, `marketleak/shadow/`: v2 diagnostics, evaluation, and frozen shadow protocol.
- `marketleak/multimodal/`: Phase 15 event store, features, labels, fusion, baselines, neural shell, retrieval, restraint, bundles, and serving.
- `marketleak/llm/`: constrained downstream language-model utilities.
- `marketleak/cli_v2.py`, `marketleak/cli_v3.py`: operator entry points; preserve documented safety semantics.
- `configs/`: example/frozen source, detector, evaluation, shadow, and Phase 15 contracts. Example configs must not imply enabled coverage.
- `tests/v2/`, `tests/phase15/`, `tests/integration/`: behavioral and boundary tests.
- `specs/marketleak.feature`: legacy/v2 Gherkin acceptance behavior; preserve it for compatibility and update it only when legacy behavior changes.
- `specs/phase15_multimodal.feature`: Phase 15 and target multimodal Gherkin contract; update it when Phase 15 user-visible guarantees change, then add executable tests.
- `docs/architecture-blueprint.md`: current-versus-target implementation blueprint; keep its status matrix synchronized with code and verified behavior.
- `docs/`: architecture, operations, validation, and evidence policies; update current-vs-target language with code changes.
- `ui/`: React/Vite read-only review interface; never expose secrets or upgrade metadata into accusations.

## Implementation workflow

1. Read the relevant architecture/policy docs and adjacent tests before editing.
2. State whether the change affects current behavior, target design, source coverage, a label contract, or a safety boundary.
3. For external APIs, verify current official documentation and add bounded, injectable transport plus captured fixtures from real deliveries where permitted.
4. Write/update Gherkin for changed observable behavior and executable tests for success, missingness, cutoff leakage, source gaps, malformed input, and abstention/fail-closed behavior.
5. Make the smallest coherent change. Preserve backward compatibility unless the task explicitly authorizes a migration.
6. Run focused tests, then the full suite. If UI changes, also run its build. Report exact commands and results; do not say "done" without fresh evidence.
7. Update documentation and examples to distinguish implemented behavior from proposal. Never relax a safety invariant merely to satisfy a test.

## Verification commands

Python 3.11 with pinned dependencies in `requirements.txt`:

```powershell
python -m pytest tests/phase15 -q --basetemp pytest-tmp-phase15-current
python -m pytest tests/v2 tests/integration -q --basetemp pytest-tmp-v2-current
python -m pytest -q --basetemp pytest-tmp-current
python -m marketleak.cli_v2 capabilities
```

For UI changes:

```powershell
Set-Location ui
npm run build
```

Use unique `--basetemp` directories when concurrent agents are running. Never delete or rewrite another agent's worktree changes or test artifacts. Preserve unrelated user edits.
