# Validation, readiness, and prospective shadow protocol

## Current empirical status

MarketLeak is not empirically validated as a fraud or misconduct predictor. The correct current statement is **`effectiveness_unknown`**. Minimal test fixtures verify data-handling mechanics only; they are not evidence of real-world predictive effectiveness. A prospective shadow cycle proves reproducibility and ledger integrity only.

No score, ranked mechanism, OOD value, queue position, or external-case reference may be presented as a finding about a person. Human review and independently sourced evidence remain required.

## Human labels and evaluation targets

Phase 15 labels are human-authored multi-axis records:

- observable mechanism;
- evidence strength;
- human disposition;
- optional external legal audit, which is audit-only and never a predictive target.

`unknown` and `unmapped` are never negatives or training eligible. An external legal outcome does not make a label trainable. Labels are append-only through `supersedes_uid`, so prior decisions remain auditable.

## Readiness gates

A Phase 15 candidate evaluation requires all of the following. For the current naive compatibility baselines, orchestration v5 independently recomputes submitted values from frozen source facts under the immutable, canonically hashed `NaiveBaselineFeatureSpec`:

1. facts and feature rows bound to a snapshot whose manifest `as_of` exactly equals the assembly cutoff, including exactly recomputed observation/availability clocks, source-fact UIDs, raw-artifact UIDs, and the checked-in feature-specification hash;
2. required source coverage and market context complete;
3. independent human labels that are explicitly training eligible;
4. a fixed baseline plan and explicit baseline labels;
5. forward-time partitions disjoint by event cluster, market, and actor/wallet where applicable; actor groups come only from source-fact provenance and cross-boundary groups are dropped;
6. separately predeclared validation and untouched-test minimum labeled, positive, and negative counts over unique stable row UIDs.

The admitted numeric contract is deliberately narrow. Identity is exact `(market_uid, outcome_uid)`. The primary event must be the terminal `MarketStateSlice`, with `event_time == window_ends_at`, defining one exact 300-second lookback. Exact-identity market slices must cover that window completely, contiguously, and without overlap; gaps, overlap, subsets, out-of-window facts, and multiday horizons are rejected. `price_change` uses the first and last distinct non-null trade-price points inside the window. `volume` sums each admitted slice's non-null `trade_notional` once. The only other accepted source is an exact-identity `OnChainSettlementFact` with a non-null observed wallet UID, used for actor provenance only; it must occur inside the same window and be available no later than the market-derived feature availability. Cross-outcome market facts, unrelated, unidentified, late actor facts, and other modalities are excluded. All caller values and bindings must match recomputation; identical duplicate UIDs count once and conflicting duplicates fail closed. These guarantees cover the current naive baselines only, not a general specialist-feature pipeline or model effectiveness.

The assembly's canonical `input_hash` covers complete accepted row declarations—including labels/adjudication, coverage/context, derived values, window clocks, modality, and actor provenance. Each excluded record retains and hashes the complete immutable caller input plus its reason, so different rejected bodies have different input hashes and run UIDs. The `run_uid` separately binds orchestration v5, the cutoff, snapshot manifest, feature-specification hash, input hash, and accepted/excluded identities. Direct construction revalidates causal/window derivation, retained excluded identities, and both hashes. These hashes provide tamper evidence and reproducibility, not effectiveness or approval.

If any gate fails, `not_ready` is the correct output. A one-label test partition cannot emit a candidate evaluation. The baseline candidate remains unapproved and unpublished even when gates pass. The experimental neural shared/private architecture has no default weights and cannot enter protected execution: no current cryptographically bound, independently approved object connects the in-memory model state, specification, calibration artifacts, and operational attestation. Caller Booleans and arbitrary objects do not grant approval.

## Calibration, OOD, and adaptive feedback restraint

Calibration metrics are withheld until minimum labeled positive and negative counts pass. All-zero one-vs-rest isotonic support makes calibration unavailable; the system does not silently fall back to uncalibrated support. It must not turn any support score into a probability of fraud.

The selective restraint layer abstains for insufficient modalities, weak support, cross-modality disagreement, absent calibration when required, unavailable OOD reference history, or out-of-distribution inputs. Cross-modal disagreement uses maximum pairwise bounded Jensen-Shannon distance over observed specialist mechanism distributions; disjoint sparse support is therefore a contradiction rather than false agreement. OOD combines k-nearest-neighbor distance and an energy-like value against an explicit reference population; it is a similarity restraint, not a legal-risk score.

The current `AdaptiveFeedbackThresholdController` is an empirical queue heuristic. It uses only feedback available at the decision cutoff, refuses escalation when history is insufficient, requires a score strictly above its adaptive threshold, and caps the queue at a declared daily budget. Feedback means whether an escalation helped the analyst workflow, not whether misconduct occurred. It provides no conformal coverage or finite-sample risk-control guarantee; the legacy `RollingConformalRiskController` name is deprecated. A statistically justified conformal controller remains target work.

## Legacy v2 diagnostic versus Phase 15 features

The v2 detector's `thin_liquidity_threshold` produces a legacy diagnostic tag and can contribute to a v2 unscorable/abstention state. It is not a Phase 15 hard feature threshold. Phase 15 carries continuous liquidity, depth, spread, price, freshness, and missingness values and must evaluate them on unseen markets and time periods before drawing any operational conclusion.

## Frozen v2 prospective shadow

The prospective shadow runner consumes canonical records only. It freezes the git revision or `UNTRACKED` sentinel, input partition hashes, code/configuration hashes, UTC cutoff, coverage/source high-watermarks, random seed, and control sampling. The ledger is append-only canonical JSONL with a SHA-256 hash chain.

```powershell
python -m marketleak.cli_v2 shadow-prospective `
  --data-root data `
  --run-dir output/shadow/2026-07-13 `
  --as-of 2026-07-13T03:00:00Z `
  --coverage-status partial `
  --coverage-limitation "collection began after the preregistered window"

python -m marketleak.cli_v2 shadow-verify `
  --run-dir output/shadow/2026-07-13
```

The runner excludes future files and facts before quality accounting, raw-hash checks, manifest inclusion, or detector execution. It validates the frozen byte snapshot instead of rereading mutable paths. Missing canonical data or too little historical baseline produces `blocked_by_data_quality` or `not_scorable_insufficient_history`; it never falls back to the legacy Parquet demo.

`shadow-create`, `shadow-run`, and `shadow-cycle` are prepared-input engineering harnesses. They trust caller-supplied outcomes by design and must not be used to claim prospective effectiveness.

## Phase 15 read-only checks

Use the v3 CLI to inspect causal assembly and reasons; it does not train or serve a model:

```powershell
python -m marketleak.cli_v3 assemble `
  --input data/phase15/request.json `
  --as-of 2026-07-13T12:00:00Z

python -m marketleak.cli_v3 readiness `
  --input data/phase15/request.json `
  --as-of 2026-07-13T12:00:00Z
```

For the full Phase 15 operator sequence and immutable serving boundary, see [Phase 15 multimodal architecture](phase15-multimodal.md).
