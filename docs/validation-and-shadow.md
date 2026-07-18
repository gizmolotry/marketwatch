# Validation, readiness, and prospective shadow protocol

## Current empirical status

MarketLeak is not empirically validated as a fraud or misconduct predictor. The correct current statement is **`effectiveness_unknown`**. Existing synthetic hard negatives test data-handling mechanics, not real-world predictive effectiveness. A prospective shadow cycle proves reproducibility and ledger integrity only.

No score, ranked mechanism, OOD value, queue position, or external-case reference may be presented as a finding about a person. Human review and independently sourced evidence remain required.

## Human labels and evaluation targets

Phase 15 labels are human-authored multi-axis records:

- observable mechanism;
- evidence strength;
- human disposition;
- optional external legal audit, which is audit-only and never a predictive target.

`unknown` and `unmapped` are never negatives or training eligible. An external legal outcome does not make a label trainable. Labels are append-only through `supersedes_uid`, so prior decisions remain auditable.

## Readiness gates

A Phase 15 candidate evaluation requires all of the following:

1. facts and feature rows available at the exact `as_of` cutoff;
2. required source coverage and market context complete;
3. independent human labels that are explicitly training eligible;
4. a fixed baseline plan and explicit baseline labels;
5. forward-time partitions disjoint by event cluster, market, and actor/wallet where applicable;
6. sufficient class and calibration coverage under the frozen gate.

If any gate fails, `not_ready` is the correct output. The baseline candidate remains unapproved and unpublished even when gates pass. The experimental neural shared/private architecture has no default weight bundle and needs a separately approved immutable bundle before protected execution; no current command grants that approval.

## Calibration, OOD, and conformal restraint

Calibration metrics are withheld until minimum labeled positive and negative counts pass. The system must not turn an uncalibrated support score into a probability of fraud.

The selective restraint layer abstains for insufficient modalities, weak support, cross-modality disagreement, absent calibration when required, unavailable OOD reference history, or out-of-distribution inputs. OOD combines k-nearest-neighbor distance and an energy-like value against an explicit reference population; it is a similarity restraint, not a legal-risk score.

The rolling conformal controller uses only feedback available at the decision cutoff. With insufficient feedback it refuses escalation. When feedback is available, it constrains a finite analyst queue using observed unsupported escalations and a declared daily budget. Feedback means whether an escalation helped the analyst workflow, not whether misconduct occurred.

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
