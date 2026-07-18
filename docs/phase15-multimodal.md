# Phase 15 multimodal architecture and operator workflow

## Scope and current limitation

Phase 15 is an evidence and restraint architecture for research and human review. It is not a deployed fraud predictor. The current empirical status is **`effectiveness_unknown`**: no frozen, sufficiently large corpus of independently adjudicated, exactly market-mapped cases and no adequate prospective evaluation establish predictive effectiveness. A bundle hash, a neural architecture, a candidate evaluation, or an analyst queue entry does not change that status.

The architecture is designed to answer bounded operational questions: what was observable at a cutoff, which non-personal market mechanism is supported by the available facts, and whether the system should abstain. It must never convert a score into a legal conclusion, an identity claim, or a claim of intent.

```mermaid
flowchart LR
  S["Official/raw sources"] --> R["Raw objects + SHA-256 receipts"]
  R --> E["Point-in-time event store"]
  C["Market context and documented reference mapping"] --> E
  H["Human multi-axis adjudication"] --> B["Baseline readiness/evaluation"]
  E --> F["Specialist features and late fusion"]
  E --> X["Exact per-modality retrieval"]
  F --> U["OOD, disagreement, conformal restraint"]
  B --> G["Immutable bundle gate"]
  X --> G
  U --> G
  G --> V["Read-only v3 metadata and frozen routing records"]
```

The arrows are evidence lineage and gating relationships, not a claim that every component is active in production.

## 1. Point-in-time event memory

The event store accepts only typed, immutable facts with raw provenance and source reliability:

- `MarketStateSlice` for market observations;
- `PublicDocumentClaim` for archived public evidence;
- `OnChainSettlementFact` for restricted verified chain facts.

Each fact has an `event_time`, `ingested_at`, raw artifact hash/UID, source URL where applicable, and source reliability. A snapshot at `as_of` includes only records whose event and availability clocks are no later than the cutoff. Later retrieval or publication cannot be backfilled into a historical decision.

Market context is a separate raw-lineaged contract: question, category, outcomes, documented siblings, scheduled-event controls, and resolution schedule. A scheduled future event is allowed as context only if the control was already observed and ingested before the decision cutoff. It does not label a movement, assign suspicion, or make an activity diagnostic scorable by itself.

## 2. Labels and human adjudication

Phase 15 uses a multi-axis human ontology instead of a binary fraud label:

- observable mechanism, such as `thin_liquidity_artifact`, public-information response, scheduled-event response, underlying-reference move, sibling repricing, or `unexplained_activity`;
- evidence strength: absent, conflicting, limited, corroborated, unknown, or unmapped;
- human disposition: benign mechanical/public response, escalate for review, insufficient evidence, unresolved, unknown, or unmapped.

`unknown` and `unmapped` are never negatives or training-eligible labels. Optional external legal outcomes are audit-only and cannot make a case trainable. The LLM, when used elsewhere for extraction or chronology formatting, is not a ground-truth generator or final decision authority.

## 3. Market features: no Phase 15 hard liquidity cutoff

The legacy v2 `thin_liquidity_threshold` remains a v2 diagnostic tag used with its causal detector. It should not be interpreted as a Phase 15 fraud threshold, an exclusion rule, or an approval rule.

Phase 15 feature snapshots are causal five-minute buckets with continuous price change, fill count/notional/size, top-of-book spread, bid/ask depth, depth imbalance, freshness, source watermarks, and explicit missingness. This lets a later, separately evaluated model distinguish conditions such as a sparse market print and a move in a liquid market without a hard-coded `if volume < threshold` decision.

## 4. Specialist baseline and neural readiness gates

The first evaluable model path is calibrated, interpretable specialist baselines and late fusion. A candidate can be evaluated only after the as-of event snapshot and features meet all applicable gates:

- no future or later-ingested facts;
- complete required coverage and context;
- human adjudications present and training eligible;
- fixed forward, event-cluster-, market-, and actor-disjoint partitions;
- explicit baseline labels and a fixed plan;
- calibration counts and class coverage sufficient for the frozen gate.

Even `ready_for_candidate_evaluation` does not allow serving. Candidate metadata is unapproved and unpublished by construction.

The shared/private event-space neural module is experimental. It accepts precomputed numeric modality values and masks, retains private modality features alongside a shared representation, and applies late fusion. It has no default weights, does not collect data, and does not silently replace an unavailable runtime with a heuristic. Protected execution requires an explicitly approved bundle; no current workflow creates that approval.

## 5. Retrieval before ANN

Retrieval uses separate modality indexes and exact `faiss.IndexFlatIP` search over already-produced embeddings. Metadata is filtered by time, source coverage, provenance, and modality before results are returned. Retrieval is case/context support, not a labeler.

HNSW and IVF-PQ are deliberately unavailable. Promote an ANN implementation only after it meets predeclared gates against exact Flat retrieval: Recall@K, temporal holdouts, provenance behavior, calibration/selective-risk behavior, and missing-modality behavior. A faster index is not a substitute for validated retrieval geometry.

## 6. Restraint layer: OOD, abstention, and conformal routing

The restraint layer can abstain for insufficient observed modalities, weak support, cross-modality disagreement, unavailable calibration, insufficient OOD reference data, or an out-of-distribution result. OOD scoring is based on an explicit reference population using k-nearest-neighbor distance and an energy-like distance measure; its score is not a probability of fraud.

The rolling conformal controller uses only analyst feedback available by `as_of`. It withholds escalation when feedback history is insufficient, adjusts a conservative operational threshold from unsupported escalations, and respects an analyst daily queue budget. Its `supported` feedback field means useful to the analyst workflow, not a legal or misconduct label.

## 7. Immutable bundles and read-only v3 serving

An immutable serving manifest binds SHA-256 hashes for the dataset, feature specification, model artifact, calibration, OOD, conformal, retrieval, and code. Repository verification reads JSON pointer/manifest metadata and checks the pointer identity; it does not deserialize weights.

The v3 API is intentionally one-way and read-only:

- readiness and model-status endpoints expose verified identifiers and policy state;
- assessments expose only precomputed, frozen routing or abstention records;
- retrieval and event endpoints expose safe lineage summaries and point-in-time references;
- no endpoint trains, loads model bytes, builds an index, performs live inference, calls an LLM, exposes raw evidence, or approves a bundle.

`MARKETLEAK_V3_BUNDLE_ROOT` is a server-side path option only. A valid manifest still reports policy-blocked until independent operational gates are attested.

## 8. `cli_v3` operator commands

`cli_v3` is local and read-only. It consumes one JSON object with `events`, `features`, and optional `plan`, then prints one stable JSON object. It does not connect to services, write files, train/persist weights, publish a candidate, or serve a model.

```powershell
python -m marketleak.cli_v3 assemble `
  --input data/phase15/request.json `
  --as-of 2026-07-13T12:00:00Z

python -m marketleak.cli_v3 readiness `
  --input data/phase15/request.json `
  --as-of 2026-07-13T12:00:00Z
```

The compatibility-named `train-baseline` command performs an in-memory naive-baseline candidate evaluation only after readiness. It does not train a persisted production model, write artifacts, publish a bundle, or enable an API inference endpoint.

```powershell
python -m marketleak.cli_v3 train-baseline `
  --input data/phase15/request.json `
  --as-of 2026-07-13T12:00:00Z
```

Treat `not_ready`, missing calibration, or any abstention reason as an outcome to preserve, not an error to override.

## 9. Operator sequence

1. Configure only documented official/public sources and retain raw artifacts before parsing.
2. Maintain a persistent public-evidence archive before using public-explanation coverage claims.
3. Capture market context and documented reference mappings; do not use a generic price feed as a settlement source.
4. Build the as-of event snapshot and continuous market features with explicit missingness.
5. Obtain independent human multi-axis adjudications; preserve unknown/unmapped cases.
6. Run `cli_v3 assemble` and `readiness`; record every reason code.
7. Evaluate baselines only on frozen forward disjoint partitions if gates pass. Compare any neural proposal to the baseline on unseen markets, time holdouts, calibration, and selective risk.
8. Keep retrieval exact until ANN promotion gates pass. Keep v3 on metadata/frozen routing only until artifact, provenance, label, calibration, OOD, conformal, and human-policy gates are all independently satisfied.

The system should remain unavailable or abstain whenever that sequence lacks evidence.
