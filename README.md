# MarketLeak

MarketLeak is a validation-first prediction-market surveillance research system. It records unusual market activity and the evidence needed for human review; it does not produce fraud findings, legal conclusions, or an identity claim.

Current status: **`effectiveness_unknown`**. Phase 15 supplies an auditable multimodal event-memory, retrieval, restraint, and read-only serving foundation. It does not establish that any model predicts fraud or misconduct. There are not yet enough independently adjudicated, market-mapped cases or prospective operating results to make that claim.

## Safety boundary

The system keeps three questions separate:

1. **A — market activity:** was a movement unusual under causal market-state controls?
2. **B — public explanation:** did matching public information exist in the monitored point-in-time archive before the movement?
3. **C — actor/access evidence:** is independently sourced actor context available?

None of A, B, or C alone proves intent or misconduct. `unknown`, `unmapped`, missing data, incomplete coverage, and unavailable sources stay explicit; they are not negative evidence.

The v2 `thin_liquidity_threshold` is a diagnostic tag for the legacy v2 pipeline. It is not a fraud rule and is not a Phase 15 input cutoff. Phase 15 retains continuous volume, depth, spread, price-action, freshness, and missingness features so that later, separately evaluated models can learn their relationships without a hard thin-market exclusion.

## Install and inspect

Requires Python 3.11.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Copy-Item .env.example .env
python -m marketleak.cli_v2 capabilities
```

`.env.example` contains names only. Keep all credentials on the server; never put a private key, Kalshi signature, RPC secret, Slack token, or bundle path in browser code, a raw receipt, or a checked-in configuration file.

## What is available now

- Official-source v2 collection writes immutable raw objects, retrieval receipts, canonical records, quarantine records, and coverage claims.
- Persistent public-evidence collection is raw-first and records `first_seen_at`; it has no default source list.
- v2 prospective shadow runs freeze input, code/configuration, coverage, cutoff, and an append-only hash-chained ledger.
- Phase 15 has a point-in-time event store, multi-axis human labels, exact per-modality retrieval, feature assembly, experimental shared/private neural architecture, OOD and conformal restraint primitives, immutable bundle manifests, and a read-only v3 metadata surface.

The Phase 15 components are gated infrastructure and research primitives. They do not activate a production predictor by themselves.

## Bounded v2 collection

Collection performs no network work until an explicit command is run. Every response body and receipt are persisted before parsing.

```powershell
python -m marketleak.cli_v2 collect-once `
  --platform kalshi `
  --kalshi-ticker KXEXAMPLE `
  --page-size 100 `
  --max-pages 1 `
  --output-dir data/v2
```

```powershell
python -m marketleak.cli_v2 collect-evidence-once `
  --config configs/evidence/sources.json `
  --output-dir data/evidence `
  --max-pages 1 `
  --max-entries 100
```

See [data and evidence operations](docs/data-and-evidence.md) for the source-capability and lineage rules.

## Phase 15: read-only orchestration

`cli_v3` accepts a local JSON request, freezes the supplied facts at `--as-of`, and prints one JSON result. It does **not** contact a service, retrieve a live index, write artifacts, train/persist weights, approve a candidate, or serve a model.

```powershell
python -m marketleak.cli_v3 assemble `
  --input data/phase15/request.json `
  --as-of 2026-07-13T12:00:00Z

python -m marketleak.cli_v3 readiness `
  --input data/phase15/request.json `
  --as-of 2026-07-13T12:00:00Z
```

`train-baseline` is retained as a compatibility command name for an in-memory candidate-evaluation workflow. It does not publish, serialize, deploy, or serve a model.

The full architecture, gates, and operating workflow are in [Phase 15 multimodal architecture](docs/phase15-multimodal.md).

## Frozen v2 shadow runs

Prospective shadow is engineering validation, not proof of effectiveness:

```powershell
python -m marketleak.cli_v2 shadow-prospective `
  --data-root data `
  --run-dir output/shadow/2026-07-13 `
  --as-of 2026-07-13T03:00:00Z `
  --coverage-status partial `
  --coverage-limitation "collection began after the preregistered window"
```

See [validation and shadow protocol](docs/validation-and-shadow.md) for the label, calibration, and release gates.

## Read-only v3 API surface

When a hash-verified immutable bundle pointer is present, the API may expose metadata only:

- `GET /api/v3/readiness`
- `GET /api/v3/model-status`
- `GET /api/v3/assessments`
- `GET /api/v3/retrieval/{event_uid}`
- `GET /api/v3/events/{event_uid}`

These endpoints never load model weights, perform live inference, call an LLM, return raw evidence bodies, or turn a verified manifest into an approval. Every response preserves `not_proof_of_fraud=true` and `effectiveness_unknown=true`.

## Tests

```powershell
pytest -q --basetemp pytest-tmp-current
```

Further reading: [v2 architecture](docs/architecture-v2.md), [data and evidence](docs/data-and-evidence.md), [Phase 15 architecture](docs/phase15-multimodal.md), and [validation and shadow](docs/validation-and-shadow.md).
