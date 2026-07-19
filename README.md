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

## Recorded real-case demo

The repository includes one tracked, hash-checked safe review packet at [configs/phase15/polymarket_btc65k_review_case.json](configs/phase15/polymarket_btc65k_review_case.json), exported from a local bounded public Polymarket/Binance co-capture for “Will Bitcoin reach $65,000 in July?” The tracked packet includes the frozen trigger, coverage and abstention state, redacted lineage identifiers, individual content hashes, and a canonical `review_cases_sha256` value.

The raw capture directory and its 319 raw receipt objects are local runtime data and are not shipped in a fresh clone. The `review_cases_sha256` value verifies internal consistency between the embedded review-case list and its digest only. It does not establish authenticity, authenticate a publisher, prove source provenance, or verify any unshipped raw artifact. A clone can validate the safe packet's schema and canonical inner hash, but cannot independently re-hash every raw object named by its lineage. This is recorded public data, not a simulated effectiveness example. The observed market price moved from `0.60` to `0.72` across the selected window, but there are no verified fills or order-book snapshots in the packet. Public-evidence coverage is unavailable and market-mechanics coverage is partial, so the recorded decision is `abstain_insufficient_evidence`. It is not evidence of fraud, intent, identity, or model effectiveness.

### Inspect the frozen packet with the CLI

```powershell
python -m marketleak.cli_v3 review-case `
  --config configs/phase15/polymarket_btc65k_review_case.json `
  --as-of 2026-07-14T00:38:53.861000Z `
  --case-uid "review:polymarket-btc65k-cocaptured"
```

This command validates the configuration hash and causal cutoff, then prints the precomputed safe packet. It performs no collection, model loading, or inference.

### Run the read-only API and UI

In a PowerShell terminal at the repository root:

```powershell
$env:MARKETLEAK_V3_REVIEW_CASES_CONFIG = (Resolve-Path "configs/phase15/polymarket_btc65k_review_case.json").Path
python -m uvicorn marketleak.api:app --host 127.0.0.1 --port 8000
```

With the API running, inspect the list or the exact case:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v3/review-cases" |
  ConvertTo-Json -Depth 20

Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v3/review-cases/review%3Apolymarket-btc65k-cocaptured" |
  ConvertTo-Json -Depth 20
```

In a second terminal:

```powershell
Set-Location ui
npm ci
npm run dev
```

Open [http://localhost:5173/evidence-review](http://localhost:5173/evidence-review). The browser reads the precomputed API packet; it does not receive credentials, fetch live market data, or perform inference.

### Phase 15 orchestration is a separate research path

The `assemble`, `readiness`, and compatibility-named `train-baseline` commands require a caller-created JSON document containing `events`, `features`, and an optional evaluation `plan`. The repository does not ship a `data/phase15/request.json`, and the recorded review-case configuration is intentionally not an orchestration request. `train-baseline` performs only an in-memory candidate evaluation; it does not publish, serialize, deploy, or serve a model.

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
- `GET /api/v3/review-cases`
- `GET /api/v3/review-cases/{case_uid}`
- `GET /api/v3/retrieval/{event_uid}`
- `GET /api/v3/events/{event_uid}`

These endpoints never load model weights, perform live inference, call an LLM, return raw evidence bodies, or turn a verified manifest into an approval. Every response preserves `not_proof_of_fraud=true` and `effectiveness_unknown=true`.

## Tests

```powershell
$testTemp = Join-Path $env:TEMP "marketwatch-pytest"
pytest -q --basetemp $testTemp
```

Keeping pytest's temporary tree outside the checkout preserves tests that require a genuinely non-repository directory.

Further reading: [v2 architecture](docs/architecture-v2.md), [architecture blueprint](docs/architecture-blueprint.md), [data and evidence](docs/data-and-evidence.md), [Phase 15 architecture](docs/phase15-multimodal.md), [20-hour sprint plan](docs/SPRINT-20H.md), [Phase 15 Gherkin contract](specs/phase15_multimodal.feature), and [validation and shadow](docs/validation-and-shadow.md).
