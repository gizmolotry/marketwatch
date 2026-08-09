# Polymarket public-wallet population and observational corpus contract

## What is implemented

MarketLeak can collect a bounded population of public Polymarket trades for one exact condition ID and a frozen inclusive whole-second interval:

```powershell
python -m marketleak.cli_v2 collect-polymarket-population `
  --output-dir data/polymarket-population `
  --condition-id 0x<64-hex-condition-id> `
  --start 2026-07-01T00:00:00Z `
  --end 2026-07-01T23:59:59Z `
  --source-bound path/to/source-bound.json `
  --approved-contract-sha256-file path/to/approved-contracts.json
```

The collector is deliberately narrower than a generic API query. It fixes one `conditionId`, always uses `takerOnly=false`, and requires the lower and upper time bounds before network activity. It also requires a `--source-bound` evidence envelope and a separately governed approval policy. The envelope carries raw objects and receipts for the official Polymarket `/trades` contract snapshot and exact Gamma market metadata; the approval-policy file names the allowed official-contract snapshot SHA-256 values. Gamma `acceptingOrdersTimestamp` supplies only the condition-specific **market-activity lower bound**. It is not a Data API retention floor. The exact market-query retention floor remains unknown/approximate, so this bound cannot establish all-time coverage. The end bound cannot be later than the captured source observation.

Every response-bearing HTTP attempt is captured before parsing. A successful page carries all preceding retryable-response captures and receipts, in attempt order, into its batch, leaf, and manifest. `max_requests` bounds logical page/filter calls; `max_http_attempts` separately bounds the total responses including retries. The collector re-runs every successful raw trade row through the production Polymarket parser and requires exact ordered equality with the connector's returned canonical fills, including source, parser, clocks, actor visibility, and lineage. A malformed `proxyWallet` or a source side other than exact uppercase `BUY`/`SELL` fails closed.

The [official Polymarket trades endpoint](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets) documents the key external constraints that this contract accounts for:

- `offset` is capped at 10,000; deeper retrieval must use `start`/`end` windows;
- `takerOnly` defaults to `true`, so both roles require an explicit `false`;
- `BUY` and `SELL` are the documented side filters;
- market- and event-scoped queries have only an approximate historical horizon; the exact retained cutoff is not promised. A user-only query may request deeper history with a positive epoch, but that does not establish or extend market/event retention.

When an interval saturates page retrieval, MarketLeak splits it into disjoint inclusive subwindows. If one second remains saturated, it separately queries `BUY` and `SELL`. If the connector cannot perform that partition, or either side remains saturated, the terminal leaf is `irreducibly_partial`. The hard logical-request, total-HTTP-attempt, and leaf bounds create explicit `budget_exhausted` leaves instead of silently widening or dropping work. Each fetched leaf binds every attempt's exact request parameters, status, receipt, and raw-delivery hash; parent/child reconciliation is checked and any disagreement remains a source inconsistency.

No market/event manifest is complete: the Data API retention floor is unknown/approximate. A fully exhausted, source-consistent query is reported as `query_exhausted_coverage_limited`; a run with budget, saturation, or consistency limitations is `partial`. A fetched terminal leaf may append an incomplete `CoverageLedger` row only with its actual retrieval time, exact filters, continuation, and raw hashes. An unfetched budget leaf has no observed delivery and remains an explicit unrecorded interval in the command output/manifest, never a forged ledger row. Manifest bytes publish through a durable temporary file and no-overwrite atomic winner; the winner bytes and SHA-256 are reread before reporting success.

## What actor data means

Polymarket provides a public `proxyWallet` on its trade records. That makes Polymarket the current primary source for pseudonymous, venue-exposed actor behavior. It supports questions such as “what did this public actor do in this declared market/time population?”

It does not answer who controls that wallet, whether several wallets share control, whether the actor is a particular person, or whether the actor had access to nonpublic information. The public response can contain a transaction hash, but Polygon facts are usable only after an exact join to a canonical venue fill. On-chain proximity, transfers, and Bitcoin data are not substitutes for that join and do not create an attribution.

Kalshi remains a useful market-level source for public trades, price observations, and aggregate order-book state. Its public interfaces expose no account or wallet actor, so it cannot presently produce a Kalshi wallet cohort or be merged into a Polymarket wallet population.

## Freezing local observational data

`freeze_observational_trade_fills` accepts existing local canonical `TradeFill` files plus explicit raw and coverage roots. It performs no network I/O. Its immutable manifest hashes:

- the selected canonical source and record sets;
- raw object and matching receipt sets;
- exact selection policy and coverage-ledger rows;
- source/parser/schema versions and the freezer source hash.

It fails closed for malformed/noncanonical records, missing or corrupt raw objects, missing matching receipts, and conflicting fill UIDs. Re-verification repeats the exact freeze and requires byte-identical manifest content. A clean integrity result only authenticates the selected local evidence; it does not repair coverage, add labels, or train a model.

The current local exploratory corpus contains 630,277 public fills. It is intentionally not committed to this repository. Its 238 relevant coverage records are all incomplete, so its valid frozen status is observational `ready_with_limitations`, not complete. It is not a fraud dataset, lacks adjudicated outcomes, and does not substantiate model effectiveness, precision, recall, calibration, or wallet ownership. Test data in this repository remains minimal mechanics fixtures, never a substitute for the local corpus or evidence of effectiveness.

## What must happen before training claims

The corpus freeze is a repeatability boundary, not a training run. Before any trained model or effectiveness claim, MarketLeak still needs a separately governed adjudicated case corpus, declared training eligibility, complete required coverage for each analysis, forward and cluster-disjoint partitions, frozen feature/model/calibration plans, and prospective evaluation. The repository currently has no trained production neural weights or validated fraud-probability model.
