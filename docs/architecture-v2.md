# Validation-first architecture

## Safety boundary

MarketLeak v2 is a market-surveillance pipeline for human review. It does not expose an aggregate fraud verdict. A, B, and C remain separate through domain records, labels, evaluation, APIs, reports, and shadow ledgers.

```text
official response -> raw hash + receipt -> strict normalization -> quality gate
    -> A activity diagnostics
    -> B point-in-time public explanation
    -> C independently sourced actor/access context
    -> human review / append-only adjudication
```

Failure or unavailable evidence produces `not_scorable`, `unknown_coverage`, `no_actor_data`, `unknown`, or `unmapped`; it does not silently become a negative or incriminating signal.

## Canonical contracts

`marketleak.domain` separates `PriceObservation`, `TradeFill`, and `OrderBookSnapshot`. Decimal prices and sizes are preserved, timestamps are timezone-aware UTC, identifiers are platform-qualified, and every canonical record includes source and raw-artifact lineage.

`PriceObservation` never implies a fill. `TradeFill` requires an explicit side. Actor availability is an enum, not an empty string interpreted downstream. An unattributed Kalshi trade remains a price observation; MarketLeak does not invent a wallet or direction.

## Detection and controls

A is based on causal, bucketed diagnostics with minimum-history and liquidity checks, multiple-testing control, event clustering, sibling-market controls, and explicit unscorable states. Diagnostic scores are not probabilities of fraud.

B uses only sources archived by first-seen time. Retrieval time and publication time are distinct. Absence claims require complete coverage for the relevant query and interval.

C accepts graph claims only when their provenance is independent of the alert. An edge created because an alert fired cannot be used to support the same alert. Access context, affiliation, and wallet activity remain contextual evidence, not proof of intent.

## Compatibility boundary

Legacy fixtures and endpoints remain read-only compatibility inputs. The v2 audit reports their defects and produces no actor or public-information conclusions. New collection writes under `data/v2`; it never overwrites `demo_data`.

## Local graph persistence boundary

The contextual graph and proxy-cluster cache use versioned, canonical JSON artifacts (`demo_data/graph.json` and `demo_data/cache_clusters.json`). Reads enforce byte, record, nesting, container, and string bounds and validate exact schemas before constructing a graph or returning a wallet-to-cluster mapping. Writes use a flushed temporary file followed by an atomic replacement, so an interrupted replacement leaves the prior artifact authoritative.

Legacy `.pkl` and `.pickle` artifacts are unsupported and are never opened or deserialized. Their presence may be reported for operator cleanup, but no runtime surface silently migrates or trusts them. Missing artifacts produce an unavailable state; corrupt, oversized, non-canonical, or unknown-schema artifacts produce a corrupt/unavailable state rather than an empty-evidence claim.

Graph availability is propagated separately from graph contents. Only a successfully loaded canonical artifact may report `empty_graph_observed=true` or support a zero-path interpretation. Missing and corrupt artifacts report `empty_graph_observed=null`, `absence_claim_eligible=false`, and suppress graph enrichment. Live context collected during that request may still be displayed as partial context, but it cannot repair or disguise the missing persisted graph. Proxy clustering likewise refuses to replace its cache unless the persisted graph was verified.
