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
