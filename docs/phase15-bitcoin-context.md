# Phase 15 Bitcoin context boundary

## Purpose

Bitcoin information is optional, read-only public-chain context. It can record a raw-lineaged public fact such as a block timestamp, a transaction confirmation, or observed public address activity when collection has been explicitly configured. It is not a person profile, wallet-risk service, identity resolver, fraud indicator, or market-participant attribution system.

The `/bitcoin-context` UI has no address input or search field. It displays only the policy-redacted, precomputed snapshot state, its decision/publish timestamps, and neutral attachment UIDs returned by the read-only v3 context surface. It intentionally does not render address text, calculate a score, create a relationship, or modify a watchlist.

## Public chain fact is not ownership or intent

An address appearing in a public Bitcoin transaction does not establish:

- who controls the address;
- whether a person, institution, or market participant owns it;
- access to nonpublic information;
- intent, coordination, fraud, misconduct, or legal responsibility.

Address labels, clustering heuristics, balance size, transaction size, or exchange guesses must not be used to turn public Bitcoin data into any of those claims. Missing data is unavailable context, not benign or adverse evidence.

## Configuration and documented links

Start from `configs/phase15/bitcoin_watchlist.example.json`. It contains one intentionally non-real placeholder BTC address and a separate opaque `address_uid`. Production entries require all of the following before facts can be admitted as market context:

1. an explicitly scoped, server-side collection configuration;
2. raw artifacts and UTC receive/retrieval provenance for every collected public fact;
3. an independently documented external link with source UID, raw artifact UID, content hash, first-seen time, and public source URL;
4. explicit human review of the link provenance.

The external link must be documented evidence, not an automatic address match, an LLM inference, a heuristic cluster, or a graph path created by an alert. Without it, the correct state is `required_before_context_admission` or unavailable.

## Prediction-market settlement boundary

Polymarket settlement corroboration is restricted to raw-verified Polygon (chain ID 137) `OrderFilled` events that exactly join a canonical fill. Bitcoin is not a Polymarket settlement source, does not supply a replacement execution price, and cannot substitute for the Polygon rule. `TransferSingle` logs and placeholder prices are also excluded from settlement corroboration.

Therefore a Bitcoin fact can be displayed only as contextual public-chain evidence after its documented external link passes review. It cannot itself link a Bitcoin address to a Polymarket account, identify a trader, or support a conclusion about any event.

## Read-only v3 UI contract

The UI requests `GET /api/v3/bitcoin-context`. The endpoint returns only neutral, policy-redacted metadata: `status`, `reason`, `snapshot_uid`, `decision_as_of`, `published_at`, and `contexts[]`. Each context contains a redacted `address_uid`, an `attachment_status`, and only the precomputed Polygon-address, market-address, and event target UIDs. It does not return source artifacts, evidence bodies, raw address text, ownership claims, or a link explanation. A missing endpoint, missing watchlist, unavailable source, or context published after the cutoff renders as unavailable; the UI must not invent a fallback.

The endpoint and UI must never accept a user-supplied address, expose server-side collection credentials, return an ownership/risk score, or initiate a live chain lookup. Every displayed status remains context only and not proof of fraud, identity, access, intent, or misconduct.
