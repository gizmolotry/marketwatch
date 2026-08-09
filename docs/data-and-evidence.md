# Data and evidence operations

## Raw-first rule

Every admitted source delivery is captured before parsing. The immutable raw object is addressed by SHA-256, and each retrieval receives a separate UTC receipt recording the request and response metadata. A parser only sees a payload after both records exist. Exact replay may reuse the raw object but creates a new receipt; a semantic UID conflict is quarantined rather than overwritten.

Transport integrity gates run before durable capture. HTTP sources must use an explicitly approved HTTPS origin on the default port, without userinfo, and resolve only to public destinations. The approved resolution is passed to the transport and the TLS socket connects directly to those addresses in deterministic order while preserving the hostname for SNI and certificate verification; a second hostname lookup is not used for any connection attempt. Sanitized failures may advance to the next already-approved address, but may never discover a new address. Redirects are disabled, and an effective URL, attested peer address, or pagination link outside the bounded approved origin policy is rejected. A missing peer attestation is non-evidentiary and fails before raw capture. This connection binding prevents a public DNS answer from being replaced by a private, loopback, link-local, or reserved destination between validation and connection.

Each HTTP source declares `max_response_bytes`. The transport reads at most that limit plus one byte. An oversized delivery is recorded as explicitly unavailable/incomplete but its body is neither materialized in full nor written as a raw object or receipt. This is the intentional exception to raw-first capture: an untrusted delivery must pass the connection and byte-bound integrity gates before it becomes durable evidence.

Receipts contain only allowlisted non-secret headers, parameter names rather than values, explicit redaction state, and origin-only URLs. Paths and queries are excluded because either can contain credentials or signed values. Error sanitation slices string/byte input to an 8,192-character work bound before any regex pass and never invokes conversion methods on arbitrary objects. It then applies ordered fail-closed passes for Authorization, `Cookie`, `Set-Cookie`, custom sensitive headers, and assignment/JSON forms. Supported sensitive names include case-insensitive hyphen/underscore variants and containing header names for cookie, refresh/access token, client secret, credential(s), session/session ID, API key/apikey, signature, password/passwd, secret, and token. Quoted or unquoted values and Bearer/Basic scheme-plus-value forms are removed before errors cross an API or log boundary. Clear context after an unambiguous assignment delimiter is retained; an ambiguous sensitive header value is redacted through end-of-line. Arbitrary transport exceptions are converted outside the active exception handler to a type-only safe error with no retained cause, context, message, or unsafe representation. Response `Set-Cookie` and authentication headers are never retained.

The same rule applies to HTTP market collection, public-document collection, and the injectable Kalshi WebSocket adapter. Coverage is asserted per source/interval; it is never inferred from the absence of data.

## Canonical market source capabilities

| Source | Available market data | Actor/account visibility | Important limit |
|---|---|---|---|
| Polymarket Data API | Public fills with explicit BUY/SELL side, price, size, condition ID, and transaction hash when sent | Public `proxyWallet` only | The wallet is a venue-exposed pseudonymous actor, not a person, owner, maker/taker identity, or control claim. |
| Polymarket CLOB | Current token-level order-book snapshot | Unavailable | Polling begins local history; it cannot recreate past L2. |
| Kalshi REST trades | Public trades and price observations | Unavailable | Direction is retained only if explicitly exposed. |
| Kalshi REST book | Current YES/NO bid depth | Unavailable | It is aggregate depth, not owned orders. |
| Kalshi WebSocket | `trade`, `ticker`, lifecycle, and authenticated aggregate `orderbook_delta` | Unavailable | Live streams can have reconnect gaps; deltas are not synthetic snapshots. |

Raw artifacts and receipts live under the configured collection roots, for example:

```text
data/v2/raw/objects/sha256/       exact response/message payloads
data/v2/raw/receipts/             retrieval and receive-time records
data/v2/normalized/               canonical records
data/v2/quarantine/               UID/content conflicts
data/v2/coverage/ledger.jsonl     complete/partial/unavailable coverage claims
data/evidence/raw/                public-document payloads and receipts
data/evidence/normalized/         append-only normalized revisions
data/evidence/coverage/ledger.jsonl  hash-chained source attempts and gaps
```

## Kalshi WebSocket credential and data boundary

Kalshi requires an authenticated WebSocket handshake even for public market-data channels. Configure explicit market tickers and create the documented timestamp/signature server-side for each connection. The required access key, signature, private-key material, and any socket client configuration must stay in server process memory or a server secret manager.

Never send those values to a browser, include them in client-side environment variables, or write them into raw receipts. The collector deliberately records only non-secret transport metadata and raw incoming messages. Public trade messages do not expose accounts or order owners. `taker_outcome_side`, legacy `taker_side`, and `taker_book_side` are retained exactly when present; absent direction is not invented. Aggregate `orderbook_delta` is an authenticated L2 data channel, not account data.

The collector has bounded reconnect attempts and reports incomplete collection rather than silently claiming continuous coverage. It is transport-injected and does not open a live connection merely by import or construction.

## Polymarket public-wallet population intake

Polymarket is the current venue with a viable public, pseudonymous actor domain: each public trade record can expose a `proxyWallet`. Kalshi remains useful for market mechanics, price, and aggregate-book controls, but its documented public trade and book surfaces do **not** expose an account or wallet actor. The two venues therefore must not be treated as equivalent sources of wallet behavior.

`python -m marketleak.cli_v2 collect-polymarket-population` is an implemented, bounded backfill for the public trade population of **one exact `conditionId`**. It requires an inclusive frozen whole-second `start` and `end`, always sends `takerOnly=false`, and does not offer a broader event, multi-condition, continuation, or taker-only option. The Data API documents an offset limit of 10,000 and directs clients needing deeper history to use `start`/`end` windows; it also documents `takerOnly` as defaulting to true, the `BUY`/`SELL` side filter, and only an approximate market/event historical horizon. Its exact retained cutoff is unknown. A positive `start` can extend only a user-scoped query, not establish market/event retention. See the official [trades endpoint](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets).

Before the command can run, the operator supplies two canonical JSON inputs that are deliberately separate from query arguments:

- `--source-bound` identifies a raw/receipt-bound capture of the official Polymarket `/trades` contract **and** a raw/receipt-bound capture of the exact Gamma market metadata. The metadata must bind the requested condition ID and Gamma market ID. Its `acceptingOrdersTimestamp` supplies only a market-activity lower bound; it is never represented as a Data API retention floor. The request starts no earlier than that activity bound and ends no later than the later of the two captured observations.
- `--approved-contract-sha256-file` is a separately governed canonical allowlist of operator-approved official-contract snapshot SHA-256 values. A captured document is evidence-bound but is not automatically an approved policy input. Tampered raw objects, receipts, metadata identity, non-2xx contract receipts, or an unapproved snapshot fail closed.

The collector records the entire query tree. Every fetched leaf binds the exact source request and every response-bearing retry attempt's status, raw delivery, and authenticated receipt. The successful raw page is re-normalized by the production parser and must exactly equal the ordered returned fills; invalid `proxyWallet` values and non-exact source `BUY`/`SELL` values fail closed. When paging saturates, the collector recursively partitions a window into disjoint inclusive second ranges. If a one-second range still saturates, it performs separately auditable `BUY` and `SELL` requests. An unsupported or still-saturated side partition is an **`irreducibly_partial`** terminal leaf. `max_requests` bounds logical calls, `max_http_attempts` bounds all response attempts including retries, and `max_leaves` bounds the query tree. An exhausted bound is an explicit **`budget_exhausted`** leaf, not a hidden gap.

Every population manifest has `complete=false`, because the Data API market-query retention floor is unknown/approximate. It reports `query_exhausted_coverage_limited` only if every terminal query is exhausted and the parent/child source reconciliation is consistent; it reports `partial` for budget exhaustion, irreducible leaves, or source inconsistency. A fetched terminal leaf can append a `CoverageLedger` row only with its actual retrieval time, exact filters, continuation, and raw hashes; all such rows remain incomplete. An unfetched budget leaf has none of those observations, so it is returned as an explicit unrecorded interval and is never forged into the ledger.

This obtains evidence about publicly exposed venue actors only. It neither discovers all Polymarket activity beyond the selected condition/time range nor identifies the human behind a `proxyWallet`. A Polygon address, transaction hash, funding proximity, or a Bitcoin address is not a substitute for the exact canonical-fill join required for on-chain corroboration, and none establishes wallet ownership or common control.

## Point-in-time public evidence

No public source is enabled by default. Copy `configs/evidence/sources.example.json`, declare each monitored source, and collect before evaluation:

```powershell
python -m marketleak.cli_v2 collect-evidence-once `
  --config configs/evidence/sources.json `
  --output-dir data/evidence `
  --max-pages 1 `
  --max-entries 100
```

Public documents retain content hash, canonical URL, claimed publication time when provided, `first_seen_at`, retrieval time, and coverage linkage. Backfilled publication dates can provide context but cannot prove historical observability. A negative public-explanation claim is unavailable when archive coverage is partial, unavailable, unknown, or unverified.

## Market context and reference prices

Phase 15 context records are raw-lineaged snapshots of the question, category, outcomes, explicit sibling-market relationships, scheduled-event controls, and resolution schedule. They have event, first-seen, retrieved, and ingested clocks. A future calendar event is admissible only as a control already known at the historical cutoff; it is not a label for a price movement.

Reference prices fail closed. A price observation must name a per-market documented settlement/reference mapping, its settlement rule URL, documented primary-source URL, raw lineage, primary-source availability fact, and source reliability. The mapping and the price's event/first-seen/retrieved/ingested clocks must all precede `as_of`.

If no documented source exists, the result is `missing_documented_settlement_source`. If a source is unavailable, ambiguous, or later than the cutoff, the result remains unavailable. Do not substitute a convenient BTC/USD, exchange, oracle, or any other price just because the underlying asset symbol appears related. Example-only templates are in:

- `configs/phase15/market_context.example.json`
- `configs/phase15/reference_sources.example.json`

## On-chain corroboration boundary

For Polymarket settlement corroboration, accept only raw-verified Polygon (chain ID 137) `OrderFilled` facts that exactly join an existing canonical fill and retain raw provenance. `TransferSingle` logs do not supply an execution price and must not be converted into a price/fill using a placeholder. Chain paths and sums are computed by code, not inferred by a language model.

The population collector's venue `proxyWallet` is useful only as the public pseudonymous actor field carried by that canonical fill. Polygon activity is corroboration only after the exact join; it is not a second, interchangeable actor-attribution source and cannot repair absent public-wallet visibility.

Bitcoin data is contextual-only in this system. It is not an attribution source, settlement proof, price source, or a way to connect a wallet to a market participant. Generic Bitcoin claims, addresses, and graph heuristics cannot enter the Polygon `OrderFilled` corroboration path.

## Source outages and missingness

Partial coverage, disconnects, unavailable sources, absent accounts, missing modalities, and unverifiable raw lineage are first-class facts. They can cause an abstention or a blocked readiness status. They must not be silently zero-filled, treated as no activity, or used to imply a person acted.

Historical enforcement-case research must also preserve the distinction between facts available at the decision cutoff and facts retrieved later. See [Historical wallet case replay](historical-wallet-case-replay.md) for the prospective-versus-forensic contract, legal-audit boundary, and bounded public-wallet examples.
