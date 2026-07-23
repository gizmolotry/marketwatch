@target_design @not_current @polymarket @wallet_tracking @market_integrity
Feature: Point-in-time Polymarket signal-to-wallet collection
  The system may expand a data-quality-eligible Polymarket market signal into
  bounded collection around publicly exposed pseudonymous wallets. It records
  factual market and chain behavior, produces a decisive signal priority, and
  leaves final disposition to a human reviewer.

  Background:
    Given the decision cutoff is "2026-07-19T12:05:00Z"
    And all source timestamps are timezone-aware UTC
    And every Polymarket delivery is retained before normalization with an immutable raw object and retrieval receipt
    And a wallet identifier denotes only the venue-exposed pseudonymous wallet role
    And a human reviewer owns final identity and misconduct disposition

  Rule: Only attributable market activity can enroll a wallet

    @enrollment @actor_visibility
    Scenario: Enroll public proxy wallets from genuine fills in an eligible signal window
      Given a data-quality-eligible market signal "signal-17" has an incident window from "2026-07-19T12:00:00Z" to "2026-07-19T12:05:00Z"
      And a canonical Polymarket TradeFill in that window exposes proxy wallet "0xwallet17" and explicit side "BUY"
      And the fill links to its raw artifact, receipt, market, outcome, timestamp, price, size, and transaction hash
      When the signal-to-wallet policy evaluates "signal-17" at the decision cutoff
      Then it creates one immutable watch registration for "0xwallet17"
      And the registration records trigger UID "signal-17", fill UID, market, incident window, decision cutoff, policy version, and enrollment reason "public_actor_fill_in_eligible_window"
      And it records the actor visibility as "public_proxy_wallet"
      And the registration is a bounded collection decision rather than a misconduct conclusion

    @enrollment @abstention
    Scenario Outline: Do not invent a wallet from non-actor market data
      Given a data-quality-eligible market signal has only "<source_fact>" in its incident window
      And the source fact exposes no public proxy wallet
      When the signal-to-wallet policy evaluates the signal
      Then it creates no wallet watch registration
      And it records reason code "<reason_code>"
      And wallet-specific outputs are unavailable rather than zero-filled

      Examples:
        | source_fact                    | reason_code               |
        | price observation               | no_actor_visible_fill     |
        | aggregate order-book observation | no_actor_visible_fill     |
        | unattributed public trade       | no_actor_visible_fill     |
        | feed gap covering trade activity | actor_coverage_incomplete |

    @deduplication @cooldown
    Scenario: Suppress a duplicate enrollment within the cooldown scope
      Given watch registration "watch-17" already exists for wallet "0xwallet17", market "market-17", and trigger family "residual_move"
      And "watch-17" has cooldown end "2026-07-19T12:35:00Z"
      And a second eligible signal for the same wallet, market, and trigger family occurs at "2026-07-19T12:15:00Z"
      When the signal-to-wallet policy evaluates the second signal
      Then it does not create a second watch registration
      And it records reason code "duplicate_trigger_within_cooldown"
      And it may append the new trigger UID as an observation of "watch-17" without changing the original cutoff

  Rule: Wallet history collection is complete, bounded, and reproducible

    @collection @polymarket_data_api
    Scenario: Request both passive and aggressive publicly reported trades
      Given the collector requests Polymarket trade history for wallet "0xwallet17"
      When it constructs the documented trades request
      Then the request includes user "0xwallet17"
      And it includes takerOnly "false"
      And the receipt retains the request filter values without credentials
      And a missing maker or taker role remains an explicit source-field limitation

    @collection @pagination @coverage
    Scenario: Page a historical wallet window without an offset ceiling
      Given watch "watch-17" requires trade history from "2025-10-01T00:00:00Z" through "2026-07-19T12:05:00Z"
      And the documented endpoint returns pages of at most 500 rows
      And the wallet has more rows in the requested interval than one endpoint offset cap can retrieve
      When the collector backfills the wallet history
      Then it partitions the requested interval into recorded server-side start and end windows
      And every request includes user "0xwallet17", start, end, limit, offset, and takerOnly "false"
      And it continues each partition until the source reports exhaustion or a declared bounded stop condition
      And it stores raw payloads, receipts, source watermarks, and a coverage record for every partition
      And a resumed run continues from the unfinished window and cursor rather than silently restarting or declaring complete coverage

    @collection @coverage @abstention
    Scenario Outline: Do not make historical absence claims from incomplete wallet collection
      Given watch "watch-17" requires history for interval "2025-10-01T00:00:00Z" through "2026-07-19T12:05:00Z"
      And the trade-history coverage for the exact user, filters, and interval is "<coverage_state>"
      When the wallet specialist evaluates a feature that requires complete history
      Then it records the exact uncovered user, filters, interval, and source watermark
      And it returns reason code "<reason_code>"
      And it does not claim the wallet had no earlier trades, no earlier positions, or no earlier related activity

      Examples:
        | coverage_state | reason_code                    |
        | partial        | wallet_history_coverage_partial |
        | unavailable    | wallet_history_unavailable      |
        | gap            | wallet_history_gap              |
        | stale          | wallet_history_stale            |

  Rule: Decisions remain causal while later collection is separately useful

    @causality @backfill
    Scenario: Preserve the original trigger cutoff while retaining late backfill as later evidence
      Given "watch-17" was registered at decision cutoff "2026-07-19T12:05:00Z"
      And a requested historical trade occurred at "2026-07-19T11:30:00Z"
      But its source delivery was first received at "2026-07-19T12:20:00Z"
      When the collector normalizes the late delivery
      Then it retains the trade event time and first-received time independently
      And the trade is excluded from the snapshot as of "2026-07-19T12:05:00Z"
      And the original watch registration and decision packet remain immutable
      And a later follow-up snapshot may use the trade only with a cutoff no earlier than "2026-07-19T12:20:00Z"

    @causality @historical_case @forensic_reconstruction @legal_audit
    Scenario: Produce a decisive hindsight signal for the documented Van Dyke case wallet
      Given complete same-market Polymarket coverage contains 21785 unique fills from 3449 wallets through cutoff "2026-01-03T09:20:59Z"
      And public Polymarket fills attributed by the venue to proxy wallet "0x31a56e9e690c621ed21de08cb559e9524cdb8ed9" have event times no later than the cutoff
      And those fills were first retrieved after the case cutoff
      And the exact market publication timestamp is unmapped
      But captured same-market trades prove public existence no later than "2025-12-12T01:20:24Z"
      When the historical wallet case is replayed with frozen generic top-1-percent and top-5-percent signal thresholds
      Then the output leads with classification "high", confidence "high", coverage "complete_same_market_population", and review priority "high"
      And it reports composite population rank 33 of 3449
      And it prominently reports Yes-buy-notional rank 7 of 1339 and gross-market-notional rank 32 of 3449
      And it retains the exact publication timestamp as unmapped while recording the observed public-existence upper bound
      And it labels the replay "hindsight_reconstructed" because availability followed the cutoff
      And final disposition is assigned to "human_reviewer"

    @selection @determinism
    Scenario: Select a bounded top-K set of participating wallets deterministically
      Given an eligible signal has 12 actor-visible fills from 7 distinct public proxy wallets
      And the enrollment policy declares top K "3" and policy version "wallet-watch-v1"
      When the policy ranks candidates using its frozen documented ranking fields and stable wallet UID tie-breaker
      Then exactly 3 wallet registrations are created unless fewer than 3 candidates pass eligibility
      And the rank, ranking inputs, tie-breaker, policy version, and candidate set hash are retained
      And replaying the same admitted facts produces the same selected wallet UIDs in the same order

  Rule: Collection includes a declared comparison population and forward observations

    @baseline @cohort
    Scenario: Maintain a non-triggered background cohort without treating it as benign ground truth
      Given a fixed cohort policy selects public proxy wallets from complete Polymarket coverage outside active trigger windows
      When the background cohort is sampled for the same venue, time slice, market family, and liquidity regime as "watch-17"
      Then cohort membership, sampling seed, eligibility query, cutoff, and coverage contract are retained
      And a cohort wallet is not labeled benign, non-fraudulent, or negative merely because it was not enrolled by a signal
      And incomplete background coverage returns reason code "background_cohort_coverage_incomplete"

    @snapshots @follow_up
    Scenario: Create immutable baseline and scheduled follow-up wallet snapshots
      Given watch "watch-17" has a baseline cutoff "2026-07-19T12:05:00Z"
      And its approved follow-up schedule is "2026-07-19T13:05:00Z", "2026-07-20T12:05:00Z", and "2026-07-26T12:05:00Z"
      When each scheduled collection attempt runs
      Then it creates a distinct wallet snapshot with its own as-of cutoff, receipt UIDs, source watermarks, and coverage state
      And it does not rewrite the baseline snapshot using follow-up facts
      And a missed attempt records "follow_up_collection_missed" with the scheduled interval and retry policy

    @profiles @mutability
    Scenario: Treat a public profile as mutable metadata rather than durable identity evidence
      Given profile delivery "profile-receipt-1" first observed wallet "0xwallet17" with handle "alpha"
      And profile delivery "profile-receipt-2" later observed the same wallet with handle "beta"
      When the system presents wallet context
      Then it retains both first-seen profile snapshots and their retrieval times
      And it may state that a handle was observed at a specified time

  Rule: Polygon facts corroborate execution

    @polygon @corroboration
    Scenario: Attach an exact Polygon execution match without expanding into identity attribution
      Given a canonical fill for "0xwallet17" includes Polygon transaction hash "0xtx17"
      And raw-verified chain ID 137 OrderFilled log "0xtx17:4" exactly matches the fill's contract, order hash, maker or taker role, token, side, amount, and timing
      When the chain collector creates wallet context
      Then it records the exact matching transaction, block, log index, contract version, and raw provenance
      And it reports corroboration state "exact_orderfilled_match"
      And an alert-created graph edge is not counted as independent corroboration of its triggering signal

    @polygon @contract_versioning
    Scenario Outline: Decode historical settlement with the version active at the transaction block
      Given wallet history contains a Polygon transaction at block "<block>"
      And the contract registry records settlement version "<version>" as active at that block
      When the collector decodes the transaction
      Then it uses only the contracts, token semantics, and decoder declared for "<version>"
      And it retains the registry revision and block-range evidence
      And an unknown active version returns reason code "polygon_contract_version_unmapped"

      Examples:
        | block    | version             |
        | 81234567 | polymarket_v1_usdce |
        | 91234567 | polymarket_v2_pusd  |

  Rule: Wallet watches are bounded and cannot recursively create an attribution graph

    @lifecycle @retry @expiry
    Scenario: Retry bounded collection and expire the watch without converting failure into absence
      Given watch "watch-17" has expiry "2026-07-26T12:05:00Z" and maximum retry count "3"
      And a follow-up source request fails transiently before expiry
      When the scheduler applies the declared retry policy
      Then it records each failed attempt, retry time, and coverage state
      And after three failed attempts it returns reason code "wallet_follow_up_retry_exhausted"
      And at expiry it marks the watch "expired" without deleting its raw artifacts, registrations, or snapshots
      And it does not report no later activity when forward coverage is partial or unavailable

    @graph_boundary @bounded_collection
    Scenario: Do not recursively enroll counterparties or linked wallets from a watched wallet
      Given "watch-17" observes a transfer to public address "0xother"
      And "0xother" was not itself selected from an eligible market signal under the enrollment policy
      When the graph builder records the verified transfer fact
      Then it may retain a factual directed transfer edge with raw provenance
      But it does not create a watch registration for "0xother" solely from that edge

  Rule: Analyst-facing outputs lead with decisive signal priority

    @outputs @claims_boundary
    Scenario: Report a decisive wallet behavior signal for human disposition
      Given a wallet snapshot has sufficient declared coverage and feature history
      And a case-retrieval query returns procedurally labeled, mapping-grade-filtered comparable episodes
      When the system produces an analyst-facing wallet packet
      Then it reports signal classification, confidence, coverage, market-activity context, case resemblance, and review priority
      And retrieved cases retain their source, procedural status, mapping grade, and independent case-cluster identifiers
      And a human reviewer owns final disposition
      And insufficient coverage, history, provenance, or restraint support returns classification "insufficient_data"
