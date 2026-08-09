Feature: Prediction-market information leakage triage

  Scenario: Market moves before the earliest public source
    Given a prediction market with price and volume history
    And an earliest known public source timestamp
    When the market price moves unusually before that timestamp
    Then the system flags a potential pre-announcement movement signal
    And it generates a human-review memo
    And it must not claim insider trading is proven

  Scenario: Market moves after public information
    Given a prediction market with price and volume history
    And public news was available before the price movement
    When the system analyzes the market
    Then it should lower the anomaly severity
    And explain that public information may account for the move

  Scenario: Low-liquidity market
    Given a prediction market with thin trading volume
    When a large price movement occurs
    Then the system should flag low liquidity as an alternative explanation
    And reduce confidence in the integrity signal

  @implemented @polymarket @population @public_wallet
  Scenario: Collect one evidence-bound Polymarket public-wallet population without silently dropping role coverage
    Given one exact Polymarket condition ID and a frozen inclusive whole-second interval
    And an operator-approved SHA-256 policy admits the captured official trades-contract snapshot
    And raw objects and receipts exist for that official snapshot and the exact Gamma market metadata
    And Gamma "acceptingOrdersTimestamp" supplies only the condition-specific market-activity lower bound
    And the Data API market-query retention floor remains unknown or approximate
    When the population collector requests public trades
    Then every source query fixes that one condition ID and sends "takerOnly" as false
    And saturated pages are recursively partitioned into disjoint time windows
    And a saturated one-second window is partitioned by explicit "BUY" and "SELL" requests when supported
    And unsupported or still-saturated side partitions are explicit "irreducibly_partial" leaves
    And logical-request, total-HTTP-attempt, and leaf limits yield explicit "budget_exhausted" leaves rather than hidden gaps
    And each fetched leaf binds every response-bearing attempt's exact request, status, raw delivery, and receipt
    And successful raw rows exactly equal ordered production-parser fills
    And malformed source wallets or non-exact source sides fail closed
    And the immutable manifest is always incomplete because the exact Data API retention floor is unknown
    And it reports "query_exhausted_coverage_limited" only when every terminal query is exhausted and source-consistent, otherwise "partial"
    And an unfetched budget leaf is reported as unrecorded and is not forged into a CoverageLedger row
    And the result refers to an exposed "proxyWallet" only as a pseudonymous venue actor
    # Executable mapping: tests/v2/test_polymarket_population.py::test_recursive_boundaries_receipts_and_parent_reconciliation
    # Executable mapping: tests/v2/test_polymarket_population.py::test_one_second_buy_sell_partition_and_source_inconsistency
    # Executable mapping: tests/v2/test_polymarket_population.py::test_market_activity_lower_bound_is_derived_from_gamma_and_distinct_clocks
    # Executable mapping: tests/v2/test_polymarket_population.py::test_retry_attempts_are_all_bound_and_total_http_budget_is_hard
    # Executable mapping: tests/v2/test_ingestion_connectors.py::test_polymarket_trade_source_wallet_and_side_are_exact
    # Executable mapping: tests/v2/test_polymarket_population.py::test_contract_policy_tamper_non_2xx_and_unapproved_hash_fail
    # Executable mapping: tests/v2/test_polymarket_population.py::test_request_budget_preserves_first_partial_page
    # Executable mapping: tests/v2/test_polymarket_population.py::test_leaf_budget_stops_recursive_growth
    # Executable mapping: tests/v2/test_cli_v2.py::test_population_cli_serializes_one_exact_frozen_condition_query
    # Executable mapping: tests/v2/test_cli_v2.py::test_population_collection_persists_canonical_manifest_idempotently
    # Executable mapping: tests/v2/test_cli_v2.py::test_population_manifest_atomic_publication_failure_leaves_no_target_or_pending_file
    # Executable mapping: tests/v2/test_cli_v2.py::test_population_coverage_uses_terminal_leaf_filters_and_never_completes_an_inconsistent_run
    # Executable mapping: tests/v2/test_cli_v2.py::test_population_cli_max_request_budget_reports_unrecorded_terminal_interval_without_ledger_row

  Scenario: Graph and proxy-cluster caches are non-executable
    Given a bounded canonical JSON graph or proxy-cluster cache
    When a runtime surface loads the cache
    Then it validates the exact schema and declared size limits before use
    And it atomically persists only canonical JSON
    And a legacy pickle cache is ignored without deserialization
    And a missing, corrupt, oversized, or unknown-schema cache fails closed
    And only a loaded empty graph may be reported as observed zero graph evidence
    And missing or corrupt graph storage suppresses graph enrichment and absence claims

  Scenario: HTTP evidence is connection-bound before raw capture
    Given an explicitly configured HTTPS evidence origin and response-byte limit
    When the source hostname resolves to approved public addresses
    Then the transport connects directly to one approved address with hostname-verified TLS
    And it does not re-resolve the hostname while opening that connection
    And it may try later approved addresses only in deterministic bounded order
    And every captured response has an attested approved peer address and the exact approved effective origin
    And redirects, unapproved effective origins, unapproved peers, and cross-origin pagination fail closed
    And private, loopback, link-local, reserved, userinfo, HTTP, and unsafe-port destinations are rejected before capture

  Scenario: Oversized HTTP delivery is not durable evidence
    Given an explicitly configured maximum HTTP response size
    When the transport observes more than the maximum bytes
    Then it reads no more than the limit plus one byte
    And it reports an explicit unavailable or incomplete result
    And it does not persist the oversized body or a raw receipt

  Scenario: HTTP evidence metadata is secret-free
    Given a request containing authorization, API-key, cookie, signed query, or path credentials
    And a response containing authentication or Set-Cookie headers
    When the collector records provenance or reports an error
    Then persisted URLs contain only the approved origin
    And request parameter values and non-allowlisted headers are omitted
    And common single-token and multi-token authorization values are fully redacted
    And cookie, custom secret-header, and query-form assignment values are fully redacted
    And quoted JSON, Bearer, Basic, hyphenated, underscored, and case variants are fully redacted
    And arbitrary transport exceptions cross the API boundary only as bounded sanitized typed errors
    And explicit redaction state is retained without retaining the secret
