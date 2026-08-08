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
