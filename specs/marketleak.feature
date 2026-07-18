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
