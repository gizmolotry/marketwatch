# Architecture Analysis: Advanced Integrity Surveillance for Prediction Markets

## Executive Summary

This section analyzes two advanced architectural implementations for a prediction market integrity surveillance system: a knowledge graph entity extraction platform, referred to here as the "Web of Insiders," and a predictive "Leak Risk" forecasting layer that anticipates where information leakage is most likely before abnormal market activity appears. The core design premise is that market abuse is rarely visible from trade data alone. A trader's position size, fill pattern, or timing may be statistically anomalous, but the investigative value emerges when the system can attach that anomaly to external context: social coordination, wallet reuse, event calendars, role-based access, and historical patterns of leakage.

Both implementations are explicitly auxiliary to a math-based anomaly detection engine. They should not replace classical surveillance signals such as abnormal return, abnormal volume, pre-event drift, sudden liquidity demand, correlated account behavior, order book impact, wallet funding provenance, or improbable timing. Their role is to increase or decrease investigative priority by adding contextual evidence. The architecture must therefore be built around calibrated confidence, temporal validity, provenance preservation, and adversarial failure analysis.

## Implementation 1: Knowledge Graph Entity Extraction - The "Web of Insiders"

### Architecture

The Web of Insiders ingests Discord, Reddit, niche forums, wallet-tagging sources, ENS records, on-chain messages, market metadata, and permissioned platform data where legally available. The LLM is used as a constrained extraction engine, not as an autonomous investigator.

A production pipeline should be structured as:

```text
Source Connectors
  -> Immutable Raw Evidence Store
  -> Content Normalization
  -> LLM Entity and Claim Extraction
  -> Entity Resolution
  -> Claim Graph
  -> Canonical Neo4j Knowledge Graph
  -> Alert Enrichment API
```

The raw evidence store must preserve message IDs, timestamps, source URLs, edit/delete status, author handles, channel identifiers, collection authority, and retention policy. Every downstream graph edge must be traceable to evidence. The LLM should emit structured claims with exact source spans, confidence, caveats, and temporal qualifiers.

Representative extracted claim:

```json
{
  "claim_type": "ALIAS_LINK",
  "subject": { "type": "HANDLE", "value": "quant_roster88" },
  "predicate": "CLAIMS_CONTROL_OF",
  "object": { "type": "WALLET", "value": "0x1234...abcd" },
  "confidence": {
    "extractive_confidence": 0.82,
    "semantic_confidence": 0.64
  },
  "caveats": [
    "Self-claim, not cryptographic proof",
    "No independent corroboration"
  ]
}
```

The graph should separate raw claims from canonical facts. Neo4j nodes should include `Wallet`, `Handle`, `AliasCluster`, `PersonCandidate`, `Organization`, `Market`, `Event`, `Document`, `Claim`, `Trade`, and `Alert`. Edges should include confidence, evidence count, source diversity, first seen, last seen, model version, adjudication status, and risk weight.

Wallet-to-identity mapping must use evidence bands:

| Band | Meaning |
|---|---|
| A | Verified control through cryptographic, legal, or platform-verified evidence |
| B | Strongly supported control through multiple independent sources |
| C | Plausible association useful for triage only |
| D | Weak association useful only as background |
| E | Rejected or contradicted association |

When a math-based anomaly detector flags a trade, the graph expands from the wallet to linked wallets, handles, alias clusters, organizations, and event participants. It then searches for high-quality paths between the trader cluster and entities with plausible access to the market outcome. The alert should present path quality, confidence, source evidence, counter-evidence, and staleness warnings.

### Red-Team Failure Modes

The largest risk is hallucinated graph linkage. The LLM may convert sarcasm, quotes, copied wallet addresses, or rumors into factual edges. A user saying "sure, I am totally the CFO" can become an employment claim if extraction is not span-bound and caveat-aware. All LLM output must remain a claim until corroborated.

The second severe risk is the "six degrees of Kevin Bacon" problem. Crypto and prediction-market communities are highly connected. Almost any wallet can be loosely connected to almost any event through a major exchange, public Discord, influencer, subreddit, or forum. Path existence is not evidence. The graph must penalize high-degree hubs, cap path lengths, discount generic communities, and require role-relevant edges.

Other critical risks include weak identity signals becoming enforcement claims, adversarial graph poisoning, stale employment or roster relationships, wallet compromise, impersonation, copied usernames, and source reliability drift. The system must distinguish "associated with" from "same controller as" and from "verified legal identity as."

## Implementation 2: Predictive "Leak Risk" Forecasting

### Architecture

Predictive Leak Risk Forecasting estimates where information leakage is plausible before suspicious trading occurs. It scans earnings calendars, FDA decision windows, clinical trial readouts, esports roster deadlines, sports injury reports, governance votes, exchange listings, and other event sources. The output is a time-varying market-level risk prior.

The pipeline should be:

```text
Event Source Connectors
  -> Event Normalization
  -> LLM Event Understanding
  -> Market-Event Linking
  -> Historical Leak Risk Model
  -> Dynamic Polling Scheduler
  -> Anomaly Detection Configuration
```

The LLM parses messy documents, classifies event type, identifies participants, links events to prediction markets, and describes the information topology: who likely knows the outcome before the public, how long the private-to-public window is, and which sources historically leak first.

A representative event object:

```json
{
  "event_type": "FDA_PDUFA_DECISION",
  "subject_entities": ["Company ABC", "Drug XYZ"],
  "time_uncertainty": {
    "earliest": "2026-08-01T00:00:00Z",
    "latest": "2026-08-31T23:59:59Z"
  },
  "likely_access_groups": [
    "issuer",
    "regulator",
    "trial_sites",
    "outside_counsel"
  ],
  "market_candidates": ["market_123"]
}
```

The Leak Risk Score should combine event-class base rates, private-to-public lag, information holder count, historical abnormality, social chatter velocity, source reliability, market liquidity, time-to-event, and uncertainty penalties.

```text
LeakRiskScore =
  sigmoid(
    event_class_prior
    + private_window
    + access_group_count
    + historical_abnormality
    + social_chatter
    + liquidity_sensitivity
    + time_to_event
    + source_reliability
    - uncertainty_penalty
    - staleness_penalty
  )
```

The score should drive polling cadence, graph refresh rate, anomaly threshold sensitivity, order book snapshot depth, and analyst queue priority. It should not create an abuse alert by itself.

### Red-Team Failure Modes

Historical modeling fails under black swan conditions. A new disclosure channel, vendor leak, regulatory rule change, private Discord migration, or novel market type may create high leakage risk where the historical model sees none. Low historical risk must mean "not previously observed," not "safe."

Stale weightings are another major failure mode. Leak channels move, market participants adapt, calendars change, and adversaries alter behavior. A model trained on last year's incidents may confidently surveil the wrong domains. Feature drift, score drift, rolling retraining, short-term priors, and model-risk review are mandatory.

Calendar coverage gaps are also severe. Many high-risk events are not on official calendars: informal roster decisions, legal settlements, exchange listings, partnership announcements, internal security disclosures, and trial database updates. A calendar-driven model may over-monitor clean scheduled events and miss messy unscheduled ones.

LLMs can also misclassify event semantics: confusing an FDA advisory committee meeting with approval, mapping a roster rumor to the wrong team, or misunderstanding a market's resolution criteria. Market-event links must be source-cited and separately validated.

The forecasting layer can also create surveillance bias. High-risk markets are polled more often, so they produce more detected anomalies, reinforcing the model's belief that those categories are risky. Training data must include surveillance intensity and correct for unequal observation.

## Combined Operating Model

The two systems should be combined carefully:

1. Forecast leak risk for upcoming markets.
2. Adjust polling cadence and feature depth.
3. Run statistical anomaly detection continuously.
4. When an anomaly fires, enrich it with graph context.
5. Score graph paths using confidence, recency, source diversity, hub penalties, and role relevance.
6. Generate an evidence-bounded alert narrative.
7. Route to analysts with explicit uncertainty and counter-evidence.
8. Feed adjudication outcomes back into model calibration.

The leak model answers: "Where should we look harder?"

The graph answers: "When suspicious trading occurs, is there corroborated contextual evidence of plausible access?"

Neither should answer: "Is this person guilty?"

## Governance Requirements

A professional implementation requires evidence lineage, model versioning, human review gates, counter-evidence handling, identity access controls, audit logging, data minimization, retention policies, and periodic model-risk review.

The engineering posture should be skepticism by design. LLMs are useful for extracting structure from noisy text, but the architecture must assume extraction errors, adversarial posts, stale identities, biased labels, black swans, and over-connected graphs. Used correctly, these systems can improve surveillance precision and speed without collapsing into narrative-driven false positives.
```
tokens used
49,809
I attempted to write `marketleak/architecture_analysis.md`, but the workspace is currently read-only and the write was rejected by the sandbox.

Here is the exact markdown text for the file:

```markdown
# Architecture Analysis: Advanced Integrity Surveillance for Prediction Markets

## Executive Summary

This section analyzes two advanced architectural implementations for a prediction market integrity surveillance system: a knowledge graph entity extraction platform, referred to here as the "Web of Insiders," and a predictive "Leak Risk" forecasting layer that anticipates where information leakage is most likely before abnormal market activity appears. The core design premise is that market abuse is rarely visible from trade data alone. A trader's position size, fill pattern, or timing may be statistically anomalous, but the investigative value emerges when the system can attach that anomaly to external context: social coordination, wallet reuse, event calendars, role-based access, and historical patterns of leakage.

Both implementations are explicitly auxiliary to a math-based anomaly detection engine. They should not replace classical surveillance signals such as abnormal return, abnormal volume, pre-event drift, sudden liquidity demand, correlated account behavior, order book impact, wallet funding provenance, or improbable timing. Their role is to increase or decrease investigative priority by adding contextual evidence. The architecture must therefore be built around calibrated confidence, temporal validity, provenance preservation, and adversarial failure analysis.

## Implementation 1: Knowledge Graph Entity Extraction - The "Web of Insiders"

### Architecture

The Web of Insiders ingests Discord, Reddit, niche forums, wallet-tagging sources, ENS records, on-chain messages, market metadata, and permissioned platform data where legally available. The LLM is used as a constrained extraction engine, not as an autonomous investigator.

A production pipeline should be structured as:

```text
Source Connectors
  -> Immutable Raw Evidence Store
  -> Content Normalization
  -> LLM Entity and Claim Extraction
  -> Entity Resolution
  -> Claim Graph
  -> Canonical Neo4j Knowledge Graph
  -> Alert Enrichment API
```

The raw evidence store must preserve message IDs, timestamps, source URLs, edit/delete status, author handles, channel identifiers, collection authority, and retention policy. Every downstream graph edge must be traceable to evidence. The LLM should emit structured claims with exact source spans, confidence, caveats, and temporal qualifiers.

Representative extracted claim:

```json
{
  "claim_type": "ALIAS_LINK",
  "subject": { "type": "HANDLE", "value": "quant_roster88" },
  "predicate": "CLAIMS_CONTROL_OF",
  "object": { "type": "WALLET", "value": "0x1234...abcd" },
  "confidence": {
    "extractive_confidence": 0.82,
    "semantic_confidence": 0.64
  },
  "caveats": [
    "Self-claim, not cryptographic proof",
    "No independent corroboration"
  ]
}
```

The graph should separate raw claims from canonical facts. Neo4j nodes should include `Wallet`, `Handle`, `AliasCluster`, `PersonCandidate`, `Organization`, `Market`, `Event`, `Document`, `Claim`, `Trade`, and `Alert`. Edges should include confidence, evidence count, source diversity, first seen, last seen, model version, adjudication status, and risk weight.

Wallet-to-identity mapping must use evidence bands:

| Band | Meaning |
|---|---|
| A | Verified control through cryptographic, legal, or platform-verified evidence |
| B | Strongly supported control through multiple independent sources |
| C | Plausible association useful for triage only |
| D | Weak association useful only as background |
| E | Rejected or contradicted association |

When a math-based anomaly detector flags a trade, the graph expands from the wallet to linked wallets, handles, alias clusters, organizations, and event participants. It then searches for high-quality paths between the trader cluster and entities with plausible access to the market outcome. The alert should present path quality, confidence, source evidence, counter-evidence, and staleness warnings.

### Red-Team Failure Modes

The largest risk is hallucinated graph linkage. The LLM may convert sarcasm, quotes, copied wallet addresses, or rumors into factual edges. A user saying "sure, I am totally the CFO" can become an employment claim if extraction is not span-bound and caveat-aware. All LLM output must remain a claim until corroborated.

The second severe risk is the "six degrees of Kevin Bacon" problem. Crypto and prediction-market communities are highly connected. Almost any wallet can be loosely connected to almost any event through a major exchange, public Discord, influencer, subreddit, or forum. Path existence is not evidence. The graph must penalize high-degree hubs, cap path lengths, discount generic communities, and require role-relevant edges.

Other critical risks include weak identity signals becoming enforcement claims, adversarial graph poisoning, stale employment or roster relationships, wallet compromise, impersonation, copied usernames, and source reliability drift. The system must distinguish "associated with" from "same controller as" and from "verified legal identity as."

## Implementation 2: Predictive "Leak Risk" Forecasting

### Architecture

Predictive Leak Risk Forecasting estimates where information leakage is plausible before suspicious trading occurs. It scans earnings calendars, FDA decision windows, clinical trial readouts, esports roster deadlines, sports injury reports, governance votes, exchange listings, and other event sources. The output is a time-varying market-level risk prior.

The pipeline should be:

```text
Event Source Connectors
  -> Event Normalization
  -> LLM Event Understanding
  -> Market-Event Linking
  -> Historical Leak Risk Model
  -> Dynamic Polling Scheduler
  -> Anomaly Detection Configuration
```

The LLM parses messy documents, classifies event type, identifies participants, links events to prediction markets, and describes the information topology: who likely knows the outcome before the public, how long the private-to-public window is, and which sources historically leak first.

A representative event object:

```json
{
  "event_type": "FDA_PDUFA_DECISION",
  "subject_entities": ["Company ABC", "Drug XYZ"],
  "time_uncertainty": {
    "earliest": "2026-08-01T00:00:00Z",
    "latest": "2026-08-31T23:59:59Z"
  },
  "likely_access_groups": [
    "issuer",
    "regulator",
    "trial_sites",
    "outside_counsel"
  ],
  "market_candidates": ["market_123"]
}
```

The Leak Risk Score should combine event-class base rates, private-to-public lag, information holder count, historical abnormality, social chatter velocity, source reliability, market liquidity, time-to-event, and uncertainty penalties.

```text
LeakRiskScore =
  sigmoid(
    event_class_prior
    + private_window
    + access_group_count
    + historical_abnormality
    + social_chatter
    + liquidity_sensitivity
    + time_to_event
    + source_reliability
    - uncertainty_penalty
    - staleness_penalty
  )
```

The score should drive polling cadence, graph refresh rate, anomaly threshold sensitivity, order book snapshot depth, and analyst queue priority. It should not create an abuse alert by itself.

### Red-Team Failure Modes

Historical modeling fails under black swan conditions. A new disclosure channel, vendor leak, regulatory rule change, private Discord migration, or novel market type may create high leakage risk where the historical model sees none. Low historical risk must mean "not previously observed," not "safe."

Stale weightings are another major failure mode. Leak channels move, market participants adapt, calendars change, and adversaries alter behavior. A model trained on last year's incidents may confidently surveil the wrong domains. Feature drift, score drift, rolling retraining, short-term priors, and model-risk review are mandatory.

Calendar coverage gaps are also severe. Many high-risk events are not on official calendars: informal roster decisions, legal settlements, exchange listings, partnership announcements, internal security disclosures, and trial database updates. A calendar-driven model may over-monitor clean scheduled events and miss messy unscheduled ones.

LLMs can also misclassify event semantics: confusing an FDA advisory committee meeting with approval, mapping a roster rumor to the wrong team, or misunderstanding a market's resolution criteria. Market-event links must be source-cited and separately validated.

The forecasting layer can also create surveillance bias. High-risk markets are polled more often, so they produce more detected anomalies, reinforcing the model's belief that those categories are risky. Training data must include surveillance intensity and correct for unequal observation.

## Combined Operating Model

The two systems should be combined carefully:

1. Forecast leak risk for upcoming markets.
2. Adjust polling cadence and feature depth.
3. Run statistical anomaly detection continuously.
4. When an anomaly fires, enrich it with graph context.
5. Score graph paths using confidence, recency, source diversity, hub penalties, and role relevance.
6. Generate an evidence-bounded alert narrative.
7. Route to analysts with explicit uncertainty and counter-evidence.
8. Feed adjudication outcomes back into model calibration.

The leak model answers: "Where should we look harder?"

The graph answers: "When suspicious trading occurs, is there corroborated contextual evidence of plausible access?"

Neither should answer: "Is this person guilty?"

## Governance Requirements

A professional implementation requires evidence lineage, model versioning, human review gates, counter-evidence handling, identity access controls, audit logging, data minimization, retention policies, and periodic model-risk review.

The engineering posture should be skepticism by design. LLMs are useful for extracting structure from noisy text, but the architecture must assume extraction errors, adversarial posts, stale identities, biased labels, black swans, and over-connected graphs. Used correctly, these systems can improve surveillance precision and speed without collapsing into narrative-driven false positives.
