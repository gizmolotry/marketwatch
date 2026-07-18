# Technical Roadmap: Knowledge Graph Entity Extraction and Predictive Leak Risk Forecasting

## Executive Summary

This roadmap defines the engineering plan for evolving the current `marketleak` prototype into a production-grade integrity surveillance platform with two advanced systems:

1. **Knowledge Graph Entity Extraction ("Web of Insiders")** - a provenance-preserving graph layer linking markets, events, wallets, handles, organizations, claims, evidence, and alerts.
2. **Predictive "Leak Risk" Forecasting** - a market-level prior that estimates where information leakage is plausible before abnormal trading appears.

The current prototype already has the correct mathematical spine: Pandas ingestion, rolling Z-score belief-shock detection in `marketleak/scoring.py`, RAG lead-time analysis, and SAR generation. The roadmap preserves that core and adds context around it.

The operating model is:

- Z-score engine: **Did the market move abnormally?**
- Leak-risk forecaster: **Where should surveillance look harder?**
- Knowledge graph: **Is there evidence-bounded context showing plausible access?**

No system should independently assert misconduct. The platform should produce calibrated, source-grounded investigative priority.

## 1. Tech Stack Recommendations

### Graph Database

**Recommendation: Neo4j AuraDB or Neo4j Enterprise.**

Use Neo4j for the first production implementation because it has the best combination of Cypher readability, Python support, local development ergonomics, graph visualization, schema constraints, vector indexes, and Graph Data Science algorithms. It is the strongest fit for analyst-facing path queries such as wallet-to-handle-to-organization-to-event relationships.

**Use Amazon Neptune later** if the platform becomes deeply AWS-native and needs managed VPC isolation, IAM integration, automated backups, high availability, and optional RDF/SPARQL support.

**Use Memgraph experimentally** only if low-latency streaming graph mutation becomes a bottleneck. Its Kafka/Redpanda/Pulsar streaming story is attractive, but Neo4j is the safer canonical investigative graph for the first production version.

### LLM Providers

**Primary: OpenAI.** Use for structured claim extraction, event classification, market-event linking, counter-evidence analysis, and high-stakes alert narratives. Enforce JSON Schema/Pydantic validation on every structured output.

**Secondary: Google Gemini.** Keep because the current prototype already uses `google-genai`. Use for cost-effective summarization, SAR drafting, batch enrichment, and provider-diverse fallback.

**Optional: Anthropic Claude.** Use for second-pass review of severe alerts, long-context evidence packets, or independent critique of graph paths.

All providers should sit behind an internal `LLMRouter` with persisted provider, model, prompt version, schema version, input evidence IDs, output JSON, validation result, latency, and cost metadata.

### Data Ingestion and Orchestration

**Primary orchestrator: Dagster.** Its asset-centric model maps naturally to surveillance artifacts: raw market ticks, normalized markets, belief shocks, raw documents, extracted claims, resolved entities, graph edges, leak-risk scores, and alert packets.

**Streaming layer: Kafka or Redpanda.** Add only when batch polling is no longer sufficient. Initial topics should include `market.ticks.raw`, `documents.raw`, `claims.extracted`, `entities.resolved`, `graph.upserts`, `leakrisk.scores`, and `alerts.enriched`.

**Local analytics:** keep Pandas, add Parquet/PyArrow, DuckDB, Pydantic, Pandera or Great Expectations, and scikit-learn for baseline risk models.

**Storage tiers:**

| Tier | Technology | Purpose |
|---|---|---|
| Raw evidence | S3, MinIO, or local object-store layout | immutable API responses, HTML, JSON, screenshots, message archives |
| Analytical data | Parquet plus DuckDB | normalized market ticks, events, features, backtests |
| Operational state | PostgreSQL | jobs, reviews, alert queue, model runs |
| Graph | Neo4j | claims, entities, relationships, paths, alert context |

## 2. Phase 1: Data Ingestion and Schema Design

### Objective

Build the reliable data foundation for graph extraction and leak-risk forecasting without destabilizing the existing anomaly prototype.

### Deliverables

- Source registry and ingestion policy.
- Immutable raw evidence store.
- Normalized schemas for markets, ticks, documents, events, claims, and entities.
- Neo4j schema with constraints and indexes.
- LLM extraction schema with validation gates.
- Entity-resolution pipeline.
- Initial graph enrichment queries.
- Data-quality and provenance tests.

### Raw Evidence Store

Every raw record must preserve:

- `evidence_id`
- `source_id`
- `source_uri`
- `collector_version`
- `collected_at`
- `published_at`
- `observed_at`
- `content_hash`
- `raw_storage_uri`
- `retention_until`
- `pii_classification`

The raw evidence store is the system of record. The graph is only an indexed interpretation.

### Market Schema

Promote the current CSV fields into versioned records:

```text
Market
  market_uid
  platform
  platform_market_id
  market_slug
  question
  resolution_source
  open_time
  close_time
  resolution_time
  category
  subject_entities
  resolution_criteria_text
```

```text
MarketTick
  tick_uid
  market_uid
  observed_at
  price
  volume
  liquidity
  bid
  ask
  source_id
  evidence_id
```

Use deterministic `market_uid = platform + ":" + platform_market_id`. Keep `market_slug` for display, not identity.

### Event Schema

```text
Event
  event_uid
  event_type
  title
  subject_entities
  event_window_start
  event_window_end
  expected_public_release_at
  private_information_window_start
  likely_access_groups
  source_reliability
  status
  evidence_ids
```

Initial event types: earnings, FDA decisions, clinical trial readouts, sports injury reports, roster decisions, governance votes, exchange listings, legal settlements, macro releases, weather events, and crypto protocol incidents.

### LLM Claim Schema

LLMs must emit claims, not facts.

```json
{
  "schema_version": "claim_extraction.v1",
  "document_uid": "doc_...",
  "extraction_run_uid": "xrun_...",
  "claims": [
    {
      "claim_uid": "claim_...",
      "claim_type": "ALIAS_LINK",
      "subject": {"entity_type": "HANDLE", "value": "quant_roster88"},
      "predicate": "CLAIMS_CONTROL_OF",
      "object": {"entity_type": "WALLET", "value": "0x1234...abcd"},
      "source_span": {"start_char": 120, "end_char": 178, "text": "..."},
      "confidence": {
        "extractive_confidence": 0.82,
        "semantic_confidence": 0.64
      },
      "caveats": ["Self-claim, not cryptographic proof"]
    }
  ]
}
```

Reject claims without source spans, evidence IDs, valid subject/predicate/object fields, or schema-valid JSON.

### Entity Resolution

Separate these concepts:

- `Handle`
- `Wallet`
- `AliasCluster`
- `PersonCandidate`
- `Organization`
- `AccessGroup`

Evidence bands:

| Band | Meaning | Use |
|---|---|---|
| A | verified control | strong investigative context |
| B | multiple independent sources | contextual evidence with caveats |
| C | plausible weak association | triage only |
| D | weak background association | low weight |
| E | rejected or contradicted | negative evidence |

### Neo4j Schema

Core nodes:

```text
(:Market)
(:MarketTick)
(:Alert)
(:Event)
(:Document)
(:Evidence)
(:Claim)
(:Wallet)
(:Handle)
(:AliasCluster)
(:PersonCandidate)
(:Organization)
(:AccessGroup)
(:ExtractionRun)
(:ModelRun)
(:ReviewDecision)
```

Core relationships:

```text
(:Market)-[:HAS_TICK]->(:MarketTick)
(:Market)-[:LINKED_TO_EVENT]->(:Event)
(:Market)-[:HAS_ALERT]->(:Alert)
(:Alert)-[:TRIGGERED_BY_TICK]->(:MarketTick)
(:Document)-[:YIELDED_CLAIM]->(:Claim)
(:Claim)-[:ASSERTS_SUBJECT]->(:Wallet|:Handle|:Organization|:Event)
(:Claim)-[:ASSERTS_OBJECT]->(:Wallet|:Handle|:Organization|:Event)
(:Wallet)-[:MEMBER_OF]->(:AliasCluster)
(:Handle)-[:MEMBER_OF]->(:AliasCluster)
(:Organization)-[:HAS_ACCESS_GROUP]->(:AccessGroup)
(:AccessGroup)-[:HAS_PLAUSIBLE_ACCESS_TO]->(:Event)
```

Every evidence-bearing edge must include confidence, evidence band, evidence count, source diversity, first seen, last seen, validity window, model version, adjudication status, risk weight, and staleness score.

## 3. Phase 2: Integration with Existing `marketleak`

### Objective

Attach graph enrichment and leak-risk priors to the current Pandas/Z-score pipeline without replacing the anomaly engine.

Current flow:

```text
data_agent.py
  -> demo_data/market_dump.csv
  -> anomaly_agent.py
  -> scoring.py::detect_belief_shocks()
  -> scoring.py::get_top_anomalies()
  -> rag_agent.py
  -> synthesis_agent.py
  -> reports/*.md
```

### Stable Internal Contracts

Create explicit objects:

```text
AnomalyCandidate
  alert_uid
  market_uid
  market_slug
  question
  shock_timestamp
  price
  logit_belief
  belief_shock
  rolling_mean
  rolling_std
  z_score
  z_threshold
```

```text
LeakRiskPrior
  market_uid
  event_uid
  score
  score_version
  feature_snapshot_uid
  computed_at
  drivers
```

```text
GraphEnrichment
  alert_uid
  candidate_paths
  max_path_score
  path_count
  highest_evidence_band
  counter_evidence_count
  stale_edge_count
```

```text
AlertPacket
  alert_uid
  anomaly_candidate
  leak_risk_prior
  graph_enrichment
  rag_result
  queue_priority
  narrative_inputs
```

### Preserve and Harden the Z-Score Engine

Keep `detect_belief_shocks()` as the mathematical source of truth. Add an adapter that converts normalized `MarketTick` rows into `AnomalyCandidate` records.

Required hardening:

1. Preserve current tests.
2. Add multi-market tests.
3. Sort deterministically by `market_slug` and timestamp.
4. Handle duplicate ticks.
5. Handle missing prices.
6. Handle zero rolling standard deviation.
7. Add `market_uid` support while keeping `market_slug`.

### Leak Risk as a Prior

The leak-risk score should tune surveillance intensity, not create alerts by itself.

```text
LeakRiskScore =
  sigmoid(
    event_class_prior
    + private_window_weight
    + access_group_count_weight
    + source_reliability_weight
    + market_liquidity_weight
    + social_chatter_velocity_weight
    + historical_abnormality_weight
    + time_to_event_weight
    - uncertainty_penalty
    - staleness_penalty
  )
```

Initial threshold policy:

```text
effective_z_threshold =
  clamp(base_z_threshold - (leak_risk_score * 0.5), 2.0, 3.5)
```

Use this to adjust polling cadence, graph refresh cadence, anomaly sensitivity, order-book retention, and queue priority.

### Graph Enrichment After Anomaly Detection

Graph enrichment flow:

```text
AnomalyCandidate
  -> find Market
  -> find linked Event candidates
  -> find likely AccessGroups
  -> find Wallet/Handle/AliasCluster context if available
  -> search bounded paths
  -> score paths
  -> return evidence-bounded GraphEnrichment
```

Path score:

```text
PathScore =
  evidence_band_weight
  * source_diversity_weight
  * recency_weight
  * role_relevance_weight
  * temporal_validity_weight
  * contradiction_penalty
  * hub_penalty
  * path_length_penalty
```

Hard rules:

- Start with max path length 4.
- Penalize high-degree hubs.
- Discount generic social proximity.
- Require role-relevant edges for high scores.
- Reduce score for stale, contradicted, or weak identity links.

### Alert Queue Priority

```text
QueuePriority =
  0.45 * normalized_abs_z_score
  + 0.20 * leak_risk_score
  + 0.20 * graph_path_score
  + 0.10 * normalized_ppim_score
  + 0.05 * source_reliability_score
```

Keep component scores visible. Do not collapse the system into one opaque number.

## 4. Phase 3: Neuro-Symbolic LLM Routing and Deployment

### Objective

Deploy LLMs for extraction and explanation while deterministic systems validate, score, constrain, and route.

The LLM performs:

- extraction
- classification
- summarization
- candidate generation
- counter-evidence discovery
- narrative drafting

The symbolic layer performs:

- schema validation
- timestamp validation
- evidence lineage enforcement
- confidence-band enforcement
- graph path scoring
- thresholding
- alert routing
- human review gates
- audit logging

### LLM Routing Matrix

| Task | Default Route | Escalation | Symbolic Gate |
|---|---|---|---|
| document cleanup | low-cost model | none | timestamp validation |
| entity extraction | structured model | stronger model after failures | JSON/Pydantic validation |
| claim extraction | structured model | stronger model for ambiguity | source-span requirement |
| event classification | standard reasoning model | high-reasoning model | event ontology |
| market-event linking | retrieval plus reasoning | high-reasoning for high risk | resolution criteria check |
| entity-resolution tie-break | symbolic first | LLM explanation only | evidence-band policy |
| SAR narrative | standard model | high-reasoning for severe alerts | evidence citation |
| counter-evidence review | high-reasoning model | provider-diverse pass | contradiction registry |

### Production Services

```text
marketleak-api        FastAPI alert packet and analyst workflow API
marketleak-ingestion  Dagster assets and source connectors
marketleak-anomaly    Pandas/DuckDB anomaly computation
marketleak-graph      Neo4j repository and path scoring
marketleak-leakrisk   event features and risk scoring
marketleak-llm        LLM router, schemas, prompts, evals
marketleak-ui         Streamlit initially, dedicated analyst UI later
```

### Deployment

Local development should use Docker Compose with Neo4j, PostgreSQL, MinIO, Dagster, the API, and Streamlit. Staging should replay historical datasets with separate LLM keys and redacted data. Production should add managed Neo4j or Neo4j Enterprise, managed PostgreSQL, object storage retention policies, secrets management, OpenTelemetry, Prometheus/Grafana, centralized logs, RBAC, and audit trails.

### Evaluation

Create gold datasets for:

- entity and claim extraction
- market-event linking
- sarcasm, quote, copied-wallet, and impersonation false links
- historical anomalies with public-news timing
- non-anomalous high-liquidity markets
- graph path quality

Minimum launch thresholds:

- claim extraction precision above 0.90 for high-confidence claims
- zero canonical graph writes from invalid LLM output
- market-event link precision above 0.85
- SAR evidence citation completeness above 0.95
- human review for all high-priority alerts

## 5. Implementation Timeline

### Milestone 0: Prototype Hardening

Duration: 1 week.

- Preserve current demo behavior.
- Add schema models.
- Add scoring tests.
- Normalize timestamps.
- Define `AnomalyCandidate`.

### Milestone 1: Evidence Store and Normalized Assets

Duration: 2 weeks.

- Raw evidence store.
- Normalized market ticks in Parquet.
- Normalized documents and events.
- Source registry.
- Dagster local pipeline.

### Milestone 2: Neo4j Knowledge Graph MVP

Duration: 2 to 3 weeks.

- Neo4j local deployment.
- Constraints and indexes.
- Graph repository.
- Market, event, document, evidence, and claim upserts.
- Bounded path query.

### Milestone 3: LLM Claim Extraction

Duration: 2 weeks.

- Claim extraction schema.
- Prompt registry.
- Provider abstraction.
- Structured-output validation.
- Gold extraction set.
- Invalid-output quarantine.

### Milestone 4: Leak-Risk Prior MVP

Duration: 2 weeks.

- Event feature table.
- Transparent weighted model.
- Score drivers.
- Polling policy.
- Backtest report.

### Milestone 5: Alert Packet Integration

Duration: 2 weeks.

- `AlertPacket` assembly.
- Graph enrichment service.
- RAG integration.
- Streamlit enriched alert view.
- SAR generation from alert packets.
- Alert queue.

### Milestone 6: Neuro-Symbolic Deployment

Duration: 3 to 4 weeks.

- FastAPI boundary.
- Dagster orchestration.
- LLM router.
- Observability dashboards.
- Staging replay.
- Analyst workflow.
- Security and governance review.

## 6. Recommended Repository Structure

```text
marketleak/
  agents/
    anomaly_agent.py
    rag_agent.py
    synthesis_agent.py
  graph/
    repository.py
    schema.cypher
    queries/
      enrich_alert.cypher
      find_market_event_paths.cypher
  ingestion/
    sources.py
    normalize_market.py
    normalize_document.py
    normalize_event.py
  leakrisk/
    features.py
    model.py
    calibration.py
  llm/
    router.py
    schemas.py
    prompts/
      claim_extraction.md
      event_classification.md
      market_event_linking.md
      sar_generation.md
  models.py
  scoring.py
  alerting.py
  app.py
tests/
  test_scoring.py
  test_models.py
  test_leakrisk.py
  test_graph_queries.py
  test_llm_schemas.py
```

## 7. Key Risks and Controls

| Risk | Control |
|---|---|
| hallucinated graph edges | keep LLM output as claims, require source spans, validate schemas |
| over-connected graph paths | hub penalties, path caps, role relevance, counter-evidence |
| leak-risk feedback loops | track surveillance intensity, sample low-risk markets, calibrate by event type |
| stale identity links | validity windows, staleness penalties, revalidation |
| LLM model drift | model versioning, evals before upgrades, provider abstraction |
| compliance overstatement | cautious SAR language, human review, uncertainty sections |

## 8. Definition of Done

The architecture is production-ready when:

- the Z-score anomaly detector remains reproducible and tested
- every alert traces back to raw evidence
- every graph edge has provenance, confidence, temporal validity, and adjudication status
- leak-risk scores are explainable by feature drivers
- LLM outputs are schema-validated, versioned, and quarantined on failure
- alert narratives cite evidence and include uncertainty
- analyst decisions feed back into graph and model calibration
- deployment has observability, RBAC, audit logs, and retention controls

## 9. Reference Links Consulted

- Neo4j documentation: https://neo4j.com/docs/
- Neo4j vector indexes: https://neo4j.com/docs/cypher-manual/current/indexes/semantic-indexes/vector-indexes/
- Neo4j Graph Data Science: https://neo4j.com/docs/graph-data-science/current/
- Amazon Neptune user guide: https://docs.aws.amazon.com/neptune/latest/userguide/intro.html
- Memgraph documentation: https://memgraph.com/docs/getting-started
- Memgraph streams: https://memgraph.com/docs/data-streams
- OpenAI structured outputs: https://developers.openai.com/api/docs/guides/structured-outputs
- Gemini structured output: https://ai.google.dev/gemini-api/docs/structured-output
- Dagster documentation: https://docs.dagster.io/
- Apache Airflow documentation: https://airflow.apache.org/docs/apache-airflow/stable/
- Apache Kafka documentation: https://kafka.apache.org/documentation/
