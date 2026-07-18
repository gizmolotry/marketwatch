@phase15 @multimodal @market_integrity
Feature: Point-in-time multimodal market-integrity assessment
  The system assembles only evidence available at a decision cutoff,
  compares observable explanations for market activity, and routes bounded
  findings to a human analyst when evidence and operational controls permit.
  It never converts a score into a finding of fraud, identity, or intent.

  # Status tags are part of this contract:
  #   @implemented marks behavior directly supported by the current repository.
  #   @target_design @not_current marks required future behavior and must not be
  #   represented as an available collector, trained model, or validated result.

  Background:
    Given the decision cutoff is "2026-07-13T12:05:03Z"
    And all source timestamps are timezone-aware
    And raw source objects are retained with immutable identifiers and SHA-256 receipts

  Rule: Event memory is causal and point-in-time

    @implemented @ingestion @provenance
    Scenario: Include a fact that existed and was available before the cutoff
      Given a market observation occurred at "2026-07-13T12:04:58Z"
      And the observation was ingested at "2026-07-13T12:05:01Z"
      When the system assembles the event snapshot as of the decision cutoff
      Then the observation is included
      And its raw artifact identifier and source watermark are preserved

    @implemented @ingestion @future_leakage
    Scenario Outline: Exclude facts unavailable at decision time
      Given a public fact has event time "<event_time>"
      And the fact was ingested at "<ingested_at>"
      When the system assembles the event snapshot as of the decision cutoff
      Then the fact is excluded
      And the historical snapshot is not rewritten when the fact later becomes available

      Examples:
        | event_time               | ingested_at              |
        | 2026-07-13T12:05:04Z     | 2026-07-13T12:05:04Z     |
        | 2026-07-13T12:04:00Z     | 2026-07-13T12:06:00Z     |

    @implemented @ingestion @deduplication
    Scenario: Quarantine conflicting canonical records
      Given two records have the same immutable record identifier
      And their canonical contents differ
      When ingestion detects the semantic identifier and content collision
      Then the conflicting delivery is quarantined as a UID/content conflict
      And the quarantined record is not eligible for event snapshot assembly
      And neither version silently replaces the other

  Rule: Market microstructure is represented continuously

    @implemented @features @microstructure
    Scenario: Build the latest completed five-minute market window
      Given price observations, fills, and order-book snapshots exist between "2026-07-13T12:00:00Z" and "2026-07-13T12:05:00Z"
      When the system builds a market feature snapshot
      Then the feature window starts at "2026-07-13T12:00:00Z"
      And the feature window ends at "2026-07-13T12:05:00Z"
      And it contains observation, fill, and book counts
      And it contains price open, price close, and price change when observed
      And it contains fill notional and mean fill size when observed
      And it contains quoted spread, bid depth, ask depth, and depth imbalance when observed
      And the snapshot retains aggregate contributor UID lists for observations, fills, books, and raw artifacts
      And it does not claim per-feature lineage beyond those aggregate contributor lists

    @implemented @features @missingness
    Scenario: Preserve an unavailable trade or book stream as missing
      Given price observations are available for the feature window
      And no verified fill stream is available
      And no fresh order-book snapshot is available
      When the system builds a market feature snapshot
      Then fill count and order-book snapshot count are zero
      And fill-derived and order-book-derived numeric values are null
      And the broad market-state modality remains observed because price observations exist
      And null derived values are not interpreted as observed zero activity or zero liquidity

    @implemented @features @thin_market
    Scenario: Describe a large move in a sparse market without a hard liquidity verdict
      Given one small fill causes a large price change
      And quoted depth is low and the spread is wide
      When the system builds continuous microstructure features
      Then the price change, fill notional, depth, and spread remain separate inputs
      And no fixed volume threshold removes the event
      And the feature builder does not label fraud or thin-market noise

  Rule: Reference prices must match the contract's documented settlement source

    @target_design @not_current @reference @settlement_mapping
    Scenario: Use a documented BTC reference source
      Given a BTC contract's resolution rule explicitly names reference source "source-A"
      And time-aligned observations from "source-A" were captured before the cutoff
      When the system derives reference-asset features
      Then it computes the BTC return, volatility, and market residual from "source-A"
      And the reference mapping and raw observations remain in the evidence lineage

    @implemented @reference @abstention
    Scenario: Do not substitute a generic BTC price feed
      Given a BTC contract has no documented reference-source mapping
      And a generic BTC price feed is available
      When the system assesses the event
      Then the reference modality is marked "unmapped"
      And the generic feed is not described as the settlement reference
      And any conclusion requiring the settlement reference abstains

  Rule: Public evidence is archived before it is used as an explanation

    @implemented @public_evidence @chronology
    Scenario: Public information can explain only subsequent movement
      Given a relevant primary-source document was first observed at "2026-07-13T12:01:00Z"
      And a market movement began at "2026-07-13T12:02:00Z"
      When the system constructs the causal chronology
      Then the document may support mechanism "public_information_response"
      And the evidence packet shows publisher time, first-observed time, URL, and content hash

    @implemented @public_evidence @future_leakage
    Scenario: A later document cannot explain an earlier movement
      Given a market movement began at "2026-07-13T12:02:00Z"
      And a relevant document was first observed at "2026-07-13T12:06:00Z"
      When the system constructs the causal chronology at the decision cutoff
      Then the document does not support a public-information explanation
      And the document is not backfilled into the earlier evidence packet

    @target_design @not_current @public_evidence @coverage
    Scenario: Abstain when the approved explanatory-source roster is incomplete
      Given the contract requires an official event source for causal interpretation
      And the source coverage state is "partial"
      When the system evaluates public-explanation coverage
      Then the evidence packet reports the uncovered source and interval
      And the system abstains from claiming that no public explanation existed

  Rule: Wallet and on-chain evidence remains factual and pseudonymous

    @target_design @not_current @wallet @behavior
    Scenario: Derive public wallet behavior without inferring a human identity
      Given public market activity is mapped to stable proxy wallet "wallet-17"
      And its historical trades were captured before the cutoff
      When the wallet specialist builds features
      Then it may compute activity age, burstiness, concentration, entry timing, and cross-market behavior
      And it refers to the actor only as "wallet-17"
      And it does not infer the wallet owner's name, employment, control, or intent

    @implemented @on_chain @verification
    Scenario: Attach verified Polygon settlement facts
      Given a public trade includes a Polygon transaction hash
      And code verifies the block, transaction, log, token, addresses, and amounts
      When the system attaches on-chain context
      Then it records those machine-verifiable facts with raw provenance
      And transaction paths and sums are computed by code
      And an LLM does not invent or alter the graph facts

    @target_design @not_current @bitcoin @linkage_limit
    Scenario: Keep an independently observed Bitcoin address as context only
      Given Bitcoin address "bc1-example" is explicitly in the approved watch set
      And no independent evidence links it to a prediction-market wallet
      When the system analyzes Bitcoin transactions for the event
      Then it may report verified transaction and UTXO facts for "bc1-example"
      And it does not infer common control with the prediction-market wallet
      And it does not use heuristic address clustering as an identity claim

  Rule: SEC information is mapped as public context and a separate source-domain corpus

    @target_design @not_current @sec @point_in_time
    Scenario: Map a Form 4 to an issuer-linked prediction market
      Given the contract has a documented issuer mapping
      And a Form 4 for that issuer was accepted by EDGAR before the cutoff
      And the raw filing records reporting-person CIK, issuer CIK, role, transaction code, shares, price, ownership, and acceptance time
      When the system attaches SEC evidence
      Then the filing may contribute to the public-information chronology
      And acceptance time and first-observed time govern public availability
      And transaction time alone does not make the filing public earlier

    @target_design @not_current @sec @corpus_mapping
    Scenario Outline: Preserve legal-status and transaction-mapping confidence separately
      Given an enforcement episode has legal status "<legal_status>"
      And its public records have mapping grade "<mapping_grade>"
      When the system creates an insider-trading case mapping
      Then the legal status is not collapsed into a binary fraud label
      And the transaction mapping grade is retained independently
      And the case is eligible for supervised outcome training only when policy explicitly permits that status and grade

      Examples:
        | legal_status                    | mapping_grade |
        | complaint_or_charge             | D             |
        | settlement_or_consent_order     | B             |
        | final_civil_judgment             | A             |
        | criminal_conviction              | A             |
        | dismissal_or_exonerating_outcome | B             |

    @target_design @not_current @sec @domain_boundary
    Scenario: Do not treat ordinary Form 4 transactions as fraud labels
      Given an as-filed Form 4 reports an open-market purchase or sale
      When the SEC corpus is prepared for representation learning
      Then the transaction may be used to learn normal disclosed-insider chronology
      And the transaction is not labeled illegal or fraudulent
      And grants, gifts, derivatives, amendments, and 10b5-1 indicators remain distinct context

  Rule: Collection expands through an evidence waterfall

    @target_design @not_current @cascade @collection
    Scenario: A preliminary market red flag triggers bounded deeper collection
      Given low-cost market and reference collectors observe an unexplained residual movement
      And the trigger is an operational collection signal rather than a misconduct conclusion
      When the collection cascade expands the event
      Then it requests relevant sibling-market, public-document, wallet, and verified chain context
      And it requests SEC context only for a documented issuer-linked contract
      And every requested source records success, partial coverage, stale data, gap, or unmapped status
      And the expanded collection preserves the original decision cutoff

    @target_design @not_current @cascade @budget
    Scenario: Collection depth respects source relevance and resource limits
      Given multiple preliminary events compete for deeper collection
      When the cascade ranks collection jobs
      Then it uses expected information gain, source cost, freshness, and event urgency
      And failure to collect a modality becomes explicit missingness
      And missing collection does not become evidence of benign or suspicious conduct

  Rule: Human adjudication uses multiple axes

    @implemented @labels @human_only
    Scenario Outline: Store mechanism, evidence strength, and disposition independently
      Given a human adjudicator reviews event "event-17"
      When the adjudicator records mechanism "<mechanism>"
      And records evidence strength "<strength>"
      And records disposition "<disposition>"
      Then the immutable label object preserves all three axes and its supporting evidence identifiers
      And the label is marked human-adjudicated and not model-generated

      Examples:
        | mechanism                    | strength      | disposition             |
        | thin_liquidity_artifact      | corroborated  | benign_mechanical        |
        | public_information_response  | limited       | benign_public_response   |
        | unexplained_activity         | conflicting   | escalate_for_review      |
        | mixed                        | limited       | unresolved               |

    @implemented @labels @training_gate
    Scenario Outline: Unknown or unmapped adjudication is never a negative example
      Given a human adjudication has "<axis>" value "<value>"
      When training readiness is evaluated
      Then the adjudication is not training eligible
      And it is not converted to a benign, non-fraud, or zero target

      Examples:
        | axis                 | value    |
        | observable_mechanism | unknown  |
        | observable_mechanism | unmapped |
        | evidence_strength    | unknown  |
        | disposition          | unmapped |

    @target_design @not_current @labels @llm_boundary
    Scenario: An LLM extracts claims but cannot adjudicate ground truth
      Given an LLM extracts entities and claims from a public document
      When the evidence packet is assembled
      Then extracted claims retain links to machine-verifiable source spans
      And the LLM may format a chronology or audit evidence completeness
      And it cannot create a human adjudication or final probability of misconduct

  Rule: Training requires frozen, leakage-resistant evidence

    @implemented @training @readiness
    Scenario Outline: Reject candidate evaluation when a current readiness prerequisite is missing
      Given readiness prerequisite "<prerequisite>" is not satisfied
      When training readiness is evaluated
      Then readiness is "not_ready"
      And the failed prerequisite is returned as a reason code
      And no model is approved or published

      Examples:
        | prerequisite                                      |
        | causal feature rows at the cutoff                 |
        | required source coverage and market context       |
        | eligible human multi-axis adjudications           |
        | fixed evaluation plan and required holdout groups |
        | sufficient calibration counts and class coverage  |

    @implemented @training @splits
    Scenario: Keep current holdout groups out of different data partitions
      Given one enforcement episode contains 23 related contracts
      When the corpus is partitioned into training, calibration, and test sets
      Then all 23 contracts remain in the same partition
      And current partitions are disjoint by event cluster, market, actor, and forward time as applicable

    @target_design @not_current @training @positive_unlabeled
    Scenario: Unprosecuted activity remains unlabeled
      Given public market history contains no enforcement outcome for an actor
      When supervised targets are constructed
      Then the actor is not assumed innocent
      And its activity remains unlabeled unless independently adjudicated
      And evaluation does not report ordinary unlabeled observations as proven negatives

  Rule: Shared-private neural learning is a target design and not a current capability

    @target_design @not_current @neural
    Scenario: Encode observed modalities into shared and private representations
      Given approved numeric inputs and missingness masks exist for market, reference, public evidence, contract, wallet, chain, and SEC modalities
      When the target neural architecture performs a forward pass
      Then each observed specialist encoder produces a private representation
      And each observed specialist encoder projects a shared event representation
      And masked late fusion combines representations with freshness and reliability
      And an unavailable modality contributes a missingness mask rather than invented values
      But the scenario does not assert that trained or approved neural weights currently exist

    @target_design @not_current @losses
    Scenario: Optimize the proposed multi-objective neural loss only on eligible data
      Given training readiness has passed for a frozen experiment
      When the proposed neural training loop is implemented and executed
      Then total loss may combine class-balanced mechanism loss
      And evidence-strength loss
      And cross-modal contrastive loss
      And private-shared redundancy regularization
      And modality-dropout consistency loss
      And temporal masked-prediction loss
      And no fraud or identity loss is created without independently adjudicated target labels
      But the scenario does not assert that this training loop currently exists

    @target_design @not_current @sec @ablation
    Scenario: Promote SEC pretraining only when issuer-disjoint ablations show value
      Given a frozen issuer-linked target corpus and evaluation plan exist
      When deterministic SEC features, target-only learning, SEC pretraining, and SEC fine-tuning are compared
      Then the target corpus partitions are disjoint by issuer
      And evaluation uses unseen issuers and forward temporal holdouts
      And placebo time shifts and unrelated-issuer mappings are tested
      And SEC transfer is removed if it does not improve selective operational performance

  Rule: Restraint precedes analyst routing

    @implemented @ood @abstention
    Scenario Outline: Abstain when a restraint prerequisite fails
      Given a fused event has restraint condition "<condition>"
      When the selective abstention policy evaluates the event
      Then the event is not escalated automatically
      And the abstention reason is "<reason>"

      Examples:
        | condition                         | reason                       |
        | fewer than required modalities    | insufficient_modalities      |
        | high modality disagreement        | cross_modality_disagreement  |
        | weak mechanism support            | weak_operational_support     |
        | calibration required but missing  | calibration_unavailable      |
        | insufficient OOD reference data   | ood_reference_unavailable    |
        | OOD score above policy threshold  | out_of_distribution          |

    @implemented @ood @semantics
    Scenario: OOD score is not a fraud probability
      Given an event has a high k-nearest-neighbor distance and energy-like OOD score
      When the event is reported to an analyst
      Then the system describes it as unlike the reference population
      And it does not describe the OOD score as a probability of fraud, intent, or wrongdoing

    @implemented @conformal @queue
    Scenario: Route only with available historical feedback and analyst capacity
      Given at least the configured minimum analyst feedback was available before the cutoff
      And a candidate passes abstention and exceeds the rolling operational threshold
      And the analyst daily budget has remaining capacity
      When the conformal controller routes the candidate
      Then the candidate may enter the analyst queue
      And later feedback cannot change the historical routing threshold
      And "supported" feedback means useful to the analyst workflow rather than proven misconduct

    @implemented @conformal @queue
    Scenario: Withhold escalation when feedback or queue capacity is unavailable
      Given conformal feedback history is insufficient or the analyst daily budget is exhausted
      When the conformal controller routes candidates
      Then affected candidates are withheld
      And the reason is "conformal_history_unavailable" or "analyst_budget_exhausted"

  Rule: Evaluation reflects rare-event operations rather than a single ranking score

    @target_design @not_current @evaluation @metrics
    Scenario: Report ranking, calibration, selectivity, and analyst-budget metrics
      Given a frozen model and untouched test partition exist
      When evaluation is performed
      Then the report includes AUROC as a ranking diagnostic
      And it includes precision at K and recall at a fixed analyst budget
      And it includes false escalations per analyst-day
      And it includes Brier score and expected calibration error
      And it includes coverage versus selective risk
      And it includes OOD performance and exact-retrieval Recall at K
      And AUROC alone cannot pass the effectiveness gate

    @implemented @evaluation @prospective
    Scenario: Do not claim effectiveness without prospective evaluation
      Given retrospective evaluation has completed
      But no events collected after model freeze have been independently adjudicated
      When system status is reported
      Then empirical status remains "effectiveness_unknown"
      And the system is not described as a validated fraud predictor

  Rule: Serving is immutable, read-only, and bounded

    @implemented @serving @bundle
    Scenario: Reject an unverifiable serving bundle identity
      Given a serving manifest omits a required artifact hash binding or an artifact hash does not verify
      When the service reads bundle metadata
      Then bundle identity verification fails
      And serving remains unavailable
      And no model bytes are loaded
      And no live inference is performed

    @implemented @serving @read_only
    Scenario: Keep a hash-verified bundle policy-blocked without operational attestation
      Given an immutable bundle manifest and all bound artifact hashes verify
      And separate operational attestation is absent
      When a client requests model status or a precomputed assessment
      Then the service may return verified bundle identity metadata, frozen routing or abstention records, and safe point-in-time lineage
      And model status remains policy-blocked
      And hash verification is not represented as model approval
      And the endpoint does not train, approve, mutate, or publish a model
      And the endpoint does not expose raw sensitive evidence

  Rule: Outputs cannot assert prohibited conclusions

    @target_design @not_current @safety @prohibited_conclusion
    Scenario Outline: Reject a prohibited generated conclusion
      Given a component generates statement "<statement>"
      When the output policy validates the statement
      Then the statement is rejected
      And the system substitutes a bounded observation or abstention
      And the original statement cannot enter an evidence packet or analyst-facing result

      Examples:
        | statement                                                   |
        | This person committed insider trading                       |
        | Wallet 17 is controlled by the issuer's chief executive     |
        | The OOD score proves fraudulent intent                      |
        | An unexplained price movement is proof of wrongdoing        |

    @target_design @not_current @safety @bounded_output
    Scenario: Produce a factual evidence packet for an unexplained event
      Given point-in-time coverage, provenance, and restraint gates pass
      And no supported observable mechanism fully explains the activity
      When the system produces an analyst-facing result
      Then it may report mechanism "unexplained_activity"
      And it reports evidence strength and material alternative explanations
      And it identifies missing, stale, partial, and conflicting evidence
      And it says the event deserves human review rather than asserting fraud or intent
