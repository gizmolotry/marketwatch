# Repo Cartographer preservation map

## Decision

Repo Cartographer is being built **inside** MarketLeak as a small additive subsystem. It is not a rewrite, migration, or replacement project. Existing behavior and historical Git context remain intact; cartography consumes repository facts and helps make the next MarketLeak implementation decision auditable.

## Preserve unchanged

| Existing asset | Preservation rule |
|---|---|
| [AGENTS.md](../../AGENTS.md) and [.agents/AGENTS.md](../../.agents/AGENTS.md) | Remain the governing MarketLeak safety, evidence, and fixture rules. A nested cartographer contract may add constraints but cannot weaken them. |
| [marketleak.feature](../../specs/marketleak.feature) | Remains the legacy/v2 acceptance contract; do not rewrite it for cartographer concepts. |
| [phase15_multimodal.feature](../../specs/phase15_multimodal.feature) | Remains the Phase 15/current-vs-target contract; its `@implemented` and `@target_design @not_current` meanings are preserved. |
| Existing `marketleak/` modules, tests, manifests, receipts, and data policies | Stay MarketLeak-specific sources of engineering and evidence truth. The cartographer reads their presence; it does not replace their contracts. |
| Architecture and validation documents | Remain authoritative for MarketLeak claims, provenance, causal time, training, bundle governance, and `effectiveness_unknown`. |
| Git history and unrelated working-tree changes | Are preserved. A dirty state is inventory evidence, not permission to reset, discard, or normalize someone else's work. |

## Generalize carefully

| Existing idea | Generic cartographer use | Boundary |
|---|---|---|
| Canonical hashes and manifests | Stable byte identity and inventory comparison | A hash is not model approval or effectiveness evidence. |
| Gherkin and executable tests | Link declarations and tests to capabilities | A scenario or declared test is not a passed test. |
| Provenance and explicit missingness | Evidence UIDs, source hashes, reason codes, and negative markers | The cartographer does not invent coverage or runtime observations. |
| Retrieval and model artifacts | Recognize files and manifest bindings | Current MVP does not perform semantic retrieval or load artifacts. |
| Agent instructions | Nested rules for cartographer changes | They do not override root safety or data rules. |

## MarketLeak-specific, not generalized away

MarketLeak's causal cutoff rules, source coverage semantics, market/on-chain/public-evidence schemas, labels, bundle gates, read-only v3 surface, and claims boundary are retained as domain-specific behavior. The cartographer must never relabel them into generic “implemented” marketing claims. In particular, no cartographer output turns a model shell, baseline evaluation command, artifact hash, or static serving route into a trained fraud predictor or a proven effective system.

## Future Antigravity adapter

Antigravity means the user's Gemini CLI environment in this workflow. Its existing token-efficient-routing skill answers **which model should handle a task**. Repo Cartographer answers **what repository evidence exists at a frozen state**. These roles complement one another:

```text
Cartographer snapshot and evidence -> token-efficient router -> selected agent -> future append-only task ledger -> verification evidence
```

The arrow after the router is target design only. The MVP does not inspect Gemini sessions, logs, prompts, credentials, or agent activity. It does not replace token-efficient routing, change its routing policy, or claim that a routed task is correct. Any future adapter must be explicit, local, bounded, provenance-aware, and separately tested.
