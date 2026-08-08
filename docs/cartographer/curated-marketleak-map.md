# Curated MarketLeak capability map

## Why it exists

The raw cartographer inventory answers code-level questions precisely, but thousands of symbol-derived capabilities are not a usable development roadmap. The curated map is a deterministic projection from that verified evidence into stable MarketLeak engineering capability IDs.

It answers which high-level capability is under discussion, which exact low-level records matched at this inventory root, and what the independently supported definition, integration, verification, artifact, learning, evaluation, and runtime states are. It does not answer whether MarketLeak is effective, whether a model should be approved, or whether an alert establishes wrongdoing.

## Checked-in taxonomy

The versioned profile is [marketleak-capabilities.json](../../configs/cartographer/marketleak-capabilities.json). It covers the current engineering foundation and explicit target gaps:

- raw/normalized ingestion, coverage, quality, venue feeds, reference prices, market context, and microstructure;
- wallet enrollment, history, actor features, cohort ranking, priority, replay, on-chain context, and deterministic graph corroboration;
- public collection, archive, matching, SEC Forms 3/4/5, and enforcement-case corpus gaps;
- event memory, causal features, labels, embeddings, exact retrieval, fusion, abstention, baselines, the experimental neural shell, and absent trained specialists;
- bundle governance, evaluation, shadow operation, read-only serving, frozen review packets, and the application/agent shell;
- the current cartographer MVP and explicit Tree-sitter, SCIP, semantic-retrieval, and Antigravity-ledger targets.

The experimental neural shell has its own ID. It must not be merged with trained wallet specialists or trained multimodal fusion: architecture code and learned parameters are different facts.

## Deterministic use loop

1. Scan and verify a deliberate Git/dirty-worktree state.
2. Project that inventory with the checked-in profile.
3. Use `map-explain` on the capability selected for the next slice.
4. Run separately authorized verification with the checked-in pytest runner v2 policy and an operator/CI-held attestation key.
5. Rescan, reproject, and use `map-diff` with the same profile hash.

A profile edit changes the question, not the repository's implementation. Consequently, `map-diff` rejects profile-hash mismatch. Review taxonomy changes separately, generate both maps again under the accepted profile, and only then compare repository progress.

## Selector boundary

Selectors are exact and reviewable. Semantic similarity may eventually propose selector candidates, but it cannot enter this projection path or promote a state. Required misses are useful output: they identify a declared gap instead of disappearing. Optional matches appear in explanations but cannot upgrade the capability.

Executed-test promotion additionally requires a v2 receipt whose HMAC-SHA256 `key_id` resolves to a trusted external key and whose exact runner-policy hash resolves to an explicitly approved checked-in configuration. The map binds every consumed policy hash and receipt hash. A self-hash, unknown policy, missing key resolver, bad signature, or legacy v1 receipt cannot promote `verification`.

No aggregate completion percentage is calculated. The independent axes are the decision surface; collapsing them would recreate the ambiguity this subsystem was built to remove.
