# Repo Cartographer contract

This directory contains an additive, evidence-first repository inventory for MarketLeak. Read the repository-root [AGENTS.md](../AGENTS.md) and [.agents/AGENTS.md](../.agents/AGENTS.md) first; this contract adds constraints and never weakens them.

## Purpose and boundary

- The MVP inventories Git/file/static-source/test/spec/artifact facts. It is not a MarketLeak model, source collector, legal analysis tool, agent runtime, or effectiveness evaluator.
- Preserve the existing MarketLeak Gherkin, source contracts, tests, documents, data, artifacts, and Git history. Do not move or rewrite them to make cartographer output cleaner.
- Keep current behavior distinct from target design. Use `@implemented` only for fresh, tested cartographer mechanics. Tag future Tree-sitter, SCIP, semantic retrieval, runtime probe, agent-ledger, and Antigravity integration scenarios `@target_design @not_current`.

## Evidence rules

- Normalize only repository-relative paths. Absolute checkout roots and scan timestamps must not affect stable identities.
- Do not execute, import, or network-load code while scanning. Use static parsing only.
- Treat executable artifacts as opaque bytes. Do not deserialize pickle, joblib, Torch, ONNX, or similar formats. A JSON manifest can be parsed only under the configured size bound.
- Keep sensitive, oversized, missing, parse-failed, ignored, untracked, and contradictory states explicit. Never treat them as clean negatives.
- A hash proves byte identity only. A declaration, static import, documentation claim, Gherkin scenario, or fixture test does not prove runtime behavior, test success, training, approval, deployment, or MarketLeak effectiveness.
- Never claim fraud, intent, identity, legal outcome, model quality, or effectiveness from a cartographer fact.

## Change and verification rules

- Use the frozen records and enum vocabularies in `domain.py`; do not add optimistic aliases or collapse status axes.
- For behavior changes, update the additive feature contract under `specs/cartographer/`, focused mechanical tests, and relevant `docs/cartographer/` files together.
- Run the focused cartographer tests with a unique `--basetemp`, then report exact commands and results. Tests using temporary files are fixture-only mechanics, not real-world evidence.
- Do not commit generated inventories, caches, model weights, corpora, secrets, temporary pytest directories, or external agent logs.
