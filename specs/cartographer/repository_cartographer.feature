@cartographer @additive
Feature: Evidence-first repository capability inventory
  The Repo Cartographer is an additive MarketLeak development aid. It records
  repository facts and their evidence at a Git state; it does not replace the
  existing MarketLeak specifications, make a model decision, or establish
  prediction effectiveness.

  @implemented @mvp @snapshot
  Scenario: Freeze the Git and dirty-worktree context before discovery
    Given a Git working tree
    When the cartographer creates a repository snapshot
    Then it records the HEAD commit, branch, index tree, dirty flag, and separate staged, modified, deleted, and untracked path sets
    And it does not make an absolute checkout location part of the stable snapshot identity

  @implemented @mvp @file_bytes
  Scenario: Inventory a file by normalized relative path and exact bytes
    Given an eligible repository file
    When the cartographer inventories it
    Then it records a repository-relative POSIX path, byte size, SHA-256 digest, tracked state, file kind, and parse limitation
    And a path that escapes the repository is rejected

  @implemented @mvp @python_ast
  Scenario: Discover Python structure without importing project code
    Given an eligible non-sensitive Python source file
    When the cartographer parses its source syntax
    Then it records static symbols, imports, decorators, signatures, and recognized static entrypoints with source locations
    And it does not import, execute, or network-load the source file

  @implemented @mvp @tests @gherkin
  Scenario: Inventory declared tests and Gherkin scenarios as evidence
    Given eligible Python tests and feature files
    When the cartographer discovers the repository
    Then it records test declarations, fixture-only markers, feature/rule/scenario names, tags, and source locations
    And a declaration is not represented as a passed test or proof of effectiveness

  @implemented @mvp @artifacts
  Scenario: Inspect artifact bytes without loading an executable artifact format
    Given an artifact file or JSON manifest within the configured byte limit
    When the cartographer inspects artifacts
    Then it records opaque byte identity, bounded format hints, and explicit manifest hash bindings when present
    And it does not deserialize pickle, joblib, Torch, ONNX, or model runtime objects

  @implemented @mvp @status
  Scenario: Derive conservative capability axes from static evidence
    Given discovered components, tests, specifications, artifacts, and explicit negative markers
    When the cartographer derives component and capability facts
    Then it keeps definition, integration, verification, artifact, learning, evaluation, and runtime states separate
    And static evidence cannot claim dynamically reachable runtime, test-passed verification, trained-verified learning, or effectiveness

  @implemented @mvp @determinism @verify @explain
  Scenario: Produce reproducible, inspectable inventory evidence
    Given the same frozen Git state, scan configuration, and file bytes
    When the cartographer runs the MVP discovery and derivation functions again
    Then equivalent inventory facts have the same stable identifiers and ordered evidence
    And a capability can be traced to supporting or refuting evidence identifiers and reason codes
    And hash verification proves byte identity only, not approval, training provenance, runtime reachability, or effectiveness

  @implemented @curated_map @exact_selectors
  Scenario: Project a verified inventory into stable MarketLeak capabilities
    Given a hash-verified rendered inventory and a strict versioned curated profile
    When the cartographer maps exact capability names, scenario names or tags, and artifact paths
    Then it emits stable curated capability identifiers bound to the profile hash and source inventory root hash
    And a missing required selector remains explicit and contributes the weakest applicable state
    And an optional selector is traceable but cannot promote a state

  @implemented @curated_map @independent_axes @static_boundary
  Scenario: Aggregate curated states without a done or effectiveness score
    Given several required exact selectors for one curated capability
    When their independently applicable states are aggregated
    Then the weakest required state is retained on each independent axis
    And test selectors contribute only verification and evaluation evidence
    And scenario selectors contribute only documentation and test-declaration evidence
    And artifact selectors contribute only artifact evidence
    And static projection never emits dynamically reachable, test passed, trained verified, or running
    And no single health, done, approval, or effectiveness score is emitted

  @implemented @curated_map @determinism @diff
  Scenario: Explain and compare curated maps without changing the taxonomy silently
    Given canonical curated maps with stable capability identifiers
    When an operator explains one identifier or compares two maps
    Then the explanation retains exact selector matches and source/profile bindings
    And the diff reports changes by stable identifier
    And maps with different profile hashes are rejected from repository-progress comparison

  @implemented @test_receipt @pytest @frozen_copy
  Scenario: Execute only approved inventoried pytest declarations
    Given a verified inventory, a strict checked-in pytest runner v2 policy, and an operator-held attestation key
    When an operator runs approved test UIDs or test paths
    Then the cartographer rejects an excessive eligible source-file count or aggregate source byte size before copying or execution
    Then the cartographer hash-checks eligible source files into a temporary frozen copy
    And it invokes the current interpreter and pytest with a fixed repository root and without a shell, plugin autoload, inherited pytest options, or arbitrary arguments
    And a timeout terminates the whole isolated process tree
    And it records bounded native collection and definition locations, setup, call, teardown, skip, xfail, exit, and mutation facts without raw output or environment secrets
    And event overflow emits a bounded explicit non-promotable record
    And it atomically emits an HMAC-SHA256-attested v2 receipt for a completed passing or failing run when possible

  @implemented @test_receipt @verification @fail_closed
  Scenario: Promote only completely passed mapped test declarations
    Given a verified v2 pytest receipt bound to the exact inventory, an approved checked-in runner policy, and a trusted external attestation key
    When a curated capability has an explicit required exact test-capability selector
    Then each collected repository path, definition line, and original name must map one-to-one to the exact inventoried declaration and node ID
    Then every collected parameter case for every mapped non-fixture declaration must pass setup, call, and teardown plainly
    And ambiguity, duplicate identity, shadowing, dynamic node rewriting, skip, xfail, xpass, failure, error, collection failure, nonzero exit, unmapped selection, timeout, or mutation prevents promotion
    And a selected receipt never promotes an unselected declaration or an optional selector
    And receipt evidence can change only the verification axis
    And the curated map binds the consumed runner-policy and receipt hashes
    And a self-hash, unknown policy, absent resolver, bad signature, or legacy v1 receipt never promotes
    And a static map without receipts remains declaration-only

  @target_design @not_current @tree_sitter
  Scenario: Parse multiple languages with pinned Tree-sitter grammars
    Given a repository containing supported languages beyond Python
    When a future scanner performs syntax discovery
    Then it uses declared parser versions and emits parser coverage and parse-failure evidence

  @target_design @not_current @scip
  Scenario: Resolve cross-file references with SCIP or an equivalent pinned index
    Given a reproducible language index for the frozen repository state
    When a future cartographer resolves references
    Then it distinguishes syntactic imports from verified symbol-level references and preserves index provenance

  @target_design @not_current @semantic_retrieval
  Scenario: Use semantic retrieval only to propose review candidates
    Given a versioned semantic index over evidence-bearing repository chunks
    When a future query returns similar components
    Then similarity proposes candidates for review and cannot independently promote a capability state

  @target_design @not_current @agent_ledger @antigravity
  Scenario: Attach agent work to a frozen repository snapshot
    Given a future Antigravity or Codex adapter has an explicit task and snapshot identifier
    When it records an agent handoff
    Then it preserves routing rationale, commands, changed paths, and verification evidence as append-only ledger facts
    And it does not replace Antigravity token-efficient routing or infer an agent action from incomplete logs
