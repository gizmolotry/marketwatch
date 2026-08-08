# CI trust and provenance boundary

The ordinary `python` job in the [CI workflow](../../.github/workflows/ci.yml) runs engineering checks for every `pull_request` and for pushes to `main`. Feature-branch pushes are not a second trigger because the pull-request event already tests those revisions. The job inherits only `contents: read`, receives no repository secrets, and disables checkout credential persistence. It installs the exact versions in `requirements.txt`, compiles the Python trees, runs the full test suite with `--basetemp` under the runner's external temporary directory, and creates and verifies a fresh static Cartographer inventory.

UI CI is explicitly unavailable at present. Although `ui/package-lock.json` exists, a clean `npm ci` rejects it as inconsistent with `ui/package.json`. The ordinary workflow must not replace that deterministic check with a mutable `npm install`. Restore the UI build job only after the dependency declaration and lockfile are intentionally synchronized and `npm ci && npm run build` succeeds.

A successful run proves only that those engineering checks passed for the tested revision. Static Cartographer verification does not execute or attest a receipt, approve a capability, establish runtime reachability, or provide evidence of MarketLeak effectiveness.

## Untrusted pull-request boundary

The workflow deliberately uses `pull_request`, not `pull_request_target`. GitHub documents that fork pull-request workflows receive a read-only `GITHUB_TOKEN` and no repository secrets. GitHub also warns that checking out and executing untrusted pull-request code in a privileged `pull_request_target` or similar workflow creates a "pwn request" risk. This repository does not use that pattern.

Jobs run on GitHub-hosted ephemeral runners with explicit timeouts and share a concurrency group per workflow/ref so a newer revision cancels stale work. Tested code and dependency installation can execute arbitrary code, so the ordinary pull-request path receives neither a secret nor a write-capable token. No workflow output from an untrusted run is later downloaded and executed in the trusted job.

The workflow-level permission declaration is intentionally limited to:

```yaml
permissions:
  contents: read
```

GitHub recommends granting `GITHUB_TOKEN` the least access required. Every referenced action is pinned to the full commit SHA of an official release, because GitHub identifies a full-length commit SHA as the only immutable way to reference an action:

- `actions/checkout` `v7.0.1`: `3d3c42e5aac5ba805825da76410c181273ba90b1` ([release](https://github.com/actions/checkout/releases/tag/v7.0.1));
- `actions/setup-python` `v7.0.0`: `5fda3b95a4ea91299a34e894583c3862153e4b97` ([release](https://github.com/actions/setup-python/releases/tag/v7.0.0));

## Trusted receipt provenance on `main`

The separate `trusted-cartographer-provenance` job runs only for a `push` whose exact ref is `refs/heads/main`. There is no manual, pull-request, `pull_request_target`, or `workflow_run` entry point. It checks out the exact `github.sha`, disables credential persistence, and retains only `contents: read` permission.

The job installs only the Cartographer test path's direct pinned runtime dependencies, `pytest==9.0.2` and `jsonschema==4.25.1`. It then creates and verifies a fresh inventory, executes the approved `tests/cartographer` selection through the checked-in runner v2 policy, creates an HMAC-SHA256 receipt with nonsecret key ID `github-main-v1`, verifies the receipt against the same inventory, runner policy, key ID, and protected key, and builds a receipt-aware curated map. It fails if the receipt is valid but non-promotable. Only then does it upload the inventory, signed receipt, and map as one immutable workflow artifact. The upload action is pinned to its full official release SHA:

- `actions/upload-artifact` `v7.0.1`: `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a` ([release](https://github.com/actions/upload-artifact/releases/tag/v7.0.1)).

The HMAC key is supplied only through the named environment variable on the explicit key-check and receipt run/verify/map steps. It is not placed on argv, written to the receipt, inherited by the bounded pytest subprocess, or exposed to checkout, Python setup, or artifact-upload actions. A missing or empty secret fails the job with `provenance_unavailable`; no artifact or promotion claim is emitted.

### Secret setup and rotation

Repository administrators must create an Actions repository secret named `CARTOGRAPHER_RECEIPT_HMAC_KEY` under **Settings → Secrets and variables → Actions**. Generate at least 32 bytes with a cryptographically secure local secret generator, paste only the generated value into GitHub, and do not store it in the repository, workflow, shell history, logs, artifacts, or a command-line argument. Protect `main` and require review for workflow, runner-policy, Cartographer, and `tests/cartographer` changes because code on trusted `main` is authorized to request this secret.

For rotation, generate a new independent key, store it under a new protected secret during a reviewed transition, and change the public key ID to the next version rather than reusing `github-main-v1`. Preserve the retired key in controlled archival secret storage for verification of historical receipts, remove it from active CI after the transition, and never rewrite old receipts or maps. Emergency rotation should also cancel active runs and review artifacts created since the suspected exposure.

GitHub artifact attestation is not enabled in this workflow. The required Cartographer HMAC is the current trust contract, while GitHub-hosted artifact attestations add write permissions and their availability for private repositories depends on the GitHub plan. If enabled later, keep it confined to this trusted job with the documented `contents: read`, `id-token: write`, and `attestations: write` permissions and do not treat it as approval or effectiveness evidence.

## Official security references

- [Securely using `pull_request_target`](https://docs.github.com/en/actions/reference/security/securely-using-pull_request_target)
- [Events that trigger workflows: forked repositories](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflows-in-forked-repositories)
- [Secure use reference: pin actions and least privilege](https://docs.github.com/en/actions/reference/security/secure-use)
- [Use `GITHUB_TOKEN` for authentication in workflows](https://docs.github.com/en/actions/tutorials/use-github_token-in-workflows)
- [Workflow syntax: permissions, concurrency, and timeouts](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax)
- [Using artifact attestations to establish provenance](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
