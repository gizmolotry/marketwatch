"""Evidence-first, deterministic repository inventory primitives."""

from .canonical import canonical_json_bytes, canonical_sha256, normalize_repo_path, sha256_file, stable_sha256
from .config import load_scan_config
from .curated import diff_curated_maps, explain_curated_map, load_curated_map, project_curated_map, write_curated_map
from .curated_config import curated_profile_from_dict, load_curated_profile
from .cli import explain_inventory, scan_repository
from .domain import *
from .domain import __all__ as _domain_all
from .render import RenderResult, write_inventory
from .verify import VerificationResult, verify_inventory
from .test_receipts import (
    ATTESTATION_ALGORITHM,
    LEGACY_RECEIPT_FORMAT,
    RECEIPT_FORMAT,
    ReceiptAttestationSigner,
    VerifiedTestReceipt,
    declaration_key,
    run_pytest_receipt,
    verify_test_receipt,
)
from .test_runner_config import PytestRunnerConfig, load_pytest_runner_config, runner_config_from_dict

__all__ = [
    *_domain_all,
    "canonical_json_bytes",
    "canonical_sha256",
    "curated_profile_from_dict",
    "diff_curated_maps",
    "explain_inventory",
    "explain_curated_map",
    "load_curated_map",
    "load_curated_profile",
    "load_scan_config",
    "normalize_repo_path",
    "project_curated_map",
    "sha256_file",
    "scan_repository",
    "stable_sha256",
    "RenderResult",
    "VerificationResult",
    "verify_inventory",
    "write_inventory",
    "write_curated_map",
    "RECEIPT_FORMAT",
    "LEGACY_RECEIPT_FORMAT",
    "ATTESTATION_ALGORITHM",
    "ReceiptAttestationSigner",
    "VerifiedTestReceipt",
    "declaration_key",
    "run_pytest_receipt",
    "verify_test_receipt",
    "PytestRunnerConfig",
    "load_pytest_runner_config",
    "runner_config_from_dict",
]
