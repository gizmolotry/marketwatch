"""Experimental shared/private event-space architecture.

The module accepts precomputed numeric modality features only.  It neither
collects data nor loads parameter bundles, so an unavailable optional runtime
cannot be replaced with an implicit heuristic or served silently.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
import warnings

from pydantic import Field, field_validator, model_validator

from marketleak.domain.common import NonEmptyStr, StableUID
from marketleak.multimodal.schemas import Phase15Model, canonical_hash


try:  # Optional by design: the rest of the event-memory stack stays usable.
    import torch
    from torch import Tensor, nn

    TORCH_AVAILABLE = True
    _TORCH_REASON = "PyTorch is available; this architecture remains experimental."
except ImportError:  # pragma: no cover - exercised only in a no-torch environment.
    torch = None  # type: ignore[assignment]
    Tensor = Any  # type: ignore[misc,assignment]
    nn = None  # type: ignore[assignment]
    TORCH_AVAILABLE = False
    _TORCH_REASON = "PyTorch is not installed, so the experimental event-space is unavailable."


NEURAL_SCHEMA_VERSION = "15.0.0-neural-event-space-v1"
MECHANISM_LABELS = (
    "thin_liquidity_artifact",
    "public_information_response",
    "scheduled_event_response",
    "underlying_reference_move",
    "sibling_market_repricing",
    "market_maker_rebalance",
    "outage_or_recovery",
    "unexplained_activity",
    "mixed",
    "unknown",
    "unmapped",
)
EVIDENCE_LABELS = (
    "absent",
    "conflicting",
    "limited",
    "corroborated",
    "unknown",
    "unmapped",
)


class NeuralUnavailableError(RuntimeError):
    """Raised instead of silently substituting a different inference path."""


class BundleApprovalRequired(RuntimeError):
    """Raised when experimental execution is requested in a protected context."""


class ModalityInputError(ValueError):
    """Raised for ambiguous feature/mask pairs before tensors reach a layer."""


class NeuralAvailability(Phase15Model):
    available: bool
    reason: NonEmptyStr
    experimental_only: Literal[True] = True
    serves_without_runtime: Literal[False] = False


def neural_status() -> NeuralAvailability:
    """Return an explicit capability status without constructing a model."""

    return NeuralAvailability(available=TORCH_AVAILABLE, reason=_TORCH_REASON)


class EventSpaceSpec(Phase15Model):
    """Serializable architecture-only specification; it contains no weights."""

    schema_version: NonEmptyStr = NEURAL_SCHEMA_VERSION
    market_input_dim: int = Field(strict=True, ge=1)
    evidence_input_dim: int = Field(strict=True, ge=1)
    onchain_input_dim: int = Field(strict=True, ge=1)
    private_dim: int = Field(default=32, strict=True, ge=1)
    shared_dim: int = Field(default=16, strict=True, ge=1)
    fusion_dim: int = Field(default=48, strict=True, ge=1)
    mechanism_labels: tuple[str, ...] = MECHANISM_LABELS
    evidence_labels: tuple[str, ...] = EVIDENCE_LABELS

    @field_validator("mechanism_labels")
    @classmethod
    def validate_mechanism_labels(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if tuple(value) != MECHANISM_LABELS:
            raise ValueError("mechanism_labels must use the fixed observable-mechanism ontology")
        return value

    @field_validator("evidence_labels")
    @classmethod
    def validate_evidence_labels(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if tuple(value) != EVIDENCE_LABELS:
            raise ValueError("evidence_labels must use the fixed evidence-strength ontology")
        return value

    @property
    def spec_hash(self) -> str:
        return canonical_hash(self.model_dump(mode="json"))


class NeuralBundleManifest(Phase15Model):
    """Future-bundle manifest that proves this module has not loaded weights."""

    bundle_uid: StableUID
    spec: EventSpaceSpec
    spec_hash: str
    weights_loaded: Literal[False] = False
    experimental_only: Literal[True] = True

    @field_validator("spec_hash")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("spec_hash must be a SHA-256 hex digest")
        return value

    @model_validator(mode="after")
    def validate_spec_fingerprint(self) -> "NeuralBundleManifest":
        if self.spec_hash != self.spec.spec_hash:
            raise ValueError("spec_hash does not match serialized architecture spec")
        return self


def architecture_manifest(spec: EventSpaceSpec) -> NeuralBundleManifest:
    """Build a deterministic, serializable future-bundle envelope without weights."""

    payload = {"schema_version": NEURAL_SCHEMA_VERSION, "spec_hash": spec.spec_hash}
    return NeuralBundleManifest(
        bundle_uid=f"neural:{canonical_hash(payload)}",
        spec=spec,
        spec_hash=spec.spec_hash,
    )


def require_verified_bundle(*, production: bool, verified_bundle: object | None = None) -> None:
    """Keep protected execution unavailable until a verified approval exists.

    The current repository can hash and publish artifact manifests, but it has
    no independently attested approval object that binds those artifacts to the
    in-memory ``state_dict``.  Consequently no caller-supplied object, truthy
    flag, or manifest can unlock protected execution yet.
    """

    del verified_bundle
    if production:
        raise BundleApprovalRequired(
            "protected neural execution is unavailable: no verified approval object binds "
            "the in-memory state_dict, model specification, and calibration artifacts"
        )


def require_approved_bundle(*, production: bool, approved_bundle: object | None = None) -> None:
    """Deprecated compatibility wrapper; a boolean never establishes approval."""

    warnings.warn(
        "require_approved_bundle is deprecated; protected execution requires a verified artifact-bound approval",
        DeprecationWarning,
        stacklevel=2,
    )
    require_verified_bundle(production=production, verified_bundle=approved_bundle)


@dataclass(frozen=True)
class EventSpaceOutput:
    """Forward output with only the two governed task heads and fused embedding."""

    mechanism_logits: Any
    evidence_logits: Any
    fused_embedding: Any
    modality_mask: Any


if TORCH_AVAILABLE:

    class _PrivateEncoder(nn.Module):
        def __init__(self, input_dim: int, private_dim: int) -> None:
            super().__init__()
            self.layers = nn.Sequential(
                nn.Linear(input_dim, private_dim),
                nn.GELU(),
                nn.LayerNorm(private_dim),
                nn.Linear(private_dim, private_dim),
                nn.GELU(),
            )

        def forward(self, values: Tensor) -> Tensor:
            return self.layers(values)


    class ExperimentalEventSpace(nn.Module):
        """Trainable shared/private encoders with explicit modality masks and late fusion."""

        def __init__(self, spec: EventSpaceSpec) -> None:
            super().__init__()
            self.spec = spec
            self.market_private = _PrivateEncoder(spec.market_input_dim, spec.private_dim)
            self.evidence_private = _PrivateEncoder(spec.evidence_input_dim, spec.private_dim)
            self.onchain_private = _PrivateEncoder(spec.onchain_input_dim, spec.private_dim)
            self.market_shared = nn.Sequential(nn.Linear(spec.private_dim, spec.shared_dim), nn.GELU())
            self.evidence_shared = nn.Sequential(nn.Linear(spec.private_dim, spec.shared_dim), nn.GELU())
            self.onchain_shared = nn.Sequential(nn.Linear(spec.private_dim, spec.shared_dim), nn.GELU())
            per_modality_dim = spec.private_dim + spec.shared_dim + 1
            self.late_fusion = nn.Sequential(
                nn.Linear(per_modality_dim * 3, spec.fusion_dim),
                nn.GELU(),
                nn.LayerNorm(spec.fusion_dim),
            )
            self.mechanism_head = nn.Linear(spec.fusion_dim, len(spec.mechanism_labels))
            self.evidence_head = nn.Linear(spec.fusion_dim, len(spec.evidence_labels))

        @staticmethod
        def _validate_masks(
            *,
            market_mask: Tensor,
            evidence_mask: Tensor,
            onchain_mask: Tensor,
        ) -> tuple[int, Tensor, Tensor, Tensor]:
            masks = (market_mask, evidence_mask, onchain_mask)
            names = ("market_mask", "evidence_mask", "onchain_mask")
            for name, mask in zip(names, masks, strict=True):
                if not isinstance(mask, torch.Tensor) or mask.ndim != 1 or mask.dtype != torch.bool:
                    raise ModalityInputError(f"{name} must be a one-dimensional torch.bool tensor")
            batch_size = int(market_mask.shape[0])
            if batch_size < 1 or any(int(mask.shape[0]) != batch_size for mask in masks[1:]):
                raise ModalityInputError("all modality masks must have the same non-zero batch dimension")
            if not (market_mask.device == evidence_mask.device == onchain_mask.device):
                raise ModalityInputError("all modality masks must use the same device")
            return batch_size, market_mask, evidence_mask, onchain_mask

        @staticmethod
        def _validate_values(
            *,
            name: str,
            values: Tensor | None,
            mask: Tensor,
            batch_size: int,
            expected_dim: int,
        ) -> Tensor:
            if values is None:
                if bool(mask.any().item()):
                    raise ModalityInputError(f"{name} values are required where {name}_mask is true")
                return torch.zeros((batch_size, expected_dim), device=mask.device, dtype=torch.float32)
            if not isinstance(values, torch.Tensor):
                raise ModalityInputError(f"{name} values must be a torch tensor or None")
            if values.ndim != 2 or tuple(values.shape) != (batch_size, expected_dim):
                raise ModalityInputError(
                    f"{name} values must have shape ({batch_size}, {expected_dim}), got {tuple(values.shape)}"
                )
            if not values.dtype.is_floating_point:
                raise ModalityInputError(f"{name} values must use a floating point dtype")
            if values.device != mask.device:
                raise ModalityInputError(f"{name} values and {name}_mask must use the same device")
            if not bool(torch.isfinite(values).all().item()):
                raise ModalityInputError(f"{name} values must be finite")
            return values

        @staticmethod
        def _masked_features(private: Tensor, shared: Tensor, mask: Tensor) -> Tensor:
            gate = mask.to(dtype=private.dtype).unsqueeze(1)
            return torch.cat((private * gate, shared * gate, gate), dim=1)

        def forward(
            self,
            *,
            market: Tensor | None,
            evidence: Tensor | None,
            onchain: Tensor | None,
            market_mask: Tensor,
            evidence_mask: Tensor,
            onchain_mask: Tensor,
            production: bool = False,
            verified_bundle: object | None = None,
            approved_bundle: object | None = None,
        ) -> EventSpaceOutput:
            if approved_bundle is not None:
                warnings.warn(
                    "approved_bundle is deprecated and cannot unlock protected execution",
                    DeprecationWarning,
                    stacklevel=2,
                )
            require_verified_bundle(
                production=production,
                verified_bundle=verified_bundle if verified_bundle is not None else approved_bundle,
            )
            batch_size, market_mask, evidence_mask, onchain_mask = self._validate_masks(
                market_mask=market_mask,
                evidence_mask=evidence_mask,
                onchain_mask=onchain_mask,
            )
            market_values = self._validate_values(
                name="market",
                values=market,
                mask=market_mask,
                batch_size=batch_size,
                expected_dim=self.spec.market_input_dim,
            )
            evidence_values = self._validate_values(
                name="evidence",
                values=evidence,
                mask=evidence_mask,
                batch_size=batch_size,
                expected_dim=self.spec.evidence_input_dim,
            )
            onchain_values = self._validate_values(
                name="onchain",
                values=onchain,
                mask=onchain_mask,
                batch_size=batch_size,
                expected_dim=self.spec.onchain_input_dim,
            )
            market_private = self.market_private(market_values)
            evidence_private = self.evidence_private(evidence_values)
            onchain_private = self.onchain_private(onchain_values)
            fused = self.late_fusion(
                torch.cat(
                    (
                        self._masked_features(market_private, self.market_shared(market_private), market_mask),
                        self._masked_features(evidence_private, self.evidence_shared(evidence_private), evidence_mask),
                        self._masked_features(onchain_private, self.onchain_shared(onchain_private), onchain_mask),
                    ),
                    dim=1,
                )
            )
            return EventSpaceOutput(
                mechanism_logits=self.mechanism_head(fused),
                evidence_logits=self.evidence_head(fused),
                fused_embedding=fused,
                modality_mask=torch.stack((market_mask, evidence_mask, onchain_mask), dim=1),
            )

else:

    class ExperimentalEventSpace:  # pragma: no cover - depends on an optional runtime.
        def __init__(self, spec: EventSpaceSpec) -> None:
            del spec
            raise NeuralUnavailableError(_TORCH_REASON)


__all__ = [
    "BundleApprovalRequired",
    "EVIDENCE_LABELS",
    "EventSpaceOutput",
    "EventSpaceSpec",
    "ExperimentalEventSpace",
    "MECHANISM_LABELS",
    "ModalityInputError",
    "NEURAL_SCHEMA_VERSION",
    "NeuralAvailability",
    "NeuralBundleManifest",
    "NeuralUnavailableError",
    "TORCH_AVAILABLE",
    "architecture_manifest",
    "neural_status",
    "require_approved_bundle",
    "require_verified_bundle",
]
