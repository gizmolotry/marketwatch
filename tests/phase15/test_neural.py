from __future__ import annotations

import json

import pytest

import marketleak.multimodal.neural as neural


def spec() -> neural.EventSpaceSpec:
    return neural.EventSpaceSpec(
        market_input_dim=3,
        evidence_input_dim=4,
        onchain_input_dim=2,
        private_dim=5,
        shared_dim=3,
        fusion_dim=7,
    )


def test_neural_status_and_serializable_architecture_manifest():
    status = neural.neural_status()
    assert status.experimental_only is True
    assert status.serves_without_runtime is False
    manifest = neural.architecture_manifest(spec())
    assert manifest.spec_hash == manifest.spec.spec_hash
    assert manifest.weights_loaded is False
    assert json.loads(json.dumps(manifest.model_dump(mode="json")))["spec_hash"] == manifest.spec_hash


if not neural.TORCH_AVAILABLE:

    def test_absent_torch_is_explicitly_unavailable():
        assert neural.neural_status().available is False
        with pytest.raises(neural.NeuralUnavailableError):
            neural.ExperimentalEventSpace(spec())

else:
    import torch

    def inputs():
        return dict(
            market=torch.tensor([[0.1, 0.2, 0.3], [0.0, 0.0, 0.0]], dtype=torch.float32),
            evidence=None,
            onchain=torch.tensor([[1.0, 2.0], [3.0, 4.0]], dtype=torch.float32),
            market_mask=torch.tensor([True, False]),
            evidence_mask=torch.tensor([False, False]),
            onchain_mask=torch.tensor([True, True]),
        )

    def test_masked_modalities_validate_and_output_governed_shapes():
        torch.manual_seed(7)
        model = neural.ExperimentalEventSpace(spec())
        output = model(**inputs())

        assert tuple(output.mechanism_logits.shape) == (2, len(neural.MECHANISM_LABELS))
        assert tuple(output.evidence_logits.shape) == (2, len(neural.EVIDENCE_LABELS))
        assert tuple(output.fused_embedding.shape) == (2, 7)
        assert output.modality_mask.tolist() == [[True, False, True], [False, False, True]]
        with pytest.raises(neural.ModalityInputError, match="required"):
            model(
                market=None,
                evidence=None,
                onchain=None,
                market_mask=torch.tensor([True]),
                evidence_mask=torch.tensor([False]),
                onchain_mask=torch.tensor([False]),
            )
        with pytest.raises(neural.ModalityInputError, match="shape"):
            model(
                market=torch.ones((2, 2)),
                evidence=None,
                onchain=None,
                market_mask=torch.tensor([True, False]),
                evidence_mask=torch.tensor([False, False]),
                onchain_mask=torch.tensor([False, False]),
            )

    def test_heads_are_limited_to_fixed_observable_and_evidence_ontologies():
        model = neural.ExperimentalEventSpace(spec())

        head_names = {name for name in model._modules if name.endswith("_head")}

        assert head_names == {"mechanism_head", "evidence_head"}
        assert model.spec.mechanism_labels == neural.MECHANISM_LABELS
        assert model.spec.evidence_labels == neural.EVIDENCE_LABELS
        assert model.mechanism_head.out_features == len(neural.MECHANISM_LABELS)
        assert model.evidence_head.out_features == len(neural.EVIDENCE_LABELS)

    def test_eval_mode_is_deterministic_and_protected_execution_cannot_be_unlocked_by_a_boolean():
        torch.manual_seed(17)
        model = neural.ExperimentalEventSpace(spec()).eval()
        batch = inputs()
        with torch.no_grad():
            first = model(**batch)
            second = model(**batch)
        torch.testing.assert_close(first.mechanism_logits, second.mechanism_logits, rtol=0.0, atol=0.0)
        torch.testing.assert_close(first.evidence_logits, second.evidence_logits, rtol=0.0, atol=0.0)
        with pytest.raises(neural.BundleApprovalRequired):
            model(**batch, production=True)
        with pytest.warns(DeprecationWarning, match="cannot unlock"):
            with pytest.raises(neural.BundleApprovalRequired, match="no verified approval object"):
                model(**batch, production=True, approved_bundle=True)
        with pytest.raises(neural.BundleApprovalRequired, match="no verified approval object"):
            model(**batch, production=True, verified_bundle=object())
