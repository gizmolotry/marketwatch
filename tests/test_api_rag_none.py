import pandas as pd
from datetime import UTC, datetime

import marketleak.api as api
from marketleak.agents.synthesis_agent import build_truth_safe_memo
from marketleak.graph.repository import GraphRepository
from marketleak.models import LeakRiskPrior


class NoEvidenceRAG:
    def fetch_and_score(self, _market_payload):
        return None


class ReviewableRAG:
    def fetch_and_score(self, _market_payload):
        return {
            "evidence_text": "A supplied context field.",
            "evidence_url": "https://example.test/context",
            "evidence_date": "2026-01-01T00:00:00Z",
            "lead_time_hours": 2.0,
            "ppim_score": 2.0,
        }


class OfflineLeakModel:
    def forecast_risk(self, market_uid, event_uid, _features):
        return LeakRiskPrior(
            market_uid=market_uid,
            event_uid=event_uid,
            score=0.0,
            score_version="test",
            feature_snapshot_uid="test-snapshot",
            computed_at=0.0,
            drivers={},
        )


class OfflineBlockchainAgent:
    def enrich_graph_for_anomaly(self, *_args, **_kwargs):
        raise OSError("offline in unit test")


def test_pipeline_converts_none_rag_result_to_neutral_contract(monkeypatch, tmp_path):
    monkeypatch.setattr(api, "_get_rag_agent", lambda: NoEvidenceRAG())
    monkeypatch.setattr(api, "_get_leak_model", lambda: OfflineLeakModel())
    monkeypatch.setattr(api, "BlockchainAgent", OfflineBlockchainAgent)
    monkeypatch.setattr(api, "GraphRepository", lambda: GraphRepository(tmp_path / "missing-graph.json"))

    row = pd.Series(
        {
            "market_uid": "market-1",
            "market_slug": "example-market",
            "question": "Will the example event happen?",
            "timestamp": 1_704_067_200.0,
            "price": 0.75,
            "logit_belief": 1.0,
            "belief_shock": 2.0,
            "rolling_mean": 0.5,
            "rolling_std": 0.1,
            "z_score": 3.0,
        }
    )

    result = api._run_pipeline_for_row(row)

    rag_result = result["rag_result"]
    assert rag_result["evidence_text"] is None
    assert rag_result["evidence_url"] is None
    assert rag_result["evidence_date"] is None
    assert rag_result["evidence_scope"] == "unavailable"
    assert rag_result["search_strategy"] == "unavailable"
    assert rag_result["timing_valid"] is False
    assert rag_result["ppim_suppression_reason"] == "evidence_unavailable"
    assert rag_result["ppim_score"] == 0.0
    assert rag_result["lead_time_hours"] == 0.0
    assert rag_result["evidence_error"] is None
    assert rag_result["market_slug"] == "example-market"
    assert rag_result["question"] == "Will the example event happen?"
    assert result["ppim_score"] == 0.0
    assert result["report"] is None
    assert result["graph_availability"]["status"] == "unavailable"
    assert result["graph_availability"]["empty_graph_observed"] is None
    assert result["graph_availability"]["absence_claim_eligible"] is False
    assert result["graph_enrichment_status"] == "unavailable_persisted_graph"
    assert result["graph_enrichment"] is None
    assert result["graph_data"]["status"] == "unavailable"


def _row():
    return pd.Series(
        {
            "market_uid": "market-1",
            "market_slug": "example-market",
            "question": "Will the example event happen?",
            "timestamp": 1_704_067_200.0,
            "price": 0.75,
            "logit_belief": 1.0,
            "belief_shock": 2.0,
            "rolling_mean": 0.5,
            "rolling_std": 0.1,
            "z_score": 3.0,
        }
    )


def _enable_synthesis(monkeypatch):
    monkeypatch.setenv("MARKETLEAK_ENABLE_LEGACY_SAR", "1")
    monkeypatch.setattr(api, "_get_rag_agent", lambda: ReviewableRAG())
    monkeypatch.setattr(api, "_get_leak_model", lambda: OfflineLeakModel())
    monkeypatch.setattr(api, "BlockchainAgent", OfflineBlockchainAgent)


def test_live_api_withholds_unmarked_malicious_synthesis(monkeypatch, tmp_path):
    class MaliciousSynthesis:
        def generate_sar(self, _packet):
            return "Individuals likely traded on MNPI.", str(tmp_path / "unsafe.md")

    _enable_synthesis(monkeypatch)
    monkeypatch.setattr(api, "SynthesisAgent", MaliciousSynthesis)

    result = api._run_pipeline_for_row(_row())

    assert result["report"] is None
    assert result["report_path"] is None
    assert result["report_artifact"]["truth_firewall_verified"] is False
    assert result["report_artifact"]["content_available"] is False
    assert "likely traded" not in str(result)


def test_live_api_returns_only_verified_memo_body_and_metadata(monkeypatch, tmp_path):
    body = (
        "# Investigative Review Memo (V2 / Unvalidated)\n\n"
        "## Observed fields\n\nA supplied timestamp is retained for human review.\n"
    )
    envelope = build_truth_safe_memo(
        body,
        source_uid="marketleak:assessment/test-live-api",
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    class SafeSynthesis:
        def generate_sar(self, _packet):
            return envelope, str(tmp_path / "verified_MEMO.md")

    _enable_synthesis(monkeypatch)
    monkeypatch.setattr(api, "SynthesisAgent", SafeSynthesis)

    result = api._run_pipeline_for_row(_row())

    assert result["report"] == body
    assert "MARKETLEAK_ARTIFACT_METADATA_V2" not in result["report"]
    assert result["report_artifact"]["truth_firewall_verified"] is True
    assert result["report_artifact"]["content_available"] is True
    assert result["report_artifact"]["artifact_schema_version"] == "marketleak.investigative-memo/v2"
