from pathlib import Path
from unittest.mock import MagicMock

from marketleak.agents.synthesis_agent import SynthesisAgent, verify_truth_safe_memo


ROOT = Path(__file__).resolve().parents[2]


def test_streamlit_source_has_default_off_truth_firewall_and_non_exonerating_language():
    source = (ROOT / "marketleak" / "app.py").read_text(encoding="utf-8")
    inference_source = (ROOT / "ui" / "src" / "pages" / "Inference.jsx").read_text(
        encoding="utf-8"
    )

    assert "LEGACY / UNVALIDATED DEMO" in source
    assert "effectiveness unknown" in source
    assert "not proof of fraud" in source
    assert "MARKETLEAK_ENABLE_LEGACY_REPORTS" in source
    assert 'strip() != "1"' in source
    assert "human-review " in source and "acknowledgment checkbox" in source
    assert "Legacy Investigative Memo Archive — Not Validation Evidence" in source
    assert "Archived reports are legacy generated artifacts" in source
    assert "Legacy PPIM heuristic" in source
    assert "Deprecated Uncalibrated Legacy Leak-Risk Heuristic" in source
    assert "Contextual Wallet Graph (Unvalidated)" in source
    assert "Trading appears normal" not in source
    assert "Web of Insiders" not in source
    assert "does not establish " in source and "normal trading, absence of misconduct" in source
    assert "read_report_for_product" in source
    assert "st.markdown(content)" not in source
    assert "result.report_artifact?.truth_firewall_verified" in inference_source
    assert "result.report_artifact?.content_available" in inference_source
    assert "Generated artifact withheld" in inference_source


def _agent(tmp_path, response_text):
    agent = object.__new__(SynthesisAgent)
    agent.model_name = "test-model"
    agent.reports_dir = str(tmp_path)
    response = MagicMock()
    response.text = response_text
    agent.client = MagicMock()
    agent.client.models.generate_content.return_value = response
    return agent


def _payload():
    return {
        "market_slug": "acme-market",
        "question": "Will the Acme transaction close?",
        "shock_magnitude": 0.4,
        "lead_time_hours": 2.0,
        "ppim_score": 0.8,
        "evidence_date": "2026-07-01T12:00:00Z",
        "evidence_url": "https://example.test/source",
        "evidence_text": "The transaction closed.",
    }


def test_synthesis_prompt_prohibits_conclusions_and_requires_evidence_mapping(tmp_path):
    agent = _agent(
        tmp_path,
        "## Observed Supplied Fields\n\nA market question and timestamp context were supplied.",
    )

    report, report_path = agent.generate_sar(_payload())
    prompt = agent.client.models.generate_content.call_args.kwargs["contents"]
    verified = verify_truth_safe_memo(report)

    assert "INVESTIGATIVE REVIEW MEMO" in prompt
    assert "NEVER conclude or imply that fraud, MNPI use, insider trading, or information leakage occurred" in prompt
    assert "NEVER conclude or imply that the market, wallet, person, or organization is cleared" in prompt
    assert "Every factual claim must map only to a supplied XML field" in prompt
    assert "Data and Evidence Limitations" in prompt
    assert "deprecated_uncalibrated_legacy_ppim" in prompt
    assert "not_proof_of_fraud: true" in prompt
    assert "Human Review Questions" in prompt
    assert report.startswith("<!-- MARKETLEAK_ARTIFACT_METADATA_V2\n")
    assert verified is not None
    metadata, body = verified
    assert body.startswith("# Investigative Review Memo (V2 / Unvalidated)")
    assert "`not_proof_of_fraud: true`" in body
    assert "Effectiveness is unknown" in body
    assert metadata.not_proof_of_fraud is True
    assert metadata.effectiveness_unknown is True
    assert metadata.human_review_required is True
    assert Path(report_path).exists()
    assert Path(report_path).read_text(encoding="utf-8") == report
    assert report_path.endswith("_MEMO.md")


def test_synthesis_output_firewall_withholds_guilt_or_exoneration_conclusion(tmp_path):
    agent = _agent(tmp_path, "This market is cleared of all suspicion and trading appears normal.")

    report, _ = agent.generate_sar(_payload())
    verified = verify_truth_safe_memo(report)

    assert verified is not None
    _, body = verified
    assert "Draft Withheld by Truth Firewall" in body
    assert "cleared of all suspicion" not in body
    assert "trading appears normal" not in body
    assert "Human review is required" in body
