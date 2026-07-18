from marketleak.main import PIPELINE_COMPLETION_MESSAGE


def test_pipeline_completion_message_uses_neutral_review_language():
    assert "Investigative review memos" in PIPELINE_COMPLETION_MESSAGE
    assert "Suspicious Activity Report" not in PIPELINE_COMPLETION_MESSAGE
