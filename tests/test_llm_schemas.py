import pytest
from pydantic import ValidationError
from marketleak.llm.schemas import ExtractionResult, Claim, Entity, SourceSpan, Confidence

def test_valid_extraction_result():
    data = {
        "document_uid": "doc_123",
        "extraction_run_uid": "run_456",
        "claims": [
            {
                "claim_uid": "claim_789",
                "claim_type": "ALIAS_LINK",
                "subject": {"entity_type": "HANDLE", "value": "trader_bob"},
                "predicate": "CLAIMS_CONTROL_OF",
                "object": {"entity_type": "WALLET", "value": "0xABC123"},
                "source_span": {"start_char": 10, "end_char": 50, "text": "I own 0xABC123"},
                "confidence": {"extractive_confidence": 0.9, "semantic_confidence": 0.8},
                "caveats": ["Self-reported"]
            }
        ]
    }
    
    result = ExtractionResult(**data)
    assert result.document_uid == "doc_123"
    assert len(result.claims) == 1
    assert result.claims[0].subject.value == "trader_bob"

def test_invalid_confidence():
    # Confidence > 1.0 should fail
    data = {
        "document_uid": "doc_123",
        "extraction_run_uid": "run_456",
        "claims": [
            {
                "claim_uid": "claim_789",
                "claim_type": "ALIAS_LINK",
                "subject": {"entity_type": "HANDLE", "value": "trader_bob"},
                "predicate": "CLAIMS_CONTROL_OF",
                "object": {"entity_type": "WALLET", "value": "0xABC123"},
                "source_span": {"start_char": 10, "end_char": 50, "text": "I own 0xABC123"},
                "confidence": {"extractive_confidence": 1.5, "semantic_confidence": 0.8},
                "caveats": ["Self-reported"]
            }
        ]
    }
    
    with pytest.raises(ValidationError):
        ExtractionResult(**data)
