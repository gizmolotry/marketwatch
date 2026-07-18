from pydantic import BaseModel, Field
from typing import List

class Entity(BaseModel):
    entity_type: str = Field(..., description="E.g., HANDLE, WALLET, ORGANIZATION, EVENT")
    value: str

class Confidence(BaseModel):
    extractive_confidence: float = Field(..., ge=0.0, le=1.0)
    semantic_confidence: float = Field(..., ge=0.0, le=1.0)

class SourceSpan(BaseModel):
    start_char: int
    end_char: int
    text: str

class Claim(BaseModel):
    claim_uid: str
    claim_type: str
    subject: Entity
    predicate: str
    object: Entity
    source_span: SourceSpan
    confidence: Confidence
    caveats: List[str]

class ExtractionResult(BaseModel):
    schema_version: str = "claim_extraction.v1"
    document_uid: str
    extraction_run_uid: str
    claims: List[Claim]
