from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any

class AnomalyCandidate(BaseModel):
    alert_uid: str
    market_uid: str
    market_slug: str
    question: str
    shock_timestamp: float
    price: float
    logit_belief: float
    belief_shock: float
    rolling_mean: float
    rolling_std: float
    z_score: float
    z_threshold: float

class LeakRiskPrior(BaseModel):
    market_uid: str
    event_uid: Optional[str]
    score: float
    score_version: str
    feature_snapshot_uid: str
    computed_at: float
    drivers: Dict[str, float]

class GraphEnrichment(BaseModel):
    alert_uid: str
    candidate_paths: List[Any]
    max_path_score: float
    path_count: int
    highest_evidence_band: str
    counter_evidence_count: int
    stale_edge_count: int

class AlertPacket(BaseModel):
    alert_uid: str
    anomaly_candidate: AnomalyCandidate
    leak_risk_prior: Optional[LeakRiskPrior] = None
    graph_enrichment: Optional[GraphEnrichment] = None
    rag_result: Optional[Dict[str, Any]] = None
    queue_priority: float = 0.0
    narrative_inputs: Dict[str, Any] = Field(default_factory=dict)
