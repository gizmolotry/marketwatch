from pydantic import BaseModel
from typing import Dict

class EventFeatures(BaseModel):
    event_class_prior: float = 0.5
    private_window_days: float = 0.0
    access_group_count: int = 1
    social_chatter_velocity: float = 0.0
    liquidity_sensitivity: float = 0.5
    time_to_event_days: float = 30.0
    source_reliability: float = 0.8
    historical_abnormality: float = 0.0
    uncertainty_penalty: float = 0.0
    staleness_penalty: float = 0.0

    def to_dict(self) -> Dict[str, float]:
        return {
            "event_class_prior": self.event_class_prior,
            "private_window_days": self.private_window_days,
            "access_group_count": float(self.access_group_count),
            "social_chatter_velocity": self.social_chatter_velocity,
            "liquidity_sensitivity": self.liquidity_sensitivity,
            "time_to_event_days": self.time_to_event_days,
            "source_reliability": self.source_reliability,
            "historical_abnormality": self.historical_abnormality,
            "uncertainty_penalty": self.uncertainty_penalty,
            "staleness_penalty": self.staleness_penalty
        }
