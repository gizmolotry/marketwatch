import numpy as np
import time
import uuid
from marketleak.leakrisk.features import EventFeatures
from marketleak.models import LeakRiskPrior

class LeakRiskModel:
    def __init__(self):
        # Weights for the sigmoid function
        self.weights = {
            "event_class_prior": 1.0,
            "private_window_days": 0.1,
            "access_group_count": -0.1,  # Large access groups reduce the likelihood it's a tight 'insider' leak
            "social_chatter_velocity": 0.5,
            "liquidity_sensitivity": 0.3,
            "historical_abnormality": 1.0,
            "time_to_event_days": -0.05, # closer to event = higher risk
            "source_reliability": 0.5,
            "uncertainty_penalty": -1.0,
            "staleness_penalty": -0.5
        }

    def _sigmoid(self, x: float) -> float:
        return 1.0 / (1.0 + np.exp(-x))

    def forecast_risk(self, market_uid: str, event_uid: str, features: EventFeatures) -> LeakRiskPrior:
        f_dict = features.to_dict()
        
        # Calculate raw score
        raw_score = 0.0
        drivers = {}
        
        for feature_name, value in f_dict.items():
            if feature_name in self.weights:
                contribution = value * self.weights[feature_name]
                raw_score += contribution
                drivers[feature_name] = contribution
                
        # Apply sigmoid
        final_score = self._sigmoid(raw_score)
        
        return LeakRiskPrior(
            market_uid=market_uid,
            event_uid=event_uid,
            score=final_score,
            score_version="v1.0",
            feature_snapshot_uid=str(uuid.uuid4()),
            computed_at=time.time(),
            drivers=drivers
        )
