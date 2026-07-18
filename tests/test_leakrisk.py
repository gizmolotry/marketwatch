import pytest
from marketleak.leakrisk.features import EventFeatures
from marketleak.leakrisk.model import LeakRiskModel

def test_leak_risk_forecast():
    model = LeakRiskModel()
    
    # Baseline features
    features = EventFeatures()
    prior = model.forecast_risk("market_123", "event_456", features)
    
    assert prior.market_uid == "market_123"
    assert prior.event_uid == "event_456"
    assert 0.0 <= prior.score <= 1.0
    
def test_leak_risk_sensitivity():
    model = LeakRiskModel()
    
    # High risk event
    high_risk = EventFeatures(
        social_chatter_velocity=10.0,
        access_group_count=5,
        time_to_event_days=1.0, # very close
        historical_abnormality=2.0
    )
    
    # Low risk event
    low_risk = EventFeatures(
        social_chatter_velocity=0.0,
        access_group_count=1,
        time_to_event_days=100.0, # far away
        historical_abnormality=0.0
    )
    
    high_score = model.forecast_risk("market_high", "event_high", high_risk).score
    low_score = model.forecast_risk("market_low", "event_low", low_risk).score
    
    assert high_score > low_score
