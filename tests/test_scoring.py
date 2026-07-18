import pytest
import pandas as pd
import numpy as np
from marketleak.scoring import compute_logit, detect_belief_shocks, get_top_anomalies

def test_compute_logit():
    assert compute_logit(0.5) == 0.0
    assert compute_logit(0.99) > 0.0
    assert compute_logit(0.01) < 0.0

def test_detect_belief_shocks():
    # Create a stable market that suddenly jumps
    prices = [0.5] * 20
    prices.extend([0.8, 0.82, 0.85]) # Sudden shock
    
    df = pd.DataFrame({
        'timestamp': range(len(prices)),
        'price': prices
    })
    
    result_df = detect_belief_shocks(df, window=10, z_threshold=2.0)
    
    # The jump from 0.5 to 0.8 occurs at index 20
    assert result_df.iloc[20]['is_anomaly'] == True
    assert result_df.iloc[10]['is_anomaly'] == False
    
    anomalies = get_top_anomalies(result_df)
    assert len(anomalies) >= 1
    assert anomalies.iloc[0]['timestamp'] == 20
