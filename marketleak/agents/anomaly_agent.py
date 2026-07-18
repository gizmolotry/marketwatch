import pandas as pd
from marketleak.scoring import (
    DEFAULT_TICKS_PARQUET,
    detect_activity_v2_from_parquet,
    detect_belief_shocks_from_parquet,
    get_top_anomalies,
    v2_actionable_frame,
)
import os


V2_COLUMNS = [
    "market_uid",
    "market_slug",
    "question",
    "close_time",
    "outcome_uid",
    "timestamp",
    "activity_status",
    "diagnostic_score",
    "p_value",
    "q_value",
    "rank",
    "incident_uid",
    "not_proof_of_fraud",
    "effectiveness_unknown",
]

def _legacy_demo_enabled(explicit: bool | None) -> bool:
    if explicit is not None:
        return explicit
    return os.getenv("MARKETLEAK_LEGACY_DEMO", "").strip().lower() in {"1", "true", "yes", "on"}


def run_anomaly_detection(
    markets_path="demo_data/markets.parquet",
    ticks_path=None,
    *,
    legacy_demo: bool | None = None,
):
    """Return validation-first review candidates.

    The former rolling-window demo is available only through the explicit
    ``legacy_demo`` switch or ``MARKETLEAK_LEGACY_DEMO=1``. Empty output is a
    valid surveillance result and never signals a pipeline failure.
    """

    if not _legacy_demo_enabled(legacy_demo):
        selected_ticks = ticks_path or DEFAULT_TICKS_PARQUET
        try:
            batch = detect_activity_v2_from_parquet(ticks_path=selected_ticks)
            anomalies = v2_actionable_frame(batch, markets_path=markets_path)
        except Exception as exc:
            print(f"[Data Quality] Activity feed unavailable: {exc}")
            return pd.DataFrame(columns=V2_COLUMNS)
        print(f"Detected {len(anomalies)} validation-first activity review candidates.")
        return anomalies

    from marketleak.scoring import resolve_tick_parquet_files
    if ticks_path is None:
        ticks_path = resolve_tick_parquet_files()
    
    if not os.path.exists(markets_path):
        print("Error: Parquet files not found in demo_data.")
        return pd.DataFrame()
        
    # Run the shock detector using DuckDB directly on Parquet files
    # Since our fixture has 1h frequency, window=12 is a 12-hour baseline
    result_df = detect_belief_shocks_from_parquet(
        markets_path=markets_path, 
        ticks_path=ticks_path, 
        window=12, 
        z_threshold=2.5
    )
    
    # Get anomalies
    anomalies = get_top_anomalies(result_df)
    print(f"Detected {len(anomalies)} LEGACY DEMO anomalous market windows.")
    
    if not anomalies.empty:
        top_anomaly = anomalies.iloc[0]
        print("Top Anomaly Details:")
        print(f"Market: {top_anomaly['market_slug']}")
        print(f"Timestamp: {pd.to_datetime(top_anomaly['timestamp'], unit='s')}")
        print(f"Z-Score: {top_anomaly['z_score']:.2f}")
        print(f"Logit Shock Magnitude: {top_anomaly['belief_shock']:.2f}")
        print(f"Current Price: {top_anomaly['price']:.2f}")
        
    return anomalies

if __name__ == "__main__":
    run_anomaly_detection()
