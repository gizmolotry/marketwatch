import time
import datetime
import os
import pandas as pd
import sys
from dotenv import load_dotenv

# Ensure the parent directory is in the python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from marketleak.agents.data_agent import IngestionEngine
from marketleak.agents.anomaly_agent import run_anomaly_detection
from marketleak.graph.repository import GraphRepository
from marketleak.graph.clustering import WalletClustering
from marketleak.graph.safe_persistence import save_cluster_map

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "demo_data")
ANOMALY_CACHE = os.path.join(CACHE_DIR, "cache_anomalies.parquet")
CLUSTER_CACHE = os.path.join(CACHE_DIR, "cache_clusters.json")

def main():
    print("=========================================")
    print("MarketWatch Master Daemon Initialized")
    print("=========================================")
    
    engine = IngestionEngine()
    
    while True:
        print(f"\n[{datetime.datetime.now()}] Initiating master cycle...")
        try:
            # 1. Compute Anomalies and Cache
            print(f"[{datetime.datetime.now()}] Computing belief shocks...")
            anomalies_df = run_anomaly_detection()
            if anomalies_df is not None:
                anomalies_df.to_parquet(ANOMALY_CACHE)
                print(f"[{datetime.datetime.now()}] Saved anomalies to {ANOMALY_CACHE}")
                
            # 2. Compute Proxy Clusters and Cache
            print(f"[{datetime.datetime.now()}] Computing proxy clusters...")
            repo = GraphRepository()
            if not repo.persisted_graph_available:
                print(
                    f"[{datetime.datetime.now()}] Proxy clustering unavailable: "
                    f"{repo.load_error}. Existing cluster cache was not replaced."
                )
            else:
                cluster_engine = WalletClustering()
                proxies = cluster_engine.cluster_proxy_wallets(repo)
                save_cluster_map(proxies, CLUSTER_CACHE)
                print(f"[{datetime.datetime.now()}] Saved clusters to {CLUSTER_CACHE}")
            
            # 3. Scrape new data
            print(f"[{datetime.datetime.now()}] Skipping ingestion to preserve demo cache...")
            # engine.run_all()
            
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Error during cycle: {e}")
            
        print(f"[{datetime.datetime.now()}] Cycle complete. Sleeping for 5 minutes...")
        time.sleep(300)

if __name__ == "__main__":
    main()
