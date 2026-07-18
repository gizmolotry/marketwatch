import time
import datetime
from marketleak.agents.data_agent import IngestionEngine

def main():
    engine = IngestionEngine()
    end_time = time.time() + (8 * 3600)  # 8 hours
    
    print(f"[{datetime.datetime.now()}] Starting 8-hour continuous scrape...")
    while time.time() < end_time:
        try:
            print(f"[{datetime.datetime.now()}] Initiating ingestion cycle...")
            engine.run_all()
            print(f"[{datetime.datetime.now()}] Cycle complete. Sleeping for 15 minutes before next cycle...")
            time.sleep(900)  # Sleep 15 mins between cycles to avoid rate limits
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(60)

    print(f"[{datetime.datetime.now()}] 8-hour continuous scrape completed.")

if __name__ == "__main__":
    main()
