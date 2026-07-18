import schedule
import time
import datetime
from marketleak.agents.data_agent import IngestionEngine

def run_ingestion():
    """Trigger the multi-platform data ingestion."""
    print(f"\n[{datetime.datetime.now()}] Waking up. Starting daily data ingestion...")
    try:
        engine = IngestionEngine()
        engine.run_all()
        print(f"[{datetime.datetime.now()}] Ingestion complete. Going back to sleep.")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error during ingestion: {e}")

def main():
    print("=========================================")
    print("MarketLeak Autonomous Daemon Initialized")
    print("=========================================")
    print("Scheduling ingestion job to run every day at 00:00 UTC...")
    
    # Run once immediately on startup
    run_ingestion()
    
    # Schedule to run every day
    schedule.every().day.at("00:00").do(run_ingestion)
    
    try:
        while True:
            # Check if any scheduled tasks need to run
            schedule.run_pending()
            # Sleep to prevent high CPU usage
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nDaemon terminated by user.")

if __name__ == "__main__":
    main()
