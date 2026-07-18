import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_fixture():
    print("Generating synthetic market fixture data...")
    
    # Base configuration
    market_id = "test_market_001"
    market_slug = "will-company-x-announce-acquisition-by-july-4"
    question = "Will Company X announce an acquisition by July 4, 2026?"
    resolution_source = "https://www.companyx.com/press"
    
    # Time settings
    end_date = datetime(2026, 7, 4, 12, 0) # July 4 noon
    start_date = end_date - timedelta(days=5) # 5 days of history
    
    # News drops at T-minus 24 hours (July 3, 12:00)
    public_news_time = end_date - timedelta(days=1)
    
    # Insider leak happens 8 hours BEFORE public news (July 3, 04:00)
    leak_time = public_news_time - timedelta(hours=8)
    
    timestamps = pd.date_range(start=start_date, end=end_date, freq='1h')
    
    data = []
    current_price = 0.25 # Starts at 25%
    
    for t in timestamps:
        # Normal volatility
        current_price += np.random.normal(0, 0.01)
        
        # Insider Leak (Massive shock 8 hours before news)
        if t >= leak_time and t < public_news_time:
            # Price starts surging from 0.25 to 0.65
            current_price += np.random.normal(0.05, 0.01)
            
        # Public News Drop (Jumps the rest of the way to 0.95)
        if t >= public_news_time:
            current_price += np.random.normal(0.08, 0.01)
            
        # Clip to valid probability bounds [0.01, 0.99]
        current_price = max(0.01, min(0.99, current_price))
        
        data.append({
            'market_id': market_id,
            'market_slug': market_slug,
            'question': question,
            'resolution_source': resolution_source,
            'close_time': end_date.isoformat() + "Z",
            'timestamp': int(t.timestamp()), # Unix timestamp
            'price': current_price
        })

    df = pd.DataFrame(data)
    os.makedirs("demo_data", exist_ok=True)
    df.to_csv("demo_data/polymarket_dump.csv", index=False)
    print(f"Generated fixture: demo_data/polymarket_dump.csv with {len(df)} records.")
    
    # Generate the source corpus to match
    import json
    sources = [
        {
            "market_id": market_id,
            "url": "https://www.reuters.com/company-x-acquisition",
            "published_at": int(public_news_time.timestamp()),
            "content": "Company X officially announced it is being acquired today."
        }
    ]
    with open("demo_data/sample_sources.json", "w") as f:
        json.dump(sources, f, indent=4)
    print("Generated fixture: demo_data/sample_sources.json")

if __name__ == "__main__":
    np.random.seed(42)
    generate_fixture()
