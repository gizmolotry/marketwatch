import os
import json
import uuid
import pandas as pd
from google import genai
from google.genai import types
from dotenv import load_dotenv

from marketleak.leakrisk.features import EventFeatures
from marketleak.leakrisk.model import LeakRiskModel
from marketleak.models import LeakRiskPrior

load_dotenv()

class PredictAgent:
    def __init__(self):
        print("Initializing Predict Agent...")
        self.model_name = "gemini-2.5-flash"
        try:
            self.client = genai.Client()
        except ValueError:
            raise ValueError("GEMINI_API_KEY environment variable not found. Cannot run PredictAgent.")
        self.risk_model = LeakRiskModel()

    def _extract_features(self, question: str) -> EventFeatures:
        prompt = f"""
        You are a quantitative risk analyst for prediction markets.
        Analyze this prediction market question and estimate the features for "information leak risk".
        
        Question: "{question}"
        
        Output JSON matching this exact format, with reasonable float/int estimates:
        {{
            "event_class_prior": 0.5, // Base risk of this event type leaking (0.0 to 1.0)
            "private_window_days": 5.0, // How many days insiders know the outcome before the public
            "access_group_count": 10, // Roughly how many people know the secret?
            "social_chatter_velocity": 0.2, // Current rumor velocity (0.0 to 1.0)
            "liquidity_sensitivity": 0.5, // How sensitive is this market to whales? (0.0 to 1.0)
            "time_to_event_days": 10.0, // Estimated days until resolution
            "source_reliability": 0.8, // How reliable is the resolution source? (0.0 to 1.0)
            "historical_abnormality": 0.0, // Have past similar markets leaked? (0.0 to 1.0)
            "uncertainty_penalty": 0.1, // Penalty if outcome is highly random (0.0 to 1.0)
            "staleness_penalty": 0.0 // Penalty if market is old (0.0 to 1.0)
        }}
        """
        
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    response_mime_type="application/json"
                )
            )
            data = json.loads(response.text)
            return EventFeatures(**data)
        except Exception as e:
            print(f"Failed to extract features using LLM: {e}")
            # Fallback to default features
            return EventFeatures()

    def run_pipeline(self, markets_path="demo_data/markets.parquet", limit: int = 5):
        if not os.path.exists(markets_path):
            print(f"Error: {markets_path} not found.")
            return []
            
        df = pd.read_parquet(markets_path)
        if limit is not None:
            df = df.head(limit)
            
        results = []
        for _, row in df.iterrows():
            market_uid = str(row.get("market_uid", "unknown"))
            question = str(row.get("question", "unknown question"))
            print(f"\n[PREDICT] Analyzing market: {question[:80]}...")
            
            features = self._extract_features(question)
            prior = self.risk_model.forecast_risk(
                market_uid=market_uid,
                event_uid=str(uuid.uuid4()),
                features=features
            )
            
            print(f"  -> Leak Risk Score: {prior.score:.3f}")
            print(f"  -> Top Drivers: {list(prior.drivers.keys())[:3]}")
            results.append(prior)
            
        return results

if __name__ == "__main__":
    agent = PredictAgent()
    agent.run_pipeline()
