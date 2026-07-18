import os
import uuid
import json
from google import genai
from google.genai import types
from marketleak.llm.schemas import ExtractionResult

class LLMRouter:
    def __init__(self):
        self.api_key = os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            print("WARNING: GEMINI_API_KEY not set. LLMRouter will fail if called.")
            
        self.client = genai.Client()
        
        # Load prompt
        prompt_path = os.path.join(os.path.dirname(__file__), "prompts", "claim_extraction.md")
        if os.path.exists(prompt_path):
            with open(prompt_path, "r", encoding="utf-8") as f:
                self.system_prompt = f.read()
        else:
            self.system_prompt = "You are a claim extraction bot. Output valid JSON matching the schema."

    def extract_claims(self, text: str, document_uid: str) -> ExtractionResult:
        if not self.api_key:
            return ExtractionResult(
                document_uid=document_uid,
                extraction_run_uid=str(uuid.uuid4()),
                claims=[]
            )
            
        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-pro',
                contents=text,
                config=types.GenerateContentConfig(
                    system_instruction=self.system_prompt,
                    response_mime_type="application/json",
                    response_schema=ExtractionResult,
                    temperature=0.0
                ),
            )
            
            # The response text should be JSON
            data = json.loads(response.text)
            
            # Pydantic validation
            result = ExtractionResult(**data)
            return result
            
        except Exception as e:
            print(f"Error during claim extraction: {e}")
            return ExtractionResult(
                document_uid=document_uid,
                extraction_run_uid=str(uuid.uuid4()),
                claims=[]
            )
