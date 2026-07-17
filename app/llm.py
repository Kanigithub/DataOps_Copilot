import os
from dotenv import load_dotenv
import openai
from openai import AzureOpenAI
from typing import Optional
import time

load_dotenv()

client = AzureOpenAI(
    api_key=os.getenv("DIAL_API_KEY", ""),
    api_version=os.getenv("DIAL_API_VERSION", "2025-04-01-preview"),
    azure_endpoint=os.getenv("DIAL_ENDPOINT", "https://ai-proxy.lab.epam.com"),
    timeout=120.0,          # ✅ increase timeout (seconds)
    max_retries=2,          # ✅ retry transient failures
)

DEPLOYMENT = os.getenv("DIAL_DEPLOYMENT", "gpt-5-mini-2025-08-07")

if not os.getenv("DIAL_API_KEY"):
    raise RuntimeError("Missing DIAL_API_KEY in .env")

def run_llm(agent: str, system_prompt: str, user_prompt: str) -> str:
    # IMPORTANT: do NOT pass temperature for this model (only default=1 supported)
    last_err = None
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=DEPLOYMENT,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            return resp.choices[0].message.content
        except Exception as e:
            last_err = e
            # backoff
            time.sleep(2 * (attempt + 1))
    raise last_err