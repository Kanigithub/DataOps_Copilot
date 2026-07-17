from .llm import run_llm

SUMMARY_SYSTEM = "Summarize into <= 10 bullet points. Keep only key technical decisions, schemas, and requirements."

def summarize(text: str) -> str:
    return run_llm(
        agent="refinement",
        system_prompt=SUMMARY_SYSTEM,
        user_prompt=text[:12000],
    )