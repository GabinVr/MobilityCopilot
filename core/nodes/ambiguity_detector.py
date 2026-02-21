import json
import re

from core.state import CopilotState
from utils.llm_provider import get_llm
from pydantic import BaseModel

class AmbiguityOutput(BaseModel):
    is_ambiguous: bool
    clarification_options: list[str]


def _parse_ambiguity_output(raw_response) -> AmbiguityOutput:
    if isinstance(raw_response, AmbiguityOutput):
        return raw_response

    content = getattr(raw_response, "content", raw_response)
    text = content if isinstance(content, str) else str(content)
    text = text.strip()

    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        text = fenced.group(1).strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("Ambiguity detector output is not valid JSON")
        data = json.loads(text[start:end + 1])

    return AmbiguityOutput.model_validate(data)

def ambiguity_node(state: CopilotState) -> CopilotState:
    llm = get_llm()

    rag_context = state.get("retrieved_context")
    messages = state.get("messages")

    if messages:
        question = messages[-1].content
    else:        question = ""

    prompt = f"""
    You are the Ambiguity Detector for the Montreal Mobility Copilot.
    Your task is to verify if the user's question is precise enough to be translated into a SQL query.

    OFFICIAL GLOSSARY & SCHEMAS (RAG):
    {rag_context}

    USER QUESTION:
    {question}

    HISTORY OF CONVERSATION:
    {messages}

    INSTRUCTIONS:
    1. Analyze the user's question in the context of the provided glossary and conversation history.
    2. Check if the question lacks a specific location, time period, incident type or any other detail that would make it hard to generate a precise query.
    3. IMPORTANT: If the user is responding to a previous clarification or providing a missing detail, use the history to resolve the ambiguity instead of marking it as ambiguous again.
    4. If the question is "fuzzy" (e.g., "where are the problems?"), set is_ambiguous to True.
    5. If ambiguous, use the GLOSSARY to propose 2-3 hypotheses (e.g., "Do you mean 311 pothole requests or severe collisions?").
    6. If the question is clear and matches categories in the glossary, set is_ambiguous to False.

    OUTPUT FORMAT (JSON ONLY):
    {{
      "is_ambiguous": boolean,
      "clarification_options": ["option 1", "option 2"]
    }}
    """

    try:
        response = llm.with_structured_output(AmbiguityOutput).invoke(prompt)
    except Exception:
        response = llm.invoke(prompt)

    parsed = _parse_ambiguity_output(response)

    return {
        "is_ambiguous": parsed.is_ambiguous,
        "clarification_options": parsed.clarification_options
    }


