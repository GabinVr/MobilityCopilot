import json
import re

from core.state import CopilotState
from utils.llm_provider import get_llm
from pydantic import BaseModel, Field

class AmbiguityOutput(BaseModel):
    is_ambiguous: bool = Field(description="True if the question is missing crucial info like a date or specific location to run a precise count. False if it's a ranking, a top, a general trend or if you can find an answer in the database.")
    clarification_options: list[str] = Field(description="If ambiguous, 2-3 short options in the user's language to clarify. If not, empty list.", default=[])
    need_external_data: bool = Field(description="True if we need to query the database or weather API. False for greetings or general knowledge.")
    question: str = Field(description="The original user question.")
    language: str = Field(description="The user's language, for consistent responses write th complete word (e.g., 'français', 'english', etc.) not just the code (e.g., 'fr', 'en').")

def ambiguity_node(state: CopilotState) -> CopilotState:
    llm = get_llm()

    business_context = state.get("business_rules", "No business rules found.")
    dataset_descriptions = state.get("table_descriptions", "No dataset descriptions found.")
    messages = state.get("messages")
    
    question = messages[-1].content if messages else ""

    history_text = ""
    for m in messages:
        role = "Utilisateur" if m.type == "human" else "Assistant"
        history_text += f"{role}: {m.content}\n"


    prompt = f"""
    You are the Ambiguity Detector Router for the Montreal Mobility Copilot.
    Your ONLY job is to map the user's question to the correct routing flags.
    You can find informations in the history of the conversation, the last question can be just a precision or a follow-up, you have to consider the whole history to understand the context of the question and detect ambiguity.

    🚨 RULES (CRITICAL) 🚨
    If location is missing assume it's for the whole Montreal city.
    If timeframe is missing, assume it's for all available data.
    Only flag as ambiguous if the lack of this information would lead to a completely different answer or if the question is too vague to even attempt a database query.

    🚨 DOMAINS & DATASETS 🚨
    {business_context}
    {dataset_descriptions}

    🚨 CLASSIFICATION RULES 🚨
    1. RANKINGS/HOTSPOTS (NEVER AMBIGUOUS): Questions asking for "the most", "top", "worst", "axes", "le plus de" are NEVER ambiguous. They imply a full database scan. Set `is_ambiguous=False` and `need_external_data=True`.
    2. MISSING PARAMS (AMBIGUOUS): Questions asking for a specific count ("Combien de...") without a timeframe (year/month) or location. Set `is_ambiguous=True`.
    3. CHAT/BYPASS: Greetings or non-mobility questions. Set `is_ambiguous=False` and `need_external_data=False`.
    4. WEATHER-311 CORRELATIONS: Questions asking for correlations between weather and 311 requests are NEVER ambiguous because we have a specific algorithm for that. Set `is_ambiguous=False` and `need_external_data=True`.
    5. WEATHER PREVISIONS: Questions asking for today's weather or prevision for tomorrow are NOT ambiguous. Set `is_ambiguous=False` and `need_external_data=True`.

    ✅ EXAMPLES (YOU MUST FOLLOW THIS LOGIC) ✅
    
    Question: "Combien de collisions y a-t-il eu ?"
    Thought: Missing timeframe and location for a specific count.
    Output: is_ambiguous=True, need_external_data=False
    
    Question: "Où y a-t-il le plus de nids-de-poule ?"
    Thought: Asks for "le plus de" (Ranking/Hotspot). We scan the whole DB. Not ambiguous.
    Output: is_ambiguous=False, need_external_data=True
    
    Question: "Autour de quels axes STM (arrêts/lignes) observe-t-on le plus de collisions graves ?"
    Thought: Asks for "le plus de" (Ranking/Axes). We will let the SQL agent figure it out. Not ambiguous.
    Output: is_ambiguous=False, need_external_data=True
    
    Question: "Quels sont les signaux faibles aujourd'hui ?"
    Thought: Business rule term (weak signals) + timeframe (today). Not ambiguous.
    Output: is_ambiguous=False, need_external_data=True

    Question: "Quels types de requêtes 311 augmentent quand la température passe sous 0°C ?"
    Thought: Asks for a correlation between weather and 311. We have a specific algorithm for this, but it's not ambiguous because the user is clear about what they want.
    Output: is_ambiguous=False, need_external_data=True

    Question: Quels secteurs ont une hausse de collisions en conditions de pluie/neige
    Thought: Asks for a correlation between weather and collisions. We have a specific algorithm for this, but it's not ambiguous because the user is clear about what they want.
    Output: is_ambiguous=False, need_external_data=True

    USER QUESTION: "{question}"

    CONVERSATION HISTORY:
    {history_text}

    You also have to detect the user's language for consistent responses.

    """

    response = llm.with_structured_output(AmbiguityOutput).invoke(prompt)

    return {
        "is_ambiguous": response.is_ambiguous,
        "clarification_options": response.clarification_options,
        "need_external_data": response.need_external_data,
        "question": question,
        "language": response.language
    }