from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from core.graph import app as langgraph_app
from langchain_core.messages import HumanMessage

api = FastAPI(title="MobilityCopilot API")

class ChatRequest(BaseModel):
    query: str
    audience: str = "grand_public" # 'grand_public' or 'municipalite'
    
class ChatResponse(BaseModel):
    answer: str
    is_ambiguous: bool
    contradictor_notes: str | None = None

@api.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    try:
        initial_state = {
            "messages": [HumanMessage(content=request.query)],
            "audience": request.audience,
            "is_ambiguous": False
        }
        
        final_state = langgraph_app.invoke(initial_state)
        
        if final_state.get("is_ambiguous"):
            return ChatResponse(
                answer=final_state.get("clarification_options", "Pouvez-vous clarifier?"),
                is_ambiguous=True
            )
            
        return ChatResponse(
            answer=final_state.get("analytical_response", "Erreur de génération."),
            is_ambiguous=False,
            contradictor_notes=final_state.get("contradictor_notes")
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
