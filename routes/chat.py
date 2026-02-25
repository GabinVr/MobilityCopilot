from fastapi import APIRouter
from langchain_core.messages import HumanMessage
from langchain_core.outputs import Generation
from langchain_core.runnables import RunnableConfig
import uuid
import logging  
from models import ChatRequest, ChatResponse
from core.graph import get_langgraph_app
from fastapi import HTTPException
from cache import get_semantic_cache
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)

langraph = get_langgraph_app()
chat_router = APIRouter()

@chat_router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    try:
        # WARNING: This caching logic does not take into account the audience !
        cached = get_semantic_cache().lookup(request.query, llm_string="copilot") # TODO: Make llm_string dynamic based on environment variable or request parameter for future support of multiple LLMs
        if cached:
            return ChatResponse(answer=cached[0].text, 
                                thread_id=request.thread_id or str(uuid.uuid4()),
                                is_ambiguous=False,
                                contradictor_notes=cached[1].text if len(cached) > 1 else None)
        initial_state = {
            "messages": [HumanMessage(content=request.query)],
            "audience": request.audience,
            "is_ambiguous": False
        }
        thread_id = request.thread_id or str(uuid.uuid4())
        config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
        logger.debug(f"Initial state for LangGraph: {initial_state}")
        final_state = langraph.invoke(initial_state, config=config)
        logger.info(f"Final state from LangGraph: {final_state}")
        
        # Check if the response is ambiguous
        if final_state.get("is_ambiguous"):
            options = final_state.get("clarification_options", [])
            answer = "\n".join(options) if isinstance(options, list) else str(options)
            logger.info(f"Ambiguous response with options: {answer}")
            return ChatResponse(
                answer=answer,
                thread_id=thread_id,
                is_ambiguous=True,
            )
        
        # Extract analytical response - fallback to empty string if not present
        analytical_response = final_state.get("analytical_response") or final_state.get("answer") or "Réponse vide"
        contradictor_notes = final_state.get("contradictor_notes")
        
        logger.info(f"Normal response: {analytical_response[:100]}...")
        get_semantic_cache().update(request.query, 
                              llm_string="copilot", 
                              return_val=[
                                  Generation(text=analytical_response),
                                  Generation(text=contradictor_notes) if contradictor_notes else None
                                          ])
        return ChatResponse(
            answer=analytical_response,
            thread_id=thread_id,
            is_ambiguous=False,
            contradictor_notes=contradictor_notes
        )

    except Exception as e:
        logger.error(f"Error in chat_endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
