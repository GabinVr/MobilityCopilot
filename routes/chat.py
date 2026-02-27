from fastapi import APIRouter, Request
from langchain_core.messages import HumanMessage
from langchain_core.outputs import Generation
from langchain_core.runnables import RunnableConfig
import uuid
import logging  
import asyncio
import inspect
from models import ChatRequest, ChatResponse
from fastapi import HTTPException
from cache import get_semantic_cache
from utils.llm_provider import get_llm_name
import os
from dotenv import load_dotenv
from typing import List, Optional

load_dotenv()

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)

chat_router = APIRouter()

async def _cache_response_background(query: str, 
                                     analytical_response: str, 
                                     contradictor_notes: str, 
                                     llm_string: str,
                                     clarification_options: Optional[List[str]] = None,
                                     ):
    """Background task to cache the response without blocking the API response."""
    try:
        await asyncio.sleep(0)  # Yield control to allow immediate response return
        get_semantic_cache().update(query, 
                              llm_string=llm_string, 
                              return_val=[
                                  Generation(text=analytical_response),
                                  Generation(text=contradictor_notes) if contradictor_notes else None,
                                  Generation(text=clarification_options) if clarification_options else None
                              ])
        logger.debug(f"✅ Response cached in background for query: {query[:50]}...")
    except Exception as cache_error:
        logger.warning(f"⚠️ Background cache update failed: {cache_error}. Response not cached.")

@chat_router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest, fastapi_request: Request):
    try:
        logger.info(f"[BACKEND DEBUG] 🔴 thread_id reçu du BFF: {request.thread_id or 'AUCUN (None)'}")
        # WARNING: This caching logic does not take into account the audience !
        cached = None
        try:
            cached = get_semantic_cache().lookup(request.query, llm_string=get_llm_name()) 
        except Exception as cache_error:
            logger.warning(f"⚠️ Cache lookup failed: {cache_error}. Proceeding without cache.")
            # Continue without cache - don't return an error
        
        if cached:
            return ChatResponse(answer=cached[0].text, 
                                thread_id=request.thread_id or str(uuid.uuid4()),
                                is_ambiguous=False,
                                contradictor_notes=cached[1].text if len(cached) > 1 else None)

        thread_id = request.thread_id or str(uuid.uuid4())
        logger.info(f"[BACKEND DEBUG] 🔴 thread_id utilisé pour cette requête: {thread_id} ({'existant' if request.thread_id else 'NOUVEAU généré'})")
        
        config: RunnableConfig = {
            "configurable": {"thread_id": thread_id, "audience": request.audience}
        }

        initial_state = {
            "messages": [HumanMessage(content=request.query)],
            # "audience": request.audience,
            "is_ambiguous": False
        }
        logger.debug(f"Thread: {thread_id}, Audience: {request.audience}")
        logger.debug(f"Initial state for LangGraph: {initial_state}")
        graph = fastapi_request.app.state.graph
        if hasattr(graph, "ainvoke") and inspect.iscoroutinefunction(graph.ainvoke):
            logger.debug("Using async invoke for LangGraph")
            result = graph.ainvoke(initial_state, config=config)
        elif hasattr(graph, "invoke"):
            logger.debug("Using sync invoke for LangGraph")
            result = graph.invoke(initial_state, config=config)
        elif hasattr(graph, "ainvoke"):
            logger.debug("Using async invoke for LangGraph (fallback)")
            result = graph.ainvoke(initial_state, config=config)
        else:
            raise RuntimeError("Graph has no invoke method")

        final_state = await result if asyncio.iscoroutine(result) else result
        logger.info(f"Final state from LangGraph: {final_state}")

        if hasattr(graph, "aget_state_history"):
            try:
                history = [item async for item in graph.aget_state_history(config)]
                logger.debug(f"Thread: {thread_id}, Full history: {history}")
            except Exception as history_error:
                logger.debug(f"State history unavailable: {history_error}")
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
        
        # Cache the result in background (non-blocking) - this doesn't delay the API response
        # Only cache if it is not ambiguous or an error or if the message is long enough (to avoid caching very short/empty responses)
        if not final_state.get("is_ambiguous") and analytical_response and len(analytical_response) > 20:
            asyncio.create_task(_cache_response_background(
                request.query, 
                analytical_response, 
                contradictor_notes or "",
                get_llm_name(),
                clarification_options=final_state.get("clarification_options")
            ))
            logger.debug(f"Background caching task created for thread: {thread_id}")
            
        logger.info(f"[BACKEND DEBUG] 🔴 thread_id retourné dans la réponse: {thread_id}")
        return ChatResponse(
            answer=analytical_response,
            thread_id=thread_id,
            is_ambiguous=False,
            contradictor_notes=contradictor_notes
        )

    except Exception as e:
        logger.error(f"Error in chat_endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
