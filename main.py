from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from core.graph import app as langgraph_app
from langchain_core.messages import HumanMessage
from data.dashboard_queries import (
    WordCloudQuery311,
    CollisionHeatMapQuery,
    WeatherCorrelationQuery
)

api = FastAPI(title="MobilityCopilot API")

class ChatRequest(BaseModel):
    query: str
    audience: str = "grand_public" # 'grand_public' or 'municipalite'
    
class ChatResponse(BaseModel):
    answer: str
    is_ambiguous: bool
    contradictor_notes: str | None = None

# Dashboard query models
class WordCloudRequest(BaseModel):
    top_n: int = Field(default=10, ge=1, le=100)
    time_range: str = Field(default="last_month", description="Plage de temps: 'last_month', 'last_week', ou 'YYYY-MM-DD to YYYY-MM-DD'")

class WordCloudResponse(BaseModel):
    top_words: List[Dict[str, Any]]

class CollisionHeatMapRequest(BaseModel):
    time_range: str = Field(description="Plage de temps: 'last_month', 'last_week', ou 'YYYY-MM-DD to YYYY-MM-DD'")
    severity_filter: Optional[int] = Field(default=None, ge=0, le=4, description="Filtre de gravité (0-4)")
    death_nb: Optional[int] = Field(default=None, ge=0)
    severely_injured_nb: Optional[int] = Field(default=None, ge=0)
    lightly_injured_nb: Optional[int] = Field(default=None, ge=0)

class CollisionHeatMapResponse(BaseModel):
    collisions: List[Dict[str, Any]]
    total_count: int

class WeatherCorrelationRequest(BaseModel):
    start_date: str = Field(description="Date de début au format YYYY-MM-DD")
    end_date: str = Field(description="Date de fin au format YYYY-MM-DD")
    frequency: str = Field(default="week", description="Fréquence d'agrégation: 'week' ou 'month'")

class WeatherCorrelationResponse(BaseModel):
    summary: Dict[str, Any]
    correlations: List[Dict[str, Any]]
    temperature_analysis: Dict[str, Any]
    precipitation_analysis: Dict[str, Any]
    snow_analysis: Dict[str, Any]
    top_periods: List[Dict[str, Any]]

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

@api.post("/dashboard/wordcloud-311", response_model=WordCloudResponse)
async def wordcloud_311_endpoint(request: WordCloudRequest):
    """
    Obtenir les mots les plus courants des requêtes 311 durant une période donnée.
    """
    try:
        query = WordCloudQuery311()
        result = query.execute(top_n=request.top_n, time_range=request.time_range)
        return WordCloudResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api.post("/dashboard/collision-heatmap", response_model=CollisionHeatMapResponse)
async def collision_heatmap_endpoint(request: CollisionHeatMapRequest):
    """
    Obtenir les données des collisions pour une visualisation de heatmap.
    Les coordonnées sont en latitude/longitude.
    """
    try:
        query = CollisionHeatMapQuery()
        result = query.execute(
            time_range=request.time_range,
            severity_filter=request.severity_filter,
            death_nb=request.death_nb,
            severely_injured_nb=request.severely_injured_nb,
            lightly_injured_nb=request.lightly_injured_nb
        )
        return CollisionHeatMapResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@api.post("/dashboard/weather-correlation", response_model=WeatherCorrelationResponse)
async def weather_correlation_endpoint(request: WeatherCorrelationRequest):
    """
    Analyser la corrélation entre les conditions météorologiques et les collisions routières.
    Retourne des statistiques agrégées par période (semaine ou mois).
    """
    try:
        query = WeatherCorrelationQuery()
        result = query.execute(
            start_date=request.start_date,
            end_date=request.end_date,
            frequency=request.frequency
        )
        
        # Vérifier si une erreur est retournée par la query
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        return WeatherCorrelationResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
