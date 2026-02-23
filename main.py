from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from core.graph import get_langgraph_app
from langchain_core.messages import HumanMessage
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from data.dashboard_queries import (
    WordCloudQuery311,
    CollisionHeatMapQuery,
    WeatherCorrelationQuery
)
from data.trend import TrendQuery
import logging
logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)

DERNIER_HOTSPOT = "Aucun rapport généré pour le moment."

def hebdo_hotspots_briefing_generator():
    """Générer un briefing hebdomadaire des hotspots de mobilité à Montréal et le stocker dans une variable globale."""

    global DERNIER_HOTSPOT
    prompt = """
        Generate the weekly mobility hotspots briefing for Montreal.
    
    TASK:
    1. Analyze the mobility data (collisions, 311 requests, etc.) and weather conditions provided in the context for the last 7 days.
    2. Identify the TOP 5 absolute worst hotspots based on the highest concentration of problems.
    
    CRITICAL ANTI-HALLUCINATION RULES:
    - You MUST rely EXCLUSIVELY on the data retrieved by the SQL queries and API tools.
    - IF THE DATA IS EMPTY or insufficient to find 5 hotspots, DO NOT INVENT or hallucinate locations. 
    - If there is absolutely no data, your response must be exactly: "Aucun incident ou requête majeure n'a été enregistré dans nos bases de données pour cette semaine."
    - If you only find 2 hotspots in the real data, only output 2. Do not invent the remaining 3.
    
    STRICT FORMATTING RULES (Only for REAL data found):
    - Hotspot #[Rank] : [Location/Zone] - [Total Number] [Type of Issue] ([Specific details]), [Time/Duration/Context], [Weather condition if relevant].
    
    EXAMPLES OF EXPECTED OUTPUT (Do NOT copy these, use them only as a formatting guide):
    - Hotspot #1 : Intersection Peel/Ste-Catherine - 32 collisions (dont 6 graves), surtout entre 16h-19h, sous la pluie.
    - Hotspot #2 : Secteur Plateau-Mont-Royal - 120 requêtes 311 (Nids-de-poule) en 7 jours.
    
    IMPORTANT: 
    - Output ONLY the formatted list based on REAL data.
    - Respond strictly in French.
    """
    
    initial_state = {
        "messages": [HumanMessage(content=prompt)],
        "audience": "grand_public",
        "is_ambiguous": False
    }

    no_data_message = "Aucun incident ou requête majeure n'a été enregistré dans nos bases de données pour cette semaine."

    try:
        resultats = langgraph_app.invoke(initial_state)

        rapport = resultats.get("analytical_response", "Erreur de génération.")
        notes = resultats.get("contradictor_notes", "")

        if rapport == no_data_message:
            DERNIER_HOTSPOT = no_data_message
        else:
            DERNIER_HOTSPOT = f"Voici le rapport hebdomadaire des hotspots de mobilité à Montréal :\n\n{rapport}\n\n Notes de sécurité : {notes}"
    
    except Exception as e:
        DERNIER_HOTSPOT = f"Erreur lors de la génération du rapport "

@asynccontextmanager
async def lifespan(app: FastAPI):

    scheduler = BackgroundScheduler()

    scheduler.add_job(hebdo_hotspots_briefing_generator, 'cron', day_of_week='mon', hour=8, minute=0)

    scheduler.start()

    yield

    scheduler.shutdown()


api = FastAPI(title="MobilityCopilot API", lifespan=lifespan)
langgraph_app = get_langgraph_app()
app = api

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

class CollisionData(BaseModel):
    lat: float = Field(description="Latitude de la collision")
    lon: float = Field(description="Longitude de la collision")
    severity: str = Field(description="Niveau de gravité")
    deaths: int = Field(description="Nombre de décès")
    severely_injured: int = Field(description="Nombre de blessés graves")
    lightly_injured: int = Field(description="Nombre de blessés légers")
    date: str = Field(description="Date de la collision")
    id: str = Field(description="Identifiant unique de la collision")

class CollisionHeatMapResponse(BaseModel):
    collisions: List[CollisionData]
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


class TrendRequest(BaseModel):
    as_of_date: Optional[str] = Field(
        default=None,
        description="Date d'analyse au format YYYY-MM-DD. Si absent, utilise la date max disponible.",
    )


class TrendResponse(BaseModel):
    generated_at: str
    as_of_date: str
    monthly_collisions: Dict[str, Any]
    pedestrian_3m_vs_last_year: Dict[str, Any]
    hourly_peak_shift: Dict[str, Any]
    weekly_311_changes: Dict[str, Any]
    weak_signals_311: Dict[str, Any]
    insights: List[str]


@api.get("/last_hotspot_report")

def get_last_hotspot_report():
    """
    Return the latest generated mobility hotspot report for Montreal. This report is updated by the hebdo_hotspots_briefing_generator function.
    If no report has been generated yet, it will launch the generator to create the first report and return it.
    """
    if DERNIER_HOTSPOT == "Aucun rapport généré pour le moment.":
        hebdo_hotspots_briefing_generator()
    return {"report": DERNIER_HOTSPOT}

@api.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    try:
        initial_state = {
            "messages": [HumanMessage(content=request.query)],
            "audience": request.audience,
            "is_ambiguous": False
        }
        logger.debug(f"Initial state for LangGraph: {initial_state}")
        final_state = langgraph_app.invoke(initial_state)
        logger.info(f"Final state from LangGraph: {final_state}")
        
        # Check if the response is ambiguous
        if final_state.get("is_ambiguous"):
            options = final_state.get("clarification_options", [])
            answer = "\n".join(options) if isinstance(options, list) else str(options)
            logger.info(f"Ambiguous response with options: {answer}")
            return ChatResponse(
                answer=answer,
                is_ambiguous=True
            )
        
        # Extract analytical response - fallback to empty string if not present
        analytical_response = final_state.get("analytical_response") or final_state.get("answer") or "Réponse vide"
        contradictor_notes = final_state.get("contradictor_notes")
        
        logger.info(f"Normal response: {analytical_response[:100]}...")
        return ChatResponse(
            answer=analytical_response,
            is_ambiguous=False,
            contradictor_notes=contradictor_notes
        )

    except Exception as e:
        logger.error(f"Error in chat_endpoint: {str(e)}", exc_info=True)
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


@api.post("/dashboard/trends", response_model=TrendResponse)
async def trends_endpoint(request: TrendRequest):
    """Générer un rapport de tendances mobilité (collisions + requêtes 311)."""
    try:
        query = TrendQuery()
        result = query.execute(as_of_date=request.as_of_date)
        return TrendResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


