"""
All basemodels used in the API are defined here
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List

class ChatRequest(BaseModel):
    query: str
    thread_id: Optional[str] = None
    audience: str = "grand_public" # 'grand_public' or 'municipalite'
    
class ChatResponse(BaseModel):
    answer: str
    thread_id: str
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


class TrendItem(BaseModel):
    metric: str
    period: str
    comparison: str
    interpretation: str
    direction: str  # "up" | "down" | "stable"
    pct_change: Optional[float] = None


class TrendResponse(BaseModel):
    generated_at: str
    as_of_date: str
    trends: List[TrendItem]
