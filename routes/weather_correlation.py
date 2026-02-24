from fastapi import APIRouter, HTTPException
from data.dashboard_queries import WeatherCorrelationQuery
from models import WeatherCorrelationRequest, WeatherCorrelationResponse

weather_correlation_router = APIRouter()

@weather_correlation_router.post("/dashboard/weather-correlation", response_model=WeatherCorrelationResponse)
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

