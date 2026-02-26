from fastapi import APIRouter, HTTPException

from cache import cache
from models import CollisionForecastJ1Request, CollisionForecastJ1Response
from services.collision_forecast import CollisionForecastService

collision_forecast_router = APIRouter()


@collision_forecast_router.post(
    "/dashboard/collision-forecast-j1",
    response_model=CollisionForecastJ1Response,
)
@cache(expire=3600)
async def collision_forecast_j1_endpoint(request: CollisionForecastJ1Request):
    """
    Predire le nombre total de collisions
    pour une date cible, a l'echelle de toute la zone couverte par la BD.
    """
    try:
        service = CollisionForecastService(model_dir=request.model_dir)
        result = service.predict_for_date(target_date=request.target_date)
        return CollisionForecastJ1Response(**result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
