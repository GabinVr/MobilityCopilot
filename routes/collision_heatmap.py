import asyncio
from fastapi import APIRouter, HTTPException
from cache import cache
from data.dashboard_queries import CollisionHeatMapQuery
from models import CollisionHeatMapRequest, CollisionHeatMapResponse

collision_heatmap_router = APIRouter()

@collision_heatmap_router.post("/dashboard/collision-heatmap", response_model=CollisionHeatMapResponse)
@cache(expire=3600*3)
async def collision_heatmap_endpoint(request: CollisionHeatMapRequest):
    """
    Obtenir les données des collisions pour une visualisation de heatmap.
    Les coordonnées sont en latitude/longitude.
    """
    try:
        query = CollisionHeatMapQuery()
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: query.execute(
            time_range=request.time_range,
            severity_filter=request.severity_filter,
            death_nb=request.death_nb,
            severely_injured_nb=request.severely_injured_nb,
            lightly_injured_nb=request.lightly_injured_nb
        ))
        return CollisionHeatMapResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
