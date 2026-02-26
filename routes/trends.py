import asyncio
from fastapi import APIRouter, HTTPException
from cache import cache
from data.trend import TrendQuery
from models import TrendRequest, TrendResponse

trends_router = APIRouter()

@trends_router.post("/dashboard/trends", response_model=TrendResponse)
@cache(expire=3600)
async def trends_endpoint(request: TrendRequest):
    """Générer un rapport de tendances mobilité (collisions + requêtes 311)."""
    try:
        query = TrendQuery()
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: query.execute(as_of_date=request.as_of_date))
        return TrendResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


