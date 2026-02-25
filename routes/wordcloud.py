from fastapi import APIRouter, HTTPException
from cache import cache
from data.dashboard_queries import WordCloudQuery311
from models import WordCloudRequest, WordCloudResponse

wordcloud_router = APIRouter()

@wordcloud_router.post("/dashboard/wordcloud-311", response_model=WordCloudResponse)
@cache(expire=3600*3)
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