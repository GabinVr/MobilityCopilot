from fastapi import FastAPI
from routes.hotspot import hotspot_router
from routes.chat import chat_router
from routes.wordcloud import wordcloud_router
from routes.collision_heatmap import collision_heatmap_router
from routes.weather_correlation import weather_correlation_router
from routes.trends import trends_router
from scheduler import lifespan
import logging

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)

api = FastAPI(title="MobilityCopilot API", lifespan=lifespan)
app = api

api.include_router(hotspot_router)
api.include_router(chat_router)
api.include_router(wordcloud_router)
api.include_router(collision_heatmap_router)
api.include_router(weather_correlation_router)
api.include_router(trends_router)


