from services.weekly_report import hebdo_hotspots_briefing_generator
from services.update311 import update_311_requests
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
from cache import init_cache

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_cache()

    scheduler = BackgroundScheduler()

    scheduler.add_job(hebdo_hotspots_briefing_generator, 'cron', day_of_week='mon', hour=8, minute=0)
    scheduler.add_job(update_311_requests, 'cron', hour=3, minute=0)

    scheduler.start()

    yield

    scheduler.shutdown()