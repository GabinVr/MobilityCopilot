from services.weekly_report import hebdo_hotspots_briefing_generator
from services.update311 import update_311_requests
from core.graph import build_workflow
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from contextlib import asynccontextmanager
from cache import init_cache, close_cache
from pathlib import Path

CHECKPOINTS_DB = Path("checkpoints.db") # SQLite file for LangGraph checkpoints, stored locally for simplicity. (Better than InMemorySaver)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_cache()

    async with AsyncSqliteSaver.from_conn_string(str(CHECKPOINTS_DB)) as saver:
        workflow = build_workflow()
        app.state.saver = saver
        app.state.graph = workflow.compile(checkpointer=saver)  

        scheduler = BackgroundScheduler()
        scheduler.add_job(hebdo_hotspots_briefing_generator, 'cron', day_of_week='mon', hour=8, minute=0)
        scheduler.add_job(update_311_requests, 'cron', hour=3, minute=0)

        scheduler.start()

        try:
            yield
        finally:
            scheduler.shutdown()
            await close_cache()


__all__ = ["lifespan"]