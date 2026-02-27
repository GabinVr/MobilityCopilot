import asyncio
import json
import logging
from typing import List, Optional

import msgpack
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import cache as cache_module
from core.graph import get_langgraph_app  # noqa: F401 – LangGraph app used as LLM source
from data.trend import TrendQuery
from models import TrendItem, TrendRequest, TrendResponse
from utils.llm_provider import get_llm

_TRENDS_CACHE_TTL = 3600  # 1 hour

trends_router = APIRouter()
logger = logging.getLogger("uvicorn.error")


# ---------------------------------------------------------------------------
# Structured output schema for the LLM  (same pattern as ambiguity_detector)
# ---------------------------------------------------------------------------

class _TrendItemOut(BaseModel):
    metric: str = Field(
        description=(
            "One of: pedestrian_collisions, monthly_collisions, "
            "hourly_peak, 311_top_change, weak_signal_311"
        )
    )
    period: str = Field(
        description="Time period analysed, e.g. '3 derniers mois (2015-01-01 → 2015-03-31)'"
    )
    comparison: str = Field(
        description="What this period is compared to, e.g. 'vs même période l'an passé'"
    )
    interpretation: str = Field(
        description=(
            "One clear French sentence summarising the finding. "
            "Follow the style: 'Les collisions piétons augmentent de 18% sur les 3 derniers mois "
            "vs la même période l'an passé.' or "
            "'Le pic horaire se déplace : avant entre 17h-19h, maintenant entre 15h-17h.'"
        )
    )
    direction: str = Field(description="'up', 'down', or 'stable'")
    pct_change: Optional[float] = Field(
        default=None,
        description="Percentage change (positive = increase). Null when not applicable.",
    )


class _TrendsOut(BaseModel):
    trends: List[_TrendItemOut] = Field(
        description="List of trend cards, one per metric that has available data."
    )


_SYSTEM_PROMPT = """
Tu es un analyste expert en mobilité urbaine pour la Ville de Montréal.
À partir des statistiques brutes fournies, génère des cartes de tendances mobilitaires.

RÈGLES :
- Génère uniquement les cartes pour les métriques qui ont des données disponibles (pct_change non null, ou données non vides).
- Chaque carte doit avoir : une période précise, une comparaison claire, et UNE phrase d'interprétation factuelle en français.
- Format de la phrase : "Les [métrique] [augmentent/baissent] de X% sur [période] vs [comparaison] (N vs M)."
  ou pour le pic horaire : "Le pic horaire se déplace : avant entre Xh-X+2h, maintenant entre Yh-Y+2h."
- Ne jamais inventer de données. Si une métrique n'a pas de données, ne la retourne pas.
- Toujours en français.
"""


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@trends_router.post("/dashboard/trends", response_model=TrendResponse)
async def trends_endpoint(request: TrendRequest):
    """Generate a trends report using raw stats + LangGraph LLM structured output."""
    cache_key = f"trends:{request.as_of_date or 'latest'}"

    # Try to serve from Redis cache (fastapi-cache2 skips POST, so we do it inline)
    try:
        redis = cache_module.redis_client_async
        if redis:
            cached = await redis.get(cache_key)
            if cached:
                logger.info("💾 trends cache hit")
                data = msgpack.unpackb(cached, raw=False)
                return TrendResponse(**data)
    except Exception as e:
        logger.warning("Cache read failed: %s", e)

    try:
        # Step 1 – compute raw statistics from the database
        query = TrendQuery()
        loop = asyncio.get_event_loop()
        raw_stats = await loop.run_in_executor(
            None, lambda: query.build_raw_stats(as_of_date=request.as_of_date)
        )

        # Step 2 – use the same LLM that powers the LangGraph nodes (get_llm()),
        # with structured output exactly like the ambiguity_detector node does.
        llm = get_llm()
        structured_llm = llm.with_structured_output(_TrendsOut)

        user_message = (
            "Voici les statistiques brutes à analyser :\n\n"
            + json.dumps(raw_stats, indent=2, ensure_ascii=False)
        )

        result: _TrendsOut = await loop.run_in_executor(
            None,
            lambda: structured_llm.invoke(
                [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ]
            ),
        )

        trends = [
            TrendItem(
                metric=t.metric,
                period=t.period,
                comparison=t.comparison,
                interpretation=t.interpretation,
                direction=t.direction,
                pct_change=t.pct_change,
            )
            for t in result.trends
        ]

        response = TrendResponse(
            generated_at=raw_stats["generated_at"],
            as_of_date=raw_stats["as_of_date"],
            trends=trends,
        )

        # Store in Redis cache
        try:
            redis = cache_module.redis_client_async
            if redis:
                await redis.setex(
                    cache_key,
                    _TRENDS_CACHE_TTL,
                    msgpack.packb(response.model_dump(), default=str),
                )
                logger.info("✅ trends cached for 1 h")
        except Exception as e:
            logger.warning("Cache write failed: %s", e)

        return response

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("trends_endpoint error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
