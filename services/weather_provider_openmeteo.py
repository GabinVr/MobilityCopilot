from __future__ import annotations

from typing import Dict, Optional

import pandas as pd
import requests


OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_TIMEOUT_S = 30
OPEN_METEO_DAILY_FIELDS = [
    "temperature_2m_mean",
    "temperature_2m_min",
    "temperature_2m_max",
    "precipitation_sum",
    "snowfall_sum",
]


def _parse_daily_payload(payload: Dict) -> pd.DataFrame:
    daily = payload.get("daily") or {}
    times = daily.get("time") or []
    if not times:
        raise ValueError("Open-Meteo returned no daily weather rows")

    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(times, errors="coerce").normalize(),
            "mean_temp_c": pd.to_numeric(daily.get("temperature_2m_mean"), errors="coerce"),
            "min_temp_c": pd.to_numeric(daily.get("temperature_2m_min"), errors="coerce"),
            "max_temp_c": pd.to_numeric(daily.get("temperature_2m_max"), errors="coerce"),
            "total_precip_mm": pd.to_numeric(daily.get("precipitation_sum"), errors="coerce"),
            "total_snow_cm": pd.to_numeric(daily.get("snowfall_sum"), errors="coerce"),
        }
    )
    frame = frame.dropna(subset=["date"]).drop_duplicates(subset=["date"]).sort_values("date")
    return frame


def fetch_openmeteo_archive_weather(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    latitude: float = 45.5017,
    longitude: float = -73.5673,
    timezone: str = "America/Toronto",
) -> pd.DataFrame:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": timezone,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "daily": ",".join(OPEN_METEO_DAILY_FIELDS),
    }
    response = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=OPEN_METEO_TIMEOUT_S)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Open-Meteo archive payload")
    return _parse_daily_payload(payload)


def fetch_openmeteo_recent_plus_forecast(
    history_days: int = 7,
    forecast_days: int = 16,
    latitude: float = 45.5017,
    longitude: float = -73.5673,
    timezone: str = "America/Toronto",
) -> pd.DataFrame:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": timezone,
        "past_days": int(max(0, history_days)),
        "forecast_days": int(max(1, forecast_days)),
        "daily": ",".join(OPEN_METEO_DAILY_FIELDS),
    }
    response = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=OPEN_METEO_TIMEOUT_S)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Open-Meteo forecast payload")
    return _parse_daily_payload(payload)


def resolve_weather_window_for_date(
    target_date: pd.Timestamp,
    history_days: int,
    latitude: float = 45.5017,
    longitude: float = -73.5673,
    timezone: str = "America/Toronto",
) -> tuple[pd.DataFrame, Optional[pd.Timestamp]]:
    """
    Returns a weather frame including at least [target_date-history_days, target_date] if available.
    Also returns max available date from forecast when forecast endpoint is used.
    """
    today = pd.Timestamp.now(tz=timezone).normalize().tz_localize(None)

    window_start = target_date - pd.Timedelta(days=history_days)
    if target_date <= today:
        weather = fetch_openmeteo_archive_weather(
            start_date=window_start,
            end_date=target_date,
            latitude=latitude,
            longitude=longitude,
            timezone=timezone,
        )
        return weather, None

    weather = fetch_openmeteo_recent_plus_forecast(
        history_days=max(history_days + 2, 7),
        forecast_days=16,
        latitude=latitude,
        longitude=longitude,
        timezone=timezone,
    )
    max_available = pd.to_datetime(weather["date"].max()).normalize()
    return weather, max_available
