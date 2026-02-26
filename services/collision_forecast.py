from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

import joblib
import numpy as np
import pandas as pd

from data.collision_forecast_train import WEATHER_COLUMNS, build_weather_feature_frame
from services.weather_provider_openmeteo import resolve_weather_window_for_date

DEFAULT_MODEL_DIR_CANDIDATES = [
    Path("data/models/collision_total_weather_v1"),
]


class CollisionForecastService:
    def __init__(self, model_dir: Optional[str] = None):
        self.model_dir = _resolve_model_dir(model_dir)
        self.bundle = _load_model_bundle(str(self.model_dir))

    def _local_weather_history(self) -> pd.DataFrame:
        direct_weather = self.model_dir / "weather_history.csv"
        if direct_weather.exists():
            return _load_weather_csv(direct_weather)

        frame_path = self.model_dir / "daily_model_frame.csv"
        if frame_path.exists():
            cols = ["date"] + WEATHER_COLUMNS
            frame = pd.read_csv(frame_path, usecols=cols)
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
            frame = frame.dropna(subset=["date"]).drop_duplicates(subset=["date"]).sort_values("date")
            return frame

        raise FileNotFoundError(
            f"Missing weather history in {self.model_dir} (expected weather_history.csv or daily_model_frame.csv)."
        )

    def _build_inference_row(self, target_date: pd.Timestamp) -> tuple[np.ndarray, str]:
        history_days = int((self.bundle.get("summary") or {}).get("config", {}).get("weather_history_days", 4))
        history_days = max(2, min(4, history_days))

        local_weather = self._local_weather_history()
        window_start = target_date - pd.Timedelta(days=history_days)

        source = "local_weather_history"
        weather = local_weather.copy()

        local_max = pd.to_datetime(local_weather["date"].max()).normalize()
        if target_date > local_max:
            api_weather, max_forecast_date = resolve_weather_window_for_date(
                target_date=target_date,
                history_days=history_days,
            )
            if max_forecast_date is not None and target_date > max_forecast_date:
                raise ValueError(
                    f"target_date={target_date.date()} is after max forecast weather date {max_forecast_date.date()}."
                )
            weather = (
                pd.concat([local_weather, api_weather], ignore_index=True)
                .drop_duplicates(subset=["date"], keep="last")
                .sort_values("date")
            )
            source = "openmeteo"

        window = weather[(weather["date"] >= window_start) & (weather["date"] <= target_date)].copy()
        required_days = history_days + 1
        if window["date"].nunique() < required_days:
            raise ValueError(
                f"Insufficient weather history for target_date={target_date.date()}. "
                f"Need {required_days} days from {window_start.date()} to {target_date.date()}."
            )

        weather_features, _ = build_weather_feature_frame(window, weather_history_days=history_days)
        candidate = weather_features[weather_features["date"] == target_date].copy()
        if candidate.empty:
            raise ValueError(f"Unable to build weather features for target_date={target_date.date()}.")

        feature_columns = self.bundle["feature_columns"]
        missing_cols = [col for col in feature_columns if col not in candidate.columns]
        if missing_cols:
            raise ValueError(
                f"Missing expected feature columns for model inference: {missing_cols[:10]}"
            )

        x = candidate[feature_columns].to_numpy(dtype=float)
        return x, source

    def predict_for_date(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        parsed_date = _parse_target_date(target_date)
        requested_date = parsed_date or _default_target_date()

        x, weather_source = self._build_inference_row(requested_date)

        model = self.bundle["model"]
        raw_value = float(np.clip(model.predict(x)[0], 0.0, None))
        rounded_value = int(max(0, round(raw_value)))
        selected_candidate = (
            ((self.bundle.get("summary") or {}).get("metrics") or {}).get("total", {}).get("selected_candidate")
            or "unknown"
        )

        history_days = int((self.bundle.get("summary") or {}).get("config", {}).get("weather_history_days", 4))

        return {
            "target_date": requested_date.strftime("%Y-%m-%d"),
            "nb_collisions": rounded_value,
            "model_version": self.model_dir.name,
            "selected_model": selected_candidate,
            "raw_prediction": round(raw_value, 4),
            "weather_source": weather_source,
            "weather_history_days": history_days,
        }


@lru_cache(maxsize=8)
def _load_model_bundle(model_dir_value: str) -> Dict[str, Any]:
    model_dir = Path(model_dir_value)
    feature_columns_path = model_dir / "feature_columns.json"
    summary_path = model_dir / "training_summary.json"

    if not feature_columns_path.exists():
        raise FileNotFoundError(f"Missing feature_columns.json in {model_dir}")
    if not summary_path.exists():
        raise FileNotFoundError(f"Missing training_summary.json in {model_dir}")

    with feature_columns_path.open("r", encoding="utf-8") as handle:
        feature_columns = json.load(handle)
    with summary_path.open("r", encoding="utf-8") as handle:
        summary = json.load(handle)

    model_path = model_dir / "model_total.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"Missing model file: {model_path}")
    model = joblib.load(model_path)

    return {
        "model_dir": model_dir,
        "feature_columns": feature_columns,
        "summary": summary,
        "model": model,
    }


def _resolve_model_dir(model_dir: Optional[str]) -> Path:
    if model_dir:
        path = Path(model_dir)
        if not path.exists():
            raise FileNotFoundError(f"Model directory not found: {path}")
        return path

    for candidate in DEFAULT_MODEL_DIR_CANDIDATES:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "No model directory found. Expected one of: "
        + ", ".join(str(p) for p in DEFAULT_MODEL_DIR_CANDIDATES)
    )


def _parse_target_date(value: Optional[str]) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError("Invalid target_date format. Expected YYYY-MM-DD.")
    return parsed.normalize()


def _default_target_date() -> pd.Timestamp:
    return (pd.Timestamp.now(tz="America/Toronto").normalize().tz_localize(None) + pd.Timedelta(days=1))


def _load_weather_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    expected = set(["date"] + WEATHER_COLUMNS)
    missing = expected - set(frame.columns)
    if missing:
        raise ValueError(f"weather history file is missing columns: {sorted(missing)}")

    frame = frame[["date"] + WEATHER_COLUMNS].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame = frame.dropna(subset=["date"]).drop_duplicates(subset=["date"]).sort_values("date")
    return frame
