from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

import joblib
import numpy as np
import pandas as pd

from data.collision_forecast_train import (
    WEATHER_COLUMNS,
    build_feature_frame,
    fetch_geomet_daily_weather,
    load_collision_targets,
)

DEFAULT_DB_PATH = Path("data/db/mobility.db")
DEFAULT_MODEL_DIR_CANDIDATES = [
    Path("data/models/collision_j1_v1_tuned"),
    Path("data/models/collision_j1_v1"),
    Path("data/models/collision_j1_v1_tuned_replay"),
]


def _parse_date(value: Optional[str]) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.normalize()


def _resolve_model_dir(model_dir: Optional[str] = None) -> Path:
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

    models: Dict[str, Any] = {}
    for target in ["leger", "grave", "mortel"]:
        model_path = model_dir / f"model_{target}.joblib"
        if not model_path.exists():
            raise FileNotFoundError(f"Missing model file: {model_path}")
        models[target] = joblib.load(model_path)

    return {
        "model_dir": model_dir,
        "feature_columns": feature_columns,
        "summary": summary,
        "models": models,
    }


class CollisionForecastService:
    def __init__(
        self,
        model_dir: Optional[str] = None,
        db_path: Path = DEFAULT_DB_PATH,
        climate_identifier: str = "7025251",
    ):
        self.db_path = Path(db_path)
        self.model_dir = _resolve_model_dir(model_dir)
        self.climate_identifier = climate_identifier
        self.bundle = _load_model_bundle(str(self.model_dir))

    def _load_weather(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
        frame_path = self.model_dir / "daily_training_frame.csv"
        if frame_path.exists():
            try:
                use_cols = ["date"] + WEATHER_COLUMNS
                from_frame = pd.read_csv(frame_path, usecols=use_cols)
                from_frame["date"] = pd.to_datetime(from_frame["date"], errors="coerce").dt.normalize()
                from_frame = from_frame.dropna(subset=["date"]).drop_duplicates(subset=["date"])
                from_frame = from_frame.sort_values("date")
                window = from_frame[
                    (from_frame["date"] >= start_date) & (from_frame["date"] <= end_date)
                ].copy()
                if not window.empty and window["date"].nunique() >= int((end_date - start_date).days * 0.95):
                    return window
            except Exception:
                pass

        return fetch_geomet_daily_weather(
            start_date=start_date,
            end_date=end_date,
            climate_identifier=self.climate_identifier,
        )

    def _build_inference_row(self, as_of_date: Optional[str]) -> tuple[pd.Timestamp, np.ndarray]:
        targets = load_collision_targets(db_path=self.db_path)
        if targets.empty:
            raise ValueError("No collision targets available in database.")

        max_available_date = pd.to_datetime(targets["date"].max()).normalize()
        requested = _parse_date(as_of_date)
        if as_of_date and requested is None:
            raise ValueError("Invalid as_of_date format. Expected YYYY-MM-DD.")

        as_of = requested or max_available_date
        if as_of > max_available_date:
            raise ValueError(
                f"as_of_date={as_of.date()} is after max available date {max_available_date.date()}."
            )

        targets = targets[targets["date"] <= as_of].copy()
        if targets.empty:
            raise ValueError("No historical targets available at requested as_of_date.")

        # Ensure build_feature_frame can compute J+1 labels (it drops rows with missing targets_j1).
        next_day = as_of + pd.Timedelta(days=1)
        if targets["date"].max() < next_day:
            synthetic = {
                "date": next_day,
                "y_leger": 0.0,
                "y_grave": 0.0,
                "y_mortel": 0.0,
                "y_total": 0.0,
            }
            targets = pd.concat([targets, pd.DataFrame([synthetic])], ignore_index=True)

        start_date = pd.to_datetime(targets["date"].min()).normalize()
        weather = self._load_weather(start_date=start_date, end_date=next_day)

        frame, _, _ = build_feature_frame(targets=targets, weather=weather)
        candidate = frame[frame["date"] == as_of]
        if candidate.empty:
            first_available = frame["date"].min() if not frame.empty else None
            if first_available is None:
                raise ValueError("Unable to build inference features for requested date.")
            raise ValueError(
                f"as_of_date={as_of.date()} unavailable after feature engineering. "
                f"Try >= {pd.Timestamp(first_available).date()}."
            )

        feature_columns = self.bundle["feature_columns"]
        missing_cols = [col for col in feature_columns if col not in candidate.columns]
        if missing_cols:
            raise ValueError(
                f"Missing expected feature columns for model inference: {missing_cols[:10]}"
            )
        x = candidate[feature_columns].to_numpy(dtype=float)
        return as_of, x

    def predict_j1(self, as_of_date: Optional[str] = None) -> Dict[str, Any]:
        as_of, x = self._build_inference_row(as_of_date=as_of_date)

        raw_predictions: Dict[str, float] = {}
        rounded_predictions: Dict[str, int] = {}
        selected_candidates: Dict[str, str] = {}

        metrics = (self.bundle.get("summary") or {}).get("metrics") or {}
        for target in ["leger", "grave", "mortel"]:
            model = self.bundle["models"][target]
            raw_value = float(np.clip(model.predict(x)[0], 0.0, None))
            raw_predictions[target] = round(raw_value, 4)
            rounded_predictions[target] = int(max(0, round(raw_value)))
            selected_candidates[target] = (
                (metrics.get(target) or {}).get("selected_candidate") or "unknown"
            )

        return {
            "as_of_date": as_of.strftime("%Y-%m-%d"),
            "forecast_date": (as_of + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            "nb_leger": rounded_predictions["leger"],
            "nb_grave": rounded_predictions["grave"],
            "nb_mortel": rounded_predictions["mortel"],
            "model_version": self.model_dir.name,
            "selected_models": selected_candidates,
            "raw_predictions": raw_predictions,
        }
