from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from data.collision_forecast_models import CalendarBaselineModel, ConstantModel, WeightedBlendModel

GEOMET_BASE = "https://api.weather.gc.ca"
CLIMATE_DAILY_ITEMS = f"{GEOMET_BASE}/collections/climate-daily/items"
GEOMET_MAX_PAGES = 200
GEOMET_TIMEOUT_S = 30
GEOMET_USER_AGENT = "MobilityCopilot/1.0 (collision-total-training)"

DEFAULT_CLIMATE_IDENTIFIER = "7025251"  # Montreal/Trudeau
DEFAULT_DB_PATH = Path("data/db/mobility.db")
DEFAULT_OUTPUT_DIR = Path("data/models/collision_total_weather_v1")

WEATHER_COLUMNS = [
    "mean_temp_c",
    "min_temp_c",
    "max_temp_c",
    "total_precip_mm",
    "total_snow_cm",
]
TARGET_COLUMN = "y_total"


@dataclass
class TrainingConfig:
    db_path: Path = DEFAULT_DB_PATH
    output_dir: Path = DEFAULT_OUTPUT_DIR
    climate_identifier: str = DEFAULT_CLIMATE_IDENTIFIER
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    eval_days: int = 365
    inner_eval_days: int = 240
    weather_history_days: int = 4
    grave_weight: float = 1.5
    mortel_weight: float = 2.0
    random_state: int = 42
    weather_csv: Optional[Path] = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value))
    return text.encode("ascii", "ignore").decode("ascii").strip().lower()


def _parse_date_from_feature(properties: Dict[str, Any]) -> Optional[pd.Timestamp]:
    local_date = properties.get("LOCAL_DATE")
    if isinstance(local_date, str):
        parsed = pd.to_datetime(local_date[:10], errors="coerce")
        if pd.notna(parsed):
            return parsed.normalize()

    try:
        year = int(properties["LOCAL_YEAR"])
        month = int(properties["LOCAL_MONTH"])
        day = int(properties["LOCAL_DAY"])
        return pd.Timestamp(year=year, month=month, day=day)
    except Exception:
        return None


def _http_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = requests.get(
        url,
        params=params,
        timeout=GEOMET_TIMEOUT_S,
        headers={
            "User-Agent": GEOMET_USER_AGENT,
            "Accept": "application/json,application/geo+json",
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected payload type from {url}: {type(payload)}")
    return payload


def _next_link(payload: Dict[str, Any]) -> Optional[str]:
    for link in payload.get("links", []) or []:
        if isinstance(link, dict) and link.get("rel") == "next" and link.get("href"):
            return str(link["href"])
    return None


def fetch_geomet_daily_weather(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    climate_identifier: str = DEFAULT_CLIMATE_IDENTIFIER,
) -> pd.DataFrame:
    year_start = int(start_date.year)
    year_end = int(end_date.year)

    cql = (
        f"properties.CLIMATE_IDENTIFIER = '{climate_identifier}' "
        f"AND properties.LOCAL_YEAR >= {year_start} "
        f"AND properties.LOCAL_YEAR <= {year_end}"
    )
    params = {
        "f": "json",
        "lang": "en",
        "limit": 1000,
        "filter": cql,
    }

    url = CLIMATE_DAILY_ITEMS
    page = 0
    features: List[Dict[str, Any]] = []
    while page < GEOMET_MAX_PAGES:
        payload = _http_get_json(url, params=params if page == 0 else None)
        features.extend(payload.get("features", []) or [])
        url = _next_link(payload)
        if not url:
            break
        page += 1

    records: List[Dict[str, Any]] = []
    for feature in features:
        properties = (feature or {}).get("properties") or {}
        if not isinstance(properties, dict):
            continue
        weather_date = _parse_date_from_feature(properties)
        if weather_date is None:
            continue
        records.append(
            {
                "date": weather_date,
                "mean_temp_c": _safe_float(properties.get("MEAN_TEMPERATURE")),
                "min_temp_c": _safe_float(properties.get("MIN_TEMPERATURE")),
                "max_temp_c": _safe_float(properties.get("MAX_TEMPERATURE")),
                "total_precip_mm": _safe_float(properties.get("TOTAL_PRECIPITATION")),
                "total_snow_cm": _safe_float(properties.get("TOTAL_SNOW")),
            }
        )

    if not records:
        raise ValueError("No weather records found from GeoMet for selected period.")

    weather = pd.DataFrame.from_records(records)
    weather = weather[(weather["date"] >= start_date) & (weather["date"] <= end_date)].copy()

    aggregated = (
        weather.groupby("date", as_index=False)
        .agg(
            {
                "mean_temp_c": "mean",
                "min_temp_c": "min",
                "max_temp_c": "max",
                "total_precip_mm": "sum",
                "total_snow_cm": "sum",
            }
        )
        .sort_values("date")
    )
    return aggregated


def load_weather_from_csv(weather_csv: Path) -> pd.DataFrame:
    weather = pd.read_csv(weather_csv)
    required = set(["date"] + WEATHER_COLUMNS)
    missing = required - set(weather.columns)
    if missing:
        raise ValueError(f"weather_csv is missing required columns: {sorted(missing)}")

    weather = weather[["date"] + WEATHER_COLUMNS].copy()
    weather["date"] = pd.to_datetime(weather["date"], errors="coerce").dt.normalize()
    weather = weather.dropna(subset=["date"]).drop_duplicates(subset=["date"]).sort_values("date")
    return weather


def _fill_weather_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for col in WEATHER_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        out[col] = out[col].ffill().bfill()
        if out[col].isna().any():
            median = float(out[col].median()) if out[col].notna().any() else 0.0
            out[col] = out[col].fillna(median)
    return out


def build_weather_feature_frame(weather: pd.DataFrame, weather_history_days: int = 4) -> Tuple[pd.DataFrame, List[str]]:
    if weather_history_days not in {2, 3, 4}:
        raise ValueError("weather_history_days must be one of: 2, 3, 4")

    frame = weather[["date"] + WEATHER_COLUMNS].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame = frame.dropna(subset=["date"]).drop_duplicates(subset=["date"]).sort_values("date")
    frame = _fill_weather_columns(frame)

    frame["dow"] = frame["date"].dt.dayofweek
    frame["month"] = frame["date"].dt.month
    frame["quarter"] = frame["date"].dt.quarter
    frame["weekofyear"] = frame["date"].dt.isocalendar().week.astype(int)
    frame["day_of_year"] = frame["date"].dt.dayofyear
    frame["is_weekend"] = (frame["dow"] >= 5).astype(int)
    frame["is_winter"] = frame["month"].isin([12, 1, 2]).astype(int)
    frame["is_summer"] = frame["month"].isin([6, 7, 8]).astype(int)

    frame["dow_sin"] = np.sin(2 * np.pi * frame["dow"] / 7.0)
    frame["dow_cos"] = np.cos(2 * np.pi * frame["dow"] / 7.0)
    frame["doy_sin"] = np.sin(2 * np.pi * frame["day_of_year"] / 366.0)
    frame["doy_cos"] = np.cos(2 * np.pi * frame["day_of_year"] / 366.0)
    frame["month_sin"] = np.sin(2 * np.pi * frame["month"] / 12.0)
    frame["month_cos"] = np.cos(2 * np.pi * frame["month"] / 12.0)

    frame["freeze_day"] = (frame["max_temp_c"] <= 0.0).astype(int)
    frame["rain_day"] = (frame["total_precip_mm"] > 0.0).astype(int)
    frame["snow_day"] = (frame["total_snow_cm"] > 0.0).astype(int)
    frame["heavy_precip_day"] = (frame["total_precip_mm"] >= 10.0).astype(int)
    frame["heavy_snow_day"] = (frame["total_snow_cm"] >= 5.0).astype(int)
    frame["temp_range_c"] = frame["max_temp_c"] - frame["min_temp_c"]
    frame["temp_x_precip"] = frame["mean_temp_c"] * frame["total_precip_mm"]

    for col in WEATHER_COLUMNS:
        for lag in range(1, weather_history_days + 1):
            frame[f"{col}_lag_{lag}"] = frame[col].shift(lag)

        for window in [2, 3, 4]:
            if window <= weather_history_days:
                prev = frame[col].shift(1)
                frame[f"{col}_roll_mean_{window}"] = prev.rolling(window).mean()
                frame[f"{col}_roll_max_{window}"] = prev.rolling(window).max()

        frame[f"{col}_delta_1d"] = frame[col] - frame[f"{col}_lag_1"]

    feature_cols = [c for c in frame.columns if c != "date"]
    return frame, feature_cols


def load_collision_targets(
    db_path: Path,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    conn = sqlite3.connect(str(db_path))
    try:
        collisions = pd.read_sql_query(
            """
            SELECT DT_ACCDN, GRAVITE
            FROM collisions_routieres
            WHERE DT_ACCDN IS NOT NULL
            """,
            conn,
        )
    finally:
        conn.close()

    collisions["date"] = pd.to_datetime(
        collisions["DT_ACCDN"].astype(str).str.replace("/", "-", regex=False),
        errors="coerce",
    ).dt.normalize()
    collisions = collisions.dropna(subset=["date"]).copy()
    if collisions.empty:
        raise ValueError("No collision date found in collisions_routieres.")

    if start_date is not None:
        collisions = collisions[collisions["date"] >= start_date]
    if end_date is not None:
        collisions = collisions[collisions["date"] <= end_date]
    if collisions.empty:
        raise ValueError("No collision rows in selected date range.")

    severity_map = {
        "leger": "y_leger",
        "grave": "y_grave",
        "mortel": "y_mortel",
    }
    collisions["target"] = collisions["GRAVITE"].apply(_normalize_text).map(severity_map)

    grouped = (
        collisions.dropna(subset=["target"])
        .groupby(["date", "target"])
        .size()
        .unstack(fill_value=0)
    )

    full_dates = pd.date_range(
        start=collisions["date"].min(),
        end=collisions["date"].max(),
        freq="D",
    )
    grouped = grouped.reindex(full_dates, fill_value=0)

    for target_col in ["y_leger", "y_grave", "y_mortel"]:
        if target_col not in grouped.columns:
            grouped[target_col] = 0

    grouped = grouped[["y_leger", "y_grave", "y_mortel"]].copy()
    grouped[TARGET_COLUMN] = grouped[["y_leger", "y_grave", "y_mortel"]].sum(axis=1)
    grouped.index.name = "date"
    return grouped.reset_index()


def build_training_frame(
    targets: pd.DataFrame,
    weather: pd.DataFrame,
    weather_history_days: int,
) -> Tuple[pd.DataFrame, List[str], pd.DataFrame]:
    weather_features, weather_feature_cols = build_weather_feature_frame(
        weather=weather,
        weather_history_days=weather_history_days,
    )

    frame = targets.merge(weather_features, on="date", how="inner").sort_values("date")
    feature_cols = [c for c in weather_feature_cols if c in frame.columns]

    required = feature_cols + [TARGET_COLUMN, "y_grave", "y_mortel"]
    frame = frame.dropna(subset=required).copy()
    return frame, feature_cols, weather_features


def split_train_test(frame: pd.DataFrame, eval_days: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if eval_days < 30:
        raise ValueError("eval_days must be >= 30")

    cutoff = frame["date"].max() - pd.Timedelta(days=eval_days)
    train = frame[frame["date"] <= cutoff].copy()
    test = frame[frame["date"] > cutoff].copy()

    if len(train) < 365 or len(test) < 90:
        split_idx = int(len(frame) * 0.8)
        train = frame.iloc[:split_idx].copy()
        test = frame.iloc[split_idx:].copy()

    if train.empty or test.empty:
        raise ValueError("Unable to create non-empty temporal train/test split")
    return train, test


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    err = y_pred - y_true
    abs_err = np.abs(err)

    mae = float(abs_err.mean())
    rmse = float(math.sqrt(np.mean(err ** 2)))
    bias = float(err.mean())
    total_true = float(y_true.sum())
    total_pred = float(y_pred.sum())
    wape = float(abs_err.sum() / max(total_true, 1.0))

    sse = float(np.sum(err ** 2))
    sst = float(np.sum((y_true - y_true.mean()) ** 2))
    r2 = float(1.0 - (sse / sst)) if sst > 0 else 0.0

    non_zero = y_true > 0
    mae_non_zero = float(abs_err[non_zero].mean()) if non_zero.any() else 0.0

    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "bias": round(bias, 4),
        "wape": round(wape, 4),
        "r2": round(r2, 4),
        "mae_non_zero": round(mae_non_zero, 4),
        "sum_true": round(total_true, 4),
        "sum_pred": round(total_pred, 4),
    }


def _inner_temporal_split(train: pd.DataFrame, inner_eval_days: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    inner_days = max(60, int(inner_eval_days))
    cutoff = train["date"].max() - pd.Timedelta(days=inner_days)
    inner_train = train[train["date"] <= cutoff].copy()
    inner_valid = train[train["date"] > cutoff].copy()

    if len(inner_train) < 365 or len(inner_valid) < 60:
        split_idx = int(len(train) * 0.8)
        inner_train = train.iloc[:split_idx].copy()
        inner_valid = train.iloc[split_idx:].copy()

    if inner_train.empty or inner_valid.empty:
        raise ValueError("Unable to create inner validation split")
    return inner_train, inner_valid


def _build_validation_folds(
    train: pd.DataFrame,
    inner_eval_days: int,
    max_year_folds: int = 4,
) -> List[Tuple[pd.DataFrame, pd.DataFrame, str]]:
    candidates: List[Tuple[pd.DataFrame, pd.DataFrame, str]] = []
    years = sorted(train["date"].dt.year.unique().tolist())
    for year in years:
        valid = train[train["date"].dt.year == year].copy()
        train_part = train[train["date"] < pd.Timestamp(year=year, month=1, day=1)].copy()
        if len(valid) < 90:
            continue
        if len(train_part) < 365:
            continue
        candidates.append((train_part, valid, f"year_{year}"))

    if len(candidates) >= 2:
        return candidates[-max_year_folds:]

    inner_train, inner_valid = _inner_temporal_split(train, inner_eval_days=inner_eval_days)
    return [(inner_train, inner_valid, "recent_holdout")]


def _build_calendar_baseline_model(train: pd.DataFrame, feature_cols: List[str]) -> CalendarBaselineModel:
    if "dow" not in feature_cols or "month" not in feature_cols:
        raise ValueError("Feature columns must contain 'dow' and 'month' for calendar baseline.")

    grouped = (
        train.groupby(["dow", "month"], as_index=False)[TARGET_COLUMN]
        .mean()
    )
    lookup: Dict[Tuple[int, int], float] = {}
    for _, row in grouped.iterrows():
        lookup[(int(row["dow"]), int(row["month"]))] = float(row[TARGET_COLUMN])

    return CalendarBaselineModel(
        dow_feature_index=feature_cols.index("dow"),
        month_feature_index=feature_cols.index("month"),
        lookup=lookup,
        fallback=float(train[TARGET_COLUMN].mean()),
    )


def _build_global_baseline_model(train: pd.DataFrame) -> ConstantModel:
    return ConstantModel(value=float(train[TARGET_COLUMN].mean()))


def _sample_weights(train: pd.DataFrame, config: TrainingConfig) -> np.ndarray:
    y_total = train[TARGET_COLUMN].to_numpy(dtype=float)
    y_grave = train["y_grave"].to_numpy(dtype=float)
    y_mortel = train["y_mortel"].to_numpy(dtype=float)

    weights = np.ones_like(y_total, dtype=float)
    weights += 0.15 * np.sqrt(np.clip(y_total, 0.0, None))
    weights += float(config.grave_weight) * (y_grave > 0).astype(float)
    weights += float(config.mortel_weight) * (y_mortel > 0).astype(float)
    return weights


def _fit_hgb(
    HistGradientBoostingRegressor: Any,
    spec: Dict[str, Any],
    weight_profile: str,
    X: np.ndarray,
    y: np.ndarray,
    train_df: pd.DataFrame,
    config: TrainingConfig,
    random_state: int,
) -> Any:
    model = HistGradientBoostingRegressor(
        loss=spec["loss"],
        random_state=random_state,
        **spec["params"],
    )

    if weight_profile == "severity":
        sample_weight = _sample_weights(train_df, config)
    elif weight_profile == "uniform":
        sample_weight = np.ones(shape=len(train_df), dtype=float)
    else:
        raise ValueError(f"Unknown weight_profile: {weight_profile}")

    model.fit(X, y, sample_weight=sample_weight)
    return model


def train_model(
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_cols: List[str],
    config: TrainingConfig,
) -> Tuple[Any, Dict[str, Any], pd.DataFrame]:
    try:
        from sklearn.ensemble import HistGradientBoostingRegressor
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "scikit-learn is required for training. Install with `pip install scikit-learn`."
        ) from exc

    X_train_full = train[feature_cols].to_numpy(dtype=float)
    X_test = test[feature_cols].to_numpy(dtype=float)
    y_train_full = train[TARGET_COLUMN].to_numpy(dtype=float)
    y_test = test[TARGET_COLUMN].to_numpy(dtype=float)
    validation_folds = _build_validation_folds(train, inner_eval_days=config.inner_eval_days)
    fold_payloads: List[Dict[str, Any]] = []
    for fold_train, fold_valid, fold_name in validation_folds:
        X_fold_train = fold_train[feature_cols].to_numpy(dtype=float)
        y_fold_train = fold_train[TARGET_COLUMN].to_numpy(dtype=float)
        X_fold_valid = fold_valid[feature_cols].to_numpy(dtype=float)
        y_fold_valid = fold_valid[TARGET_COLUMN].to_numpy(dtype=float)

        calendar_model_fold = _build_calendar_baseline_model(fold_train, feature_cols=feature_cols)
        global_model_fold = _build_global_baseline_model(fold_train)

        fold_payloads.append(
            {
                "fold_name": fold_name,
                "train_df": fold_train,
                "X_train": X_fold_train,
                "y_train": y_fold_train,
                "X_valid": X_fold_valid,
                "y_valid": y_fold_valid,
                "pred_calendar": calendar_model_fold.predict(X_fold_valid),
                "pred_global": global_model_fold.predict(X_fold_valid),
            }
        )

    y_valid_all = np.concatenate([payload["y_valid"] for payload in fold_payloads])

    candidate_specs = [
        {
            "name": "hgb_poisson",
            "loss": "poisson",
            "params": {
                "learning_rate": 0.03,
                "max_depth": 6,
                "max_iter": 1100,
                "min_samples_leaf": 20,
                "l2_regularization": 0.3,
            },
        },
        {
            "name": "hgb_absolute",
            "loss": "absolute_error",
            "params": {
                "learning_rate": 0.03,
                "max_depth": 7,
                "max_iter": 1200,
                "min_samples_leaf": 12,
                "l2_regularization": 0.15,
            },
        },
    ]
    weight_profiles = ["uniform", "severity"]

    leaderboard: List[Dict[str, Any]] = []
    hgb_valid_preds: Dict[str, np.ndarray] = {}
    seed = config.random_state

    for spec in candidate_specs:
        for weight_profile in weight_profiles:
            candidate_name = f"{spec['name']}__{weight_profile}"
            pred_chunks: List[np.ndarray] = []
            for payload in fold_payloads:
                seed += 1
                model = _fit_hgb(
                    HistGradientBoostingRegressor=HistGradientBoostingRegressor,
                    spec=spec,
                    weight_profile=weight_profile,
                    X=payload["X_train"],
                    y=payload["y_train"],
                    train_df=payload["train_df"],
                    config=config,
                    random_state=seed,
                )
                pred_chunks.append(np.clip(model.predict(payload["X_valid"]), 0.0, None))

            valid_pred = np.concatenate(pred_chunks)
            hgb_valid_preds[candidate_name] = valid_pred
            score = _metrics(y_valid_all, valid_pred)
            leaderboard.append(
                {
                    "name": candidate_name,
                    "kind": "hgb",
                    "spec_name": spec["name"],
                    "weight_profile": weight_profile,
                    "mae": score["mae"],
                    "rmse": score["rmse"],
                    "status": "ok",
                }
            )

    baseline_calendar_valid = np.concatenate([payload["pred_calendar"] for payload in fold_payloads])
    baseline_global_valid = np.concatenate([payload["pred_global"] for payload in fold_payloads])
    calendar_score = _metrics(y_valid_all, baseline_calendar_valid)
    global_score = _metrics(y_valid_all, baseline_global_valid)

    leaderboard.append(
        {
            "name": "baseline_calendar",
            "kind": "baseline_calendar",
            "mae": calendar_score["mae"],
            "rmse": calendar_score["rmse"],
            "status": "ok",
        }
    )
    leaderboard.append(
        {
            "name": "baseline_global",
            "kind": "baseline_global",
            "mae": global_score["mae"],
            "rmse": global_score["rmse"],
            "status": "ok",
        }
    )

    for base_name, base_pred in hgb_valid_preds.items():
        base_row = next(row for row in leaderboard if row["name"] == base_name)
        for alpha in [0.2, 0.35, 0.5, 0.65, 0.8]:
            blend_pred = np.clip(alpha * base_pred + (1.0 - alpha) * baseline_calendar_valid, 0.0, None)
            score = _metrics(y_valid_all, blend_pred)
            leaderboard.append(
                {
                    "name": f"blend_calendar({base_name},a={alpha:.2f})",
                    "kind": "blend",
                    "base_hgb": base_name,
                    "spec_name": base_row["spec_name"],
                    "weight_profile": base_row["weight_profile"],
                    "alpha": float(alpha),
                    "mae": score["mae"],
                    "rmse": score["rmse"],
                    "status": "ok",
                }
            )

    valid_candidates = [row for row in leaderboard if row.get("status") == "ok"]
    selected = min(valid_candidates, key=lambda r: (r["mae"], r["rmse"], r["name"]))

    selected_name = selected["name"]
    selected_kind = selected["kind"]

    if selected_kind == "baseline_calendar":
        final_model = _build_calendar_baseline_model(train, feature_cols=feature_cols)
    elif selected_kind == "baseline_global":
        final_model = _build_global_baseline_model(train)
    elif selected_kind == "hgb":
        spec = next(s for s in candidate_specs if s["name"] == selected["spec_name"])
        final_model = _fit_hgb(
            HistGradientBoostingRegressor=HistGradientBoostingRegressor,
            spec=spec,
            weight_profile=selected["weight_profile"],
            X=X_train_full,
            y=y_train_full,
            train_df=train,
            config=config,
            random_state=config.random_state + 999,
        )
    elif selected_kind == "blend":
        spec = next(s for s in candidate_specs if s["name"] == selected["spec_name"])
        hgb_model = _fit_hgb(
            HistGradientBoostingRegressor=HistGradientBoostingRegressor,
            spec=spec,
            weight_profile=selected["weight_profile"],
            X=X_train_full,
            y=y_train_full,
            train_df=train,
            config=config,
            random_state=config.random_state + 999,
        )
        calendar_model = _build_calendar_baseline_model(train, feature_cols=feature_cols)
        final_model = WeightedBlendModel(
            primary_model=hgb_model,
            secondary_model=calendar_model,
            alpha_primary=float(selected["alpha"]),
        )
    else:  # pragma: no cover
        raise ValueError(f"Unsupported selected candidate kind: {selected_kind}")

    y_pred = np.clip(final_model.predict(X_test), 0.0, None)
    baseline_calendar_test = _build_calendar_baseline_model(train, feature_cols=feature_cols).predict(X_test)
    baseline_global_test = _build_global_baseline_model(train).predict(X_test)

    metrics = {
        "total": {
            "selected_candidate": selected_name,
            "validation_candidates": sorted(
                leaderboard,
                key=lambda row: (row.get("mae", float("inf")), row.get("rmse", float("inf"))),
            ),
            "model": _metrics(y_test, y_pred),
            "baseline_calendar": _metrics(y_test, baseline_calendar_test),
            "baseline_global": _metrics(y_test, baseline_global_test),
        }
    }

    predictions = pd.DataFrame(
        {
            "date": test["date"].values,
            "total_true": y_test,
            "total_pred": y_pred,
            "total_baseline_calendar": baseline_calendar_test,
            "total_baseline_global": baseline_global_test,
            "total_model_name": selected_name,
        }
    )

    return final_model, metrics, predictions


def _serialize_config(config: TrainingConfig) -> Dict[str, Any]:
    return {
        "db_path": str(config.db_path),
        "output_dir": str(config.output_dir),
        "climate_identifier": config.climate_identifier,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "eval_days": config.eval_days,
        "inner_eval_days": config.inner_eval_days,
        "weather_history_days": config.weather_history_days,
        "grave_weight": config.grave_weight,
        "mortel_weight": config.mortel_weight,
        "random_state": config.random_state,
        "weather_csv": str(config.weather_csv) if config.weather_csv else None,
    }


def _resolve_start_end(
    targets: pd.DataFrame,
    start_date: Optional[str],
    end_date: Optional[str],
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    min_date = targets["date"].min()
    max_date = targets["date"].max()

    if start_date:
        parsed_start = pd.to_datetime(start_date, errors="coerce")
        if pd.isna(parsed_start):
            raise ValueError("Invalid start_date format. Expected YYYY-MM-DD")
        min_date = max(min_date, parsed_start.normalize())

    if end_date:
        parsed_end = pd.to_datetime(end_date, errors="coerce")
        if pd.isna(parsed_end):
            raise ValueError("Invalid end_date format. Expected YYYY-MM-DD")
        max_date = min(max_date, parsed_end.normalize())

    if min_date > max_date:
        raise ValueError("Requested start_date is after end_date")

    return min_date, max_date


def save_artifacts(
    output_dir: Path,
    config: TrainingConfig,
    feature_cols: List[str],
    frame: pd.DataFrame,
    train: pd.DataFrame,
    test: pd.DataFrame,
    weather_history: pd.DataFrame,
    model: Any,
    metrics: Dict[str, Any],
    predictions: pd.DataFrame,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        import joblib
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("joblib is required for artifact export") from exc

    joblib.dump(model, output_dir / "model_total.joblib")

    (output_dir / "feature_columns.json").write_text(
        json.dumps(feature_cols, indent=2),
        encoding="utf-8",
    )

    weather_history[["date"] + WEATHER_COLUMNS].to_csv(output_dir / "weather_history.csv", index=False)
    frame.to_csv(output_dir / "daily_model_frame.csv", index=False)
    predictions.to_csv(output_dir / "test_predictions.csv", index=False)

    summary = {
        "generated_at_utc": _utc_now_iso(),
        "config": _serialize_config(config),
        "data": {
            "n_rows_total": int(len(frame)),
            "n_rows_train": int(len(train)),
            "n_rows_test": int(len(test)),
            "date_min": str(frame["date"].min().date()),
            "date_max": str(frame["date"].max().date()),
            "train_date_min": str(train["date"].min().date()),
            "train_date_max": str(train["date"].max().date()),
            "test_date_min": str(test["date"].min().date()),
            "test_date_max": str(test["date"].max().date()),
        },
        "targets": [TARGET_COLUMN],
        "metrics": metrics,
    }

    (output_dir / "training_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    return summary


def run_training(config: TrainingConfig) -> Dict[str, Any]:
    targets = load_collision_targets(db_path=config.db_path)
    start_date, end_date = _resolve_start_end(targets, config.start_date, config.end_date)

    targets = targets[(targets["date"] >= start_date) & (targets["date"] <= end_date)].copy()
    if targets.empty:
        raise ValueError("No target rows available after date filtering")

    if config.weather_csv:
        weather = load_weather_from_csv(config.weather_csv)
    else:
        weather = fetch_geomet_daily_weather(
            start_date=start_date,
            end_date=end_date,
            climate_identifier=config.climate_identifier,
        )

    weather = weather[(weather["date"] >= start_date) & (weather["date"] <= end_date)].copy()
    if weather.empty:
        raise ValueError("No weather rows available after date filtering")

    frame, feature_cols, weather_features = build_training_frame(
        targets=targets,
        weather=weather,
        weather_history_days=config.weather_history_days,
    )

    train, test = split_train_test(frame=frame, eval_days=config.eval_days)

    model, metrics, predictions = train_model(
        train=train,
        test=test,
        feature_cols=feature_cols,
        config=config,
    )

    summary = save_artifacts(
        output_dir=config.output_dir,
        config=config,
        feature_cols=feature_cols,
        frame=frame,
        train=train,
        test=test,
        weather_history=weather_features,
        model=model,
        metrics=metrics,
        predictions=predictions,
    )
    return summary


def parse_args() -> TrainingConfig:
    parser = argparse.ArgumentParser(
        description="Train total collision forecast model by date from weather + calendar features."
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Path to SQLite DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output model directory")
    parser.add_argument("--climate-identifier", default=DEFAULT_CLIMATE_IDENTIFIER, help="GeoMet station id")
    parser.add_argument("--start-date", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="End date YYYY-MM-DD")
    parser.add_argument("--eval-days", type=int, default=365, help="Temporal test window size")
    parser.add_argument("--inner-eval-days", type=int, default=240, help="Validation window size")
    parser.add_argument(
        "--weather-history-days",
        type=int,
        default=4,
        choices=[2, 3, 4],
        help="How many recent weather days are used as lag features",
    )
    parser.add_argument("--grave-weight", type=float, default=1.5, help="Extra sample weight when grave>0")
    parser.add_argument("--mortel-weight", type=float, default=2.0, help="Extra sample weight when mortel>0")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--weather-csv",
        default=None,
        help=(
            "Optional local weather CSV with columns: "
            "date,mean_temp_c,min_temp_c,max_temp_c,total_precip_mm,total_snow_cm"
        ),
    )

    args = parser.parse_args()
    return TrainingConfig(
        db_path=Path(args.db_path),
        output_dir=Path(args.output_dir),
        climate_identifier=args.climate_identifier,
        start_date=args.start_date,
        end_date=args.end_date,
        eval_days=args.eval_days,
        inner_eval_days=args.inner_eval_days,
        weather_history_days=args.weather_history_days,
        grave_weight=args.grave_weight,
        mortel_weight=args.mortel_weight,
        random_state=args.random_state,
        weather_csv=Path(args.weather_csv) if args.weather_csv else None,
    )


def _print_summary(summary: Dict[str, Any]) -> None:
    print("Training completed.")
    print(
        f"Train rows: {summary['data']['n_rows_train']} | "
        f"Test rows: {summary['data']['n_rows_test']} | "
        f"Date range: {summary['data']['date_min']} -> {summary['data']['date_max']}"
    )
    target_metrics = summary["metrics"]["total"]
    selected = target_metrics.get("selected_candidate", "n/a")
    model = target_metrics["model"]
    calendar = target_metrics["baseline_calendar"]
    global_base = target_metrics["baseline_global"]
    print("Model selection + test metrics (MAE / RMSE):")
    print(
        f"  total   [{selected}] model {model['mae']:.3f}/{model['rmse']:.3f} | "
        f"calendar {calendar['mae']:.3f}/{calendar['rmse']:.3f} | "
        f"global {global_base['mae']:.3f}/{global_base['rmse']:.3f}"
    )


if __name__ == "__main__":
    cfg = parse_args()
    result = run_training(cfg)
    _print_summary(result)
