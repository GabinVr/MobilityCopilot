from __future__ import annotations

import argparse
import json
import math
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests


GEOMET_BASE = "https://api.weather.gc.ca"
CLIMATE_DAILY_ITEMS = f"{GEOMET_BASE}/collections/climate-daily/items"
GEOMET_MAX_PAGES = 200
GEOMET_TIMEOUT_S = 30
GEOMET_USER_AGENT = "MobilityCopilot/1.0 (collision-forecast-training)"

DEFAULT_CLIMATE_IDENTIFIER = "7025251"  # Montreal/Trudeau reference station
DEFAULT_DB_PATH = Path("data/db/mobility.db")
DEFAULT_OUTPUT_DIR = Path("data/models/collision_j1_v1")

TARGET_COLUMNS = ["y_leger", "y_grave", "y_mortel"]
WEATHER_COLUMNS = [
    "mean_temp_c",
    "min_temp_c",
    "max_temp_c",
    "total_precip_mm",
    "total_snow_cm",
]


@dataclass
class TrainingConfig:
    db_path: Path = DEFAULT_DB_PATH
    output_dir: Path = DEFAULT_OUTPUT_DIR
    climate_identifier: str = DEFAULT_CLIMATE_IDENTIFIER
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    eval_days: int = 365
    inner_eval_days: int = 240
    random_state: int = 42
    weather_csv: Optional[Path] = None


class TwoStageCountModel:
    """
    Predict count as: P(event > 0) * E[count | event > 0].
    Useful for sparse targets such as grave/mortel.
    """

    def __init__(self, classifier: Any, regressor: Optional[Any], positive_default: float):
        self.classifier = classifier
        self.regressor = regressor
        self.positive_default = float(max(positive_default, 0.0))

    def predict(self, X: np.ndarray) -> np.ndarray:
        event_prob = self.classifier.predict_proba(X)[:, 1]
        if self.regressor is None:
            positive_count = np.full(shape=len(event_prob), fill_value=self.positive_default, dtype=float)
        else:
            positive_count = np.clip(self.regressor.predict(X), 0.0, None)
        return np.clip(event_prob * positive_count, 0.0, None)


class ConstantEventClassifier:
    """Fallback binary classifier with a fixed positive class probability."""

    def __init__(self, positive_probability: float):
        self.positive_probability = float(np.clip(positive_probability, 0.0, 1.0))

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        n = len(X)
        p1 = np.full(shape=n, fill_value=self.positive_probability, dtype=float)
        p0 = 1.0 - p1
        return np.column_stack([p0, p1])


class FeatureColumnModel:
    """Model wrapper returning a precomputed feature column as prediction."""

    def __init__(self, feature_index: int, fallback_value: float = 0.0):
        self.feature_index = int(feature_index)
        self.fallback_value = float(max(fallback_value, 0.0))

    def predict(self, X: np.ndarray) -> np.ndarray:
        values = X[:, self.feature_index]
        safe = np.where(np.isfinite(values), values, self.fallback_value).astype(float)
        return np.clip(safe, 0.0, None)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        return None


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


def _normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value))
    return text.encode("ascii", "ignore").decode("ascii").strip().lower()


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

    for target_col in TARGET_COLUMNS:
        if target_col not in grouped.columns:
            grouped[target_col] = 0

    grouped = grouped[TARGET_COLUMNS].copy()
    grouped["y_total"] = grouped[TARGET_COLUMNS].sum(axis=1)
    grouped.index.name = "date"
    return grouped.reset_index()


def _load_weather_from_csv(weather_csv: Path) -> pd.DataFrame:
    weather = pd.read_csv(weather_csv)
    missing = set(["date"] + WEATHER_COLUMNS) - set(weather.columns)
    if missing:
        raise ValueError(f"weather_csv is missing required columns: {sorted(missing)}")

    weather["date"] = pd.to_datetime(weather["date"], errors="coerce").dt.normalize()
    weather = weather.dropna(subset=["date"]).copy()
    return weather[["date"] + WEATHER_COLUMNS].sort_values("date")


def build_feature_frame(targets: pd.DataFrame, weather: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], List[str]]:
    frame = targets.merge(weather, on="date", how="left").sort_values("date")

    for col in WEATHER_COLUMNS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame[col] = frame[col].ffill().bfill()
        if frame[col].isna().any():
            median = float(frame[col].median()) if frame[col].notna().any() else 0.0
            frame[col] = frame[col].fillna(median)

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

    frame["freeze_day"] = (frame["max_temp_c"] <= 0).astype(int)
    frame["rain_day"] = (frame["total_precip_mm"] > 0).astype(int)
    frame["snow_day"] = (frame["total_snow_cm"] > 0).astype(int)
    frame["heavy_precip_day"] = (frame["total_precip_mm"] >= 10).astype(int)
    frame["heavy_snow_day"] = (frame["total_snow_cm"] >= 5).astype(int)
    frame["temp_x_precip"] = frame["mean_temp_c"] * frame["total_precip_mm"]
    frame["temp_range_c"] = frame["max_temp_c"] - frame["min_temp_c"]

    lag_days = [1, 2, 3, 7, 14, 21, 28, 35]
    rolling_windows = [7, 14, 28]
    lag_targets = TARGET_COLUMNS + ["y_total"]

    for target in lag_targets:
        for lag in lag_days:
            frame[f"{target}_lag_{lag}"] = frame[target].shift(lag)
        prev = frame[target].shift(1)
        for window in rolling_windows:
            rolling = prev.rolling(window=window)
            frame[f"{target}_roll_mean_{window}"] = rolling.mean()
            frame[f"{target}_roll_std_{window}"] = rolling.std()

    for col in WEATHER_COLUMNS:
        for lag in [1, 2, 7]:
            frame[f"{col}_lag_{lag}"] = frame[col].shift(lag)
        frame[f"{col}_roll_mean_3"] = frame[col].shift(1).rolling(3).mean()
        frame[f"{col}_roll_mean_7"] = frame[col].shift(1).rolling(7).mean()

    # Defragment before adding final targets to avoid repeated pandas block splits.
    frame = frame.copy()

    target_next_cols: List[str] = []
    for target in TARGET_COLUMNS:
        next_col = f"{target}_j1"
        frame[next_col] = frame[target].shift(-1)
        frame[f"{target}_baseline_weekday"] = frame[target].shift(6)
        target_next_cols.append(next_col)

    feature_cols = [c for c in frame.columns if c not in ["date"] + target_next_cols]
    model_frame = frame.dropna(subset=feature_cols + target_next_cols).copy()
    return model_frame, feature_cols, target_next_cols


def split_train_test(frame: pd.DataFrame, eval_days: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    if eval_days < 30:
        raise ValueError("eval_days must be >= 30.")

    last_date = frame["date"].max()
    cutoff = last_date - pd.Timedelta(days=eval_days)
    train = frame[frame["date"] <= cutoff].copy()
    test = frame[frame["date"] > cutoff].copy()

    if len(train) < 365 or len(test) < 90:
        split_idx = int(len(frame) * 0.8)
        train = frame.iloc[:split_idx].copy()
        test = frame.iloc[split_idx:].copy()
        cutoff = train["date"].max()

    if train.empty or test.empty:
        raise ValueError("Unable to create non-empty temporal train/test split.")

    return train, test, cutoff


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    err = y_pred - y_true
    abs_err = np.abs(err)
    mae = float(abs_err.mean())
    rmse = float(math.sqrt(np.mean(err ** 2)))
    bias = float(err.mean())

    total_true = float(y_true.sum())
    total_pred = float(y_pred.sum())
    wape = float(abs_err.sum() / max(total_true, 1.0))

    mask_non_zero = y_true > 0
    mae_non_zero = float(abs_err[mask_non_zero].mean()) if mask_non_zero.any() else 0.0

    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "bias": round(bias, 4),
        "wape": round(wape, 4),
        "mae_non_zero": round(mae_non_zero, 4),
        "sum_true": round(total_true, 4),
        "sum_pred": round(total_pred, 4),
    }


def _target_sample_weights(y: np.ndarray, target_label: str) -> np.ndarray:
    event_boost = {
        "leger": 1.0,
        "grave": 4.0,
        "mortel": 10.0,
    }.get(target_label, 1.0)
    magnitude_boost = {
        "leger": 0.15,
        "grave": 0.60,
        "mortel": 1.20,
    }.get(target_label, 0.15)

    weights = np.ones_like(y, dtype=float)
    weights += event_boost * (y > 0).astype(float)
    weights += magnitude_boost * np.sqrt(np.clip(y, 0.0, None))
    return weights


def _positive_class_weight(target_label: str) -> float:
    return {
        "leger": 2.0,
        "grave": 8.0,
        "mortel": 25.0,
    }.get(target_label, 2.0)


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
        raise ValueError("Unable to build inner validation split for tuning.")

    return inner_train, inner_valid


def _build_validation_folds(
    train: pd.DataFrame,
    inner_eval_days: int,
    n_folds: int = 3,
) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    folds: List[Tuple[pd.DataFrame, pd.DataFrame]] = []
    eval_days = max(60, int(inner_eval_days))
    max_date = train["date"].max()

    for fold_idx in range(n_folds):
        valid_end = max_date - pd.Timedelta(days=fold_idx * eval_days)
        valid_start = valid_end - pd.Timedelta(days=eval_days - 1)
        train_end = valid_start - pd.Timedelta(days=1)

        fold_train = train[train["date"] <= train_end].copy()
        fold_valid = train[(train["date"] >= valid_start) & (train["date"] <= valid_end)].copy()

        if len(fold_train) < 365 or len(fold_valid) < 60:
            continue
        folds.append((fold_train, fold_valid))

    if folds:
        return folds

    # Fallback: single split when not enough data for multiple folds.
    single_train, single_valid = _inner_temporal_split(train, inner_eval_days=inner_eval_days)
    return [(single_train, single_valid)]


def _candidate_specs(target_label: str) -> List[Dict[str, Any]]:
    target_base = f"y_{target_label}"
    poisson_base = {
        "type": "single",
        "loss": "poisson",
        "params": {
            "learning_rate": 0.05,
            "max_depth": 6,
            "max_iter": 500,
            "min_samples_leaf": 20,
            "l2_regularization": 0.1,
        },
    }
    poisson_slow = {
        "type": "single",
        "loss": "poisson",
        "params": {
            "learning_rate": 0.03,
            "max_depth": 8,
            "max_iter": 900,
            "min_samples_leaf": 15,
            "l2_regularization": 0.2,
        },
    }
    absolute_base = {
        "type": "single",
        "loss": "absolute_error",
        "params": {
            "learning_rate": 0.05,
            "max_depth": 6,
            "max_iter": 500,
            "min_samples_leaf": 20,
            "l2_regularization": 0.1,
        },
    }
    two_stage = {
        "type": "two_stage",
        "classifier_params": {
            "learning_rate": 0.05,
            "max_depth": 4,
            "max_iter": 400,
            "min_samples_leaf": 20,
            "l2_regularization": 0.05,
        },
        "regressor_params": {
            "learning_rate": 0.04,
            "max_depth": 5,
            "max_iter": 500,
            "min_samples_leaf": 12,
            "l2_regularization": 0.15,
        },
    }
    baseline_rolling7 = {
        "name": "baseline_rolling7",
        "type": "feature_column",
        "feature_name": f"{target_base}_roll_mean_7",
    }
    baseline_weekday = {
        "name": "baseline_weekday",
        "type": "feature_column",
        "feature_name": f"{target_base}_baseline_weekday",
    }

    if target_label == "leger":
        return [
            {"name": "poisson_base", **poisson_base},
            {"name": "poisson_slow", **poisson_slow},
            {"name": "absolute_base", **absolute_base},
            baseline_rolling7,
            baseline_weekday,
        ]
    if target_label == "grave":
        return [
            baseline_rolling7,
            baseline_weekday,
            {"name": "absolute_base", **absolute_base},
            {"name": "poisson_base", **poisson_base},
            {"name": "two_stage_sparse", **two_stage},
        ]
    return [
        baseline_weekday,
        {"name": "two_stage_sparse", **two_stage},
        {"name": "poisson_base", **poisson_base},
        {"name": "absolute_base", **absolute_base},
        baseline_rolling7,
    ]


def _fit_candidate_model(
    spec: Dict[str, Any],
    target_label: str,
    X_train: np.ndarray,
    y_train: np.ndarray,
    random_seed: int,
    regressor_cls: Any,
    classifier_cls: Any,
    feature_cols: List[str],
) -> Any:
    if spec["type"] == "feature_column":
        feature_name = spec["feature_name"]
        if feature_name not in feature_cols:
            raise ValueError(f"Feature '{feature_name}' not found for baseline candidate.")
        feature_index = feature_cols.index(feature_name)
        fallback_value = float(np.mean(y_train)) if len(y_train) else 0.0
        return FeatureColumnModel(feature_index=feature_index, fallback_value=fallback_value)

    if spec["type"] == "single":
        model = regressor_cls(
            loss=spec["loss"],
            random_state=random_seed,
            **spec["params"],
        )
        model.fit(X_train, y_train, sample_weight=_target_sample_weights(y_train, target_label))
        return model

    y_event = (y_train > 0).astype(int)
    if int(y_event.min()) == int(y_event.max()):
        classifier = ConstantEventClassifier(float(y_event.mean()))
    else:
        classifier = classifier_cls(
            loss="log_loss",
            random_state=random_seed,
            **spec["classifier_params"],
        )
        class_weights = np.where(y_event == 1, _positive_class_weight(target_label), 1.0).astype(float)
        classifier.fit(X_train, y_event, sample_weight=class_weights)

    positive_mask = y_event == 1
    if int(positive_mask.sum()) >= max(20, int(0.02 * len(y_train))):
        regressor = regressor_cls(
            loss="poisson",
            random_state=random_seed + 1,
            **spec["regressor_params"],
        )
        X_positive = X_train[positive_mask]
        y_positive = y_train[positive_mask]
        regressor.fit(
            X_positive,
            y_positive,
            sample_weight=_target_sample_weights(y_positive, target_label),
        )
        positive_default = float(np.mean(y_positive))
    else:
        regressor = None
        positive_default = float(np.mean(y_train[positive_mask])) if positive_mask.any() else 0.0

    return TwoStageCountModel(
        classifier=classifier,
        regressor=regressor,
        positive_default=positive_default,
    )


def train_models(
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_cols: List[str],
    target_next_cols: List[str],
    random_state: int,
    inner_eval_days: int,
) -> Tuple[Dict[str, Any], Dict[str, Any], pd.DataFrame]:
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
    except Exception as exc:  # pragma: no cover - explicit runtime guard
        raise RuntimeError(
            "scikit-learn is required for training. Install it with `pip install scikit-learn`."
        ) from exc

    models: Dict[str, Any] = {}
    metrics: Dict[str, Any] = {}
    predictions = pd.DataFrame({"date": test["date"].values})

    X_train_full = train[feature_cols].to_numpy(dtype=float)
    X_test = test[feature_cols].to_numpy(dtype=float)

    for target_idx, target_next in enumerate(target_next_cols):
        target_label = target_next.replace("y_", "").replace("_j1", "")
        target_now = target_next.replace("_j1", "")

        y_train_full = train[target_next].to_numpy(dtype=float)
        y_test = test[target_next].to_numpy(dtype=float)

        validation_folds = _build_validation_folds(
            train=train,
            inner_eval_days=inner_eval_days,
            n_folds=3,
        )

        leaderboard: List[Dict[str, Any]] = []
        candidate_specs = _candidate_specs(target_label)

        for idx, spec in enumerate(candidate_specs):
            candidate_seed = random_state + (target_idx * 100) + idx
            try:
                fold_maes: List[float] = []
                fold_rmses: List[float] = []
                fold_rows: List[int] = []

                for fold_number, (fold_train_df, fold_valid_df) in enumerate(validation_folds):
                    X_fold_train = fold_train_df[feature_cols].to_numpy(dtype=float)
                    y_fold_train = fold_train_df[target_next].to_numpy(dtype=float)
                    X_fold_valid = fold_valid_df[feature_cols].to_numpy(dtype=float)
                    y_fold_valid = fold_valid_df[target_next].to_numpy(dtype=float)

                    fold_model = _fit_candidate_model(
                        spec=spec,
                        target_label=target_label,
                        X_train=X_fold_train,
                        y_train=y_fold_train,
                        random_seed=candidate_seed + fold_number,
                        regressor_cls=HistGradientBoostingRegressor,
                        classifier_cls=HistGradientBoostingClassifier,
                        feature_cols=feature_cols,
                    )
                    valid_pred = np.clip(fold_model.predict(X_fold_valid), 0.0, None)
                    fold_metrics = _metrics(y_fold_valid, valid_pred)
                    fold_maes.append(float(fold_metrics["mae"]))
                    fold_rmses.append(float(fold_metrics["rmse"]))
                    fold_rows.append(int(len(fold_valid_df)))

                mean_mae = float(np.mean(fold_maes))
                mean_rmse = float(np.mean(fold_rmses))
                leaderboard.append(
                    {
                        "name": spec["name"],
                        "mae": round(mean_mae, 4),
                        "rmse": round(mean_rmse, 4),
                        "fold_mae": [round(v, 4) for v in fold_maes],
                        "fold_rmse": [round(v, 4) for v in fold_rmses],
                        "fold_rows": fold_rows,
                        "status": "ok",
                    }
                )
            except Exception as candidate_error:
                leaderboard.append(
                    {
                        "name": spec["name"],
                        "status": "error",
                        "error": str(candidate_error)[:240],
                    }
                )

        successful_candidates = [row for row in leaderboard if row.get("status") == "ok"]
        if not successful_candidates:
            raise RuntimeError(f"No valid candidate model for target '{target_label}'.")

        mae_tolerance = {
            "leger": 0.005,
            "grave": 0.015,
            "mortel": 0.01,
        }.get(target_label, 0.005)
        best_mae = min(row["mae"] for row in successful_candidates)
        close_candidates = [
            row
            for row in successful_candidates
            if row["mae"] <= (best_mae + mae_tolerance)
        ]
        selected_row = min(
            close_candidates,
            key=lambda row: (
                row["rmse"],
                row["mae"],
                row["name"],
            ),
        )
        spec_by_name = {spec["name"]: spec for spec in candidate_specs}
        best_spec = spec_by_name[selected_row["name"]]

        final_seed = random_state + (target_idx * 1000) + 999
        final_model = _fit_candidate_model(
            spec=best_spec,
            target_label=target_label,
            X_train=X_train_full,
            y_train=y_train_full,
            random_seed=final_seed,
            regressor_cls=HistGradientBoostingRegressor,
            classifier_cls=HistGradientBoostingClassifier,
            feature_cols=feature_cols,
        )

        y_pred = np.clip(final_model.predict(X_test), 0.0, None)
        baseline_weekday_col = f"{target_now}_baseline_weekday"
        baseline_roll7_col = f"{target_now}_roll_mean_7"

        baseline_weekday = test[baseline_weekday_col].fillna(float(np.mean(y_train_full))).to_numpy(dtype=float)
        baseline_weekday = np.clip(baseline_weekday, 0.0, None)

        baseline_roll7 = test[baseline_roll7_col].fillna(float(np.mean(y_train_full))).to_numpy(dtype=float)
        baseline_roll7 = np.clip(baseline_roll7, 0.0, None)

        leaderboard_sorted = sorted(
            leaderboard,
            key=lambda item: (
                item.get("mae", float("inf")),
                item.get("rmse", float("inf")),
                item.get("name", ""),
            ),
        )
        validation_windows = [
            {
                "start": str(fold_valid["date"].min().date()),
                "end": str(fold_valid["date"].max().date()),
                "rows": int(len(fold_valid)),
            }
            for _, fold_valid in validation_folds
        ]
        metrics[target_label] = {
            "selected_candidate": best_spec["name"],
            "validation_windows": validation_windows,
            "validation_candidates": leaderboard_sorted,
            "model": _metrics(y_test, y_pred),
            "baseline_weekday": _metrics(y_test, baseline_weekday),
            "baseline_rolling7": _metrics(y_test, baseline_roll7),
        }

        predictions[f"{target_label}_true"] = y_test
        predictions[f"{target_label}_pred"] = y_pred
        predictions[f"{target_label}_baseline_weekday"] = baseline_weekday
        predictions[f"{target_label}_baseline_rolling7"] = baseline_roll7
        predictions[f"{target_label}_model_name"] = best_spec["name"]

        models[target_label] = final_model

    return models, metrics, predictions


def _serialize_config(config: TrainingConfig) -> Dict[str, Any]:
    return {
        "db_path": str(config.db_path),
        "output_dir": str(config.output_dir),
        "climate_identifier": config.climate_identifier,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "eval_days": config.eval_days,
        "inner_eval_days": config.inner_eval_days,
        "random_state": config.random_state,
        "weather_csv": str(config.weather_csv) if config.weather_csv else None,
    }


def save_artifacts(
    output_dir: Path,
    config: TrainingConfig,
    feature_cols: List[str],
    target_next_cols: List[str],
    frame: pd.DataFrame,
    train: pd.DataFrame,
    test: pd.DataFrame,
    models: Dict[str, Any],
    metrics: Dict[str, Any],
    predictions: pd.DataFrame,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        import joblib
    except Exception as exc:  # pragma: no cover - explicit runtime guard
        raise RuntimeError(
            "joblib is required for artifact export. Install it with `pip install joblib`."
        ) from exc

    for target_label, model in models.items():
        joblib.dump(model, output_dir / f"model_{target_label}.joblib")

    (output_dir / "feature_columns.json").write_text(
        json.dumps(feature_cols, indent=2),
        encoding="utf-8",
    )

    latest_features = frame[["date"] + feature_cols].tail(1).copy()
    latest_features.to_csv(output_dir / "latest_features_row.csv", index=False)
    predictions.to_csv(output_dir / "test_predictions.csv", index=False)
    frame.to_csv(output_dir / "daily_training_frame.csv", index=False)

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
        "targets": target_next_cols,
        "metrics": metrics,
    }
    (output_dir / "training_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    return summary


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
            raise ValueError("Invalid start_date format. Expected YYYY-MM-DD.")
        min_date = max(min_date, parsed_start.normalize())
    if end_date:
        parsed_end = pd.to_datetime(end_date, errors="coerce")
        if pd.isna(parsed_end):
            raise ValueError("Invalid end_date format. Expected YYYY-MM-DD.")
        max_date = min(max_date, parsed_end.normalize())

    if min_date > max_date:
        raise ValueError("Requested start_date is after end_date.")
    return min_date, max_date


def run_training(config: TrainingConfig) -> Dict[str, Any]:
    targets = load_collision_targets(db_path=config.db_path)
    start_date, end_date = _resolve_start_end(targets, config.start_date, config.end_date)

    targets = targets[(targets["date"] >= start_date) & (targets["date"] <= end_date)].copy()
    if targets.empty:
        raise ValueError("No target rows available after date filtering.")

    if config.weather_csv:
        weather = _load_weather_from_csv(config.weather_csv)
    else:
        weather = fetch_geomet_daily_weather(
            start_date=start_date,
            end_date=end_date,
            climate_identifier=config.climate_identifier,
        )

    frame, feature_cols, target_next_cols = build_feature_frame(targets=targets, weather=weather)
    train, test, _ = split_train_test(frame=frame, eval_days=config.eval_days)
    models, metrics, predictions = train_models(
        train=train,
        test=test,
        feature_cols=feature_cols,
        target_next_cols=target_next_cols,
        random_state=config.random_state,
        inner_eval_days=config.inner_eval_days,
    )

    summary = save_artifacts(
        output_dir=config.output_dir,
        config=config,
        feature_cols=feature_cols,
        target_next_cols=target_next_cols,
        frame=frame,
        train=train,
        test=test,
        models=models,
        metrics=metrics,
        predictions=predictions,
    )
    return summary


def parse_args() -> TrainingConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Train J+1 collision count models (leger, grave, mortel) "
            "from mobility.db and GeoMet daily weather."
        )
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Path to SQLite DB (default: data/db/mobility.db).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for model artifacts.",
    )
    parser.add_argument(
        "--climate-identifier",
        default=DEFAULT_CLIMATE_IDENTIFIER,
        help="GeoMet climate station identifier (default: 7025251).",
    )
    parser.add_argument("--start-date", default=None, help="Training start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Training end date YYYY-MM-DD.")
    parser.add_argument(
        "--eval-days",
        type=int,
        default=365,
        help="Size of the temporal test window in days (default: 365).",
    )
    parser.add_argument(
        "--inner-eval-days",
        type=int,
        default=240,
        help="Validation window size in days used for model selection (default: 240).",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random state used by sklearn estimators.",
    )
    parser.add_argument(
        "--weather-csv",
        default=None,
        help=(
            "Optional local weather CSV with columns: "
            "date,mean_temp_c,min_temp_c,max_temp_c,total_precip_mm,total_snow_cm."
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
    print("Model selection + test metrics (MAE / RMSE):")
    for target in ["leger", "grave", "mortel"]:
        target_metrics = summary["metrics"][target]
        selected = target_metrics.get("selected_candidate", "n/a")
        model = target_metrics["model"]
        baseline = target_metrics["baseline_weekday"]
        baseline_roll = target_metrics.get("baseline_rolling7")
        print(
            f"  {target:7s} [{selected}] "
            f"model {model['mae']:.3f}/{model['rmse']:.3f} | "
            f"weekday {baseline['mae']:.3f}/{baseline['rmse']:.3f}"
        )
        if baseline_roll:
            print(
                f"           rolling7 {baseline_roll['mae']:.3f}/{baseline_roll['rmse']:.3f}"
            )


if __name__ == "__main__":
    cfg = parse_args()
    result = run_training(cfg)
    _print_summary(result)
