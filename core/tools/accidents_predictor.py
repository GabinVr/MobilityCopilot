import joblib
import json
import numpy as np
import pandas as pd
from datetime import datetime
from langchain_core.tools import tool
import os


try:
    predictive_model = joblib.load("data/model/model_total.joblib")

except Exception as e:
    predictive_model = None
    print(f"Attention, modèle non chargé : {e}")

@tool
def accidents_predictor_tool(
    target_date: str, 
    mean_temp_c: float, 
    min_temp_c: float, 
    max_temp_c: float, 
    total_precip_mm: float, 
    total_snow_cm: float
) -> str:
    """
    Use this tool to PREDICT the number of collisions for a specific future date based on weather forecast.
    target_date must be 'YYYY-MM-DD'.
    """
    if predictive_model is None:
        return json.dumps({"error": "Modèle prédictif non disponible."})

    dt = pd.to_datetime(target_date)
    dow = dt.dayofweek
    day_of_year = dt.dayofyear
    month = dt.month
    
    features = {
        "mean_temp_c": mean_temp_c,
        "min_temp_c": min_temp_c,
        "max_temp_c": max_temp_c,
        "total_precip_mm": total_precip_mm,
        "total_snow_cm": total_snow_cm,
        "dow": dow,
        "month": month,
        "quarter": dt.quarter,
        "weekofyear": dt.isocalendar().week,
        "day_of_year": day_of_year,
        "is_weekend": 1 if dow >= 5 else 0,
        "is_winter": 1 if month in [12, 1, 2, 3] else 0,
        "is_summer": 1 if month in [6, 7, 8] else 0,
        "dow_sin": np.sin(2 * np.pi * dow / 7),
        "dow_cos": np.cos(2 * np.pi * dow / 7),
        "doy_sin": np.sin(2 * np.pi * day_of_year / 365.25),
        "doy_cos": np.cos(2 * np.pi * day_of_year / 365.25),
        "month_sin": np.sin(2 * np.pi * month / 12),
        "month_cos": np.cos(2 * np.pi * month / 12),
        "freeze_day": 1 if max_temp_c <= 0 else 0,
        "rain_day": 1 if (total_precip_mm > 0 and total_snow_cm == 0) else 0,
        "snow_day": 1 if total_snow_cm > 0 else 0,
        "heavy_precip_day": 1 if total_precip_mm > 15 else 0,
        "heavy_snow_day": 1 if total_snow_cm > 10 else 0,
        "temp_range_c": max_temp_c - min_temp_c,
        "temp_x_precip": mean_temp_c * total_precip_mm,
    }

    lag_prefixes = ["mean_temp_c", "min_temp_c", "max_temp_c", "total_precip_mm", "total_snow_cm"]
    for prefix in lag_prefixes:
        base_val = features[prefix]
        for i in range(1, 5):
            features[f"{prefix}_lag_{i}"] = base_val
        for i in [2, 3, 4]:
            features[f"{prefix}_roll_mean_{i}"] = base_val
            features[f"{prefix}_roll_max_{i}"] = base_val
        features[f"{prefix}_delta_1d"] = 0.0

    expected_columns = [
        "mean_temp_c", "min_temp_c", "max_temp_c", "total_precip_mm", "total_snow_cm", 
        "dow", "month", "quarter", "weekofyear", "day_of_year", "is_weekend", "is_winter", 
        "is_summer", "dow_sin", "dow_cos", "doy_sin", "doy_cos", "month_sin", "month_cos", 
        "freeze_day", "rain_day", "snow_day", "heavy_precip_day", "heavy_snow_day", 
        "temp_range_c", "temp_x_precip", "mean_temp_c_lag_1", "mean_temp_c_lag_2", 
        "mean_temp_c_lag_3", "mean_temp_c_lag_4", "mean_temp_c_roll_mean_2", "mean_temp_c_roll_max_2", 
        "mean_temp_c_roll_mean_3", "mean_temp_c_roll_max_3", "mean_temp_c_roll_mean_4", 
        "mean_temp_c_roll_max_4", "mean_temp_c_delta_1d", "min_temp_c_lag_1", "min_temp_c_lag_2", 
        "min_temp_c_lag_3", "min_temp_c_lag_4", "min_temp_c_roll_mean_2", "min_temp_c_roll_max_2", 
        "min_temp_c_roll_mean_3", "min_temp_c_roll_max_3", "min_temp_c_roll_mean_4", "min_temp_c_roll_max_4", 
        "min_temp_c_delta_1d", "max_temp_c_lag_1", "max_temp_c_lag_2", "max_temp_c_lag_3", 
        "max_temp_c_lag_4", "max_temp_c_roll_mean_2", "max_temp_c_roll_max_2", "max_temp_c_roll_mean_3", 
        "max_temp_c_roll_max_3", "max_temp_c_roll_mean_4", "max_temp_c_roll_max_4", "max_temp_c_delta_1d", 
        "total_precip_mm_lag_1", "total_precip_mm_lag_2", "total_precip_mm_lag_3", "total_precip_mm_lag_4", 
        "total_precip_mm_roll_mean_2", "total_precip_mm_roll_max_2", "total_precip_mm_roll_mean_3", 
        "total_precip_mm_roll_max_3", "total_precip_mm_roll_mean_4", "total_precip_mm_roll_max_4", 
        "total_precip_mm_delta_1d", "total_snow_cm_lag_1", "total_snow_cm_lag_2", "total_snow_cm_lag_3", 
        "total_snow_cm_lag_4", "total_snow_cm_roll_mean_2", "total_snow_cm_roll_max_2", "total_snow_cm_roll_mean_3", 
        "total_snow_cm_roll_max_3", "total_snow_cm_roll_mean_4", "total_snow_cm_roll_max_4", "total_snow_cm_delta_1d"
    ]
    
    df_input = pd.DataFrame([features])[expected_columns]
    
    try:
        prediction = predictive_model.predict(df_input)
        predicted_number = int(prediction[0])
        return json.dumps({
            "predicted_collisions": predicted_number,
            "status": "success",
            "context": f"Prediction made using HistGradientBoostingRegressor with 82 features for {target_date}."
        })
    except Exception as e:
        return json.dumps({"error": f"Erreur de prédiction: {str(e)}"})