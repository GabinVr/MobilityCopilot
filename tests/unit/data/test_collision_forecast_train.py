import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from data.collision_forecast_train import TrainingConfig, run_training


def _seed_collisions(db_path: Path, start: str, end: str) -> None:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE collisions_routieres (
            DT_ACCDN TEXT,
            GRAVITE TEXT
        )
        """
    )

    rows = []
    for day in pd.date_range(start=start, end=end, freq="D"):
        date_value = day.strftime("%Y/%m/%d")

        n_leger = 2 + int(day.dayofweek in (4, 5)) + int(day.month in (12, 1, 2))
        n_grave = int(day.dayofweek in (5, 6))
        n_mortel = int(day.dayofyear % 45 == 0)

        rows.extend([(date_value, "Léger")] * n_leger)
        rows.extend([(date_value, "Grave")] * n_grave)
        rows.extend([(date_value, "Mortel")] * n_mortel)

    cur.executemany(
        "INSERT INTO collisions_routieres (DT_ACCDN, GRAVITE) VALUES (?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _build_weather_csv(path: Path, start: str, end: str) -> None:
    dates = pd.date_range(start=start, end=end, freq="D")
    index = np.arange(len(dates), dtype=float)
    weather = pd.DataFrame(
        {
            "date": dates,
            "mean_temp_c": 8.0 + 14.0 * np.sin(index / 40.0),
            "min_temp_c": -2.0 + 10.0 * np.sin(index / 45.0),
            "max_temp_c": 16.0 + 10.0 * np.sin(index / 35.0),
            "total_precip_mm": index % 7.0,
            "total_snow_cm": ((index + 3.0) % 5.0 == 0).astype(float),
        }
    )
    weather.to_csv(path, index=False)


def test_run_training_offline_with_weather_csv(tmp_path):
    pytest.importorskip("sklearn")

    db_path = tmp_path / "mobility.db"
    weather_csv = tmp_path / "weather.csv"
    output_dir = tmp_path / "model_output"

    start = "2018-01-01"
    end = "2020-12-31"

    _seed_collisions(db_path=db_path, start=start, end=end)
    _build_weather_csv(path=weather_csv, start=start, end=end)

    config = TrainingConfig(
        db_path=db_path,
        output_dir=output_dir,
        weather_csv=weather_csv,
        eval_days=180,
        random_state=7,
    )
    summary = run_training(config)

    assert summary["data"]["n_rows_train"] > 0
    assert summary["data"]["n_rows_test"] > 0
    assert set(summary["metrics"].keys()) == {"leger", "grave", "mortel"}

    assert (output_dir / "model_leger.joblib").exists()
    assert (output_dir / "model_grave.joblib").exists()
    assert (output_dir / "model_mortel.joblib").exists()
    assert (output_dir / "feature_columns.json").exists()
    assert (output_dir / "training_summary.json").exists()
