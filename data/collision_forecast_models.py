from __future__ import annotations

from typing import Dict, Tuple

import numpy as np


class ConstantModel:
    def __init__(self, value: float):
        self.value = float(max(0.0, value))

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.full(shape=len(X), fill_value=self.value, dtype=float)


class CalendarBaselineModel:
    def __init__(
        self,
        dow_feature_index: int,
        month_feature_index: int,
        lookup: Dict[Tuple[int, int], float],
        fallback: float,
    ):
        self.dow_feature_index = int(dow_feature_index)
        self.month_feature_index = int(month_feature_index)
        self.lookup = {(int(k[0]), int(k[1])): float(v) for k, v in lookup.items()}
        self.fallback = float(max(0.0, fallback))

    def predict(self, X: np.ndarray) -> np.ndarray:
        dows = np.rint(np.asarray(X[:, self.dow_feature_index], dtype=float)).astype(int)
        months = np.rint(np.asarray(X[:, self.month_feature_index], dtype=float)).astype(int)
        out = np.empty(shape=len(dows), dtype=float)
        for i, (dow, month) in enumerate(zip(dows, months)):
            out[i] = self.lookup.get((int(dow), int(month)), self.fallback)
        return np.clip(out, 0.0, None)


class WeightedBlendModel:
    def __init__(self, primary_model, secondary_model, alpha_primary: float):
        self.primary_model = primary_model
        self.secondary_model = secondary_model
        self.alpha_primary = float(np.clip(alpha_primary, 0.0, 1.0))

    def predict(self, X: np.ndarray) -> np.ndarray:
        primary = np.asarray(self.primary_model.predict(X), dtype=float)
        secondary = np.asarray(self.secondary_model.predict(X), dtype=float)
        blended = self.alpha_primary * primary + (1.0 - self.alpha_primary) * secondary
        return np.clip(blended, 0.0, None)
