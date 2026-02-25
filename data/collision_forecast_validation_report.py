from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


DEFAULT_MODELS_ROOT = Path("data/models")
TARGETS = ["leger", "grave", "mortel"]
EVENT_TARGETS = {"grave", "mortel"}


def _safe_div(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


def _round4(value: float) -> float:
    return round(float(value), 4)


def _regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    err = y_pred - y_true
    abs_err = np.abs(err)

    mae = abs_err.mean()
    rmse = math.sqrt(np.mean(err ** 2))
    bias = err.mean()
    total_true = float(y_true.sum())
    total_pred = float(y_pred.sum())
    wape = _safe_div(float(abs_err.sum()), max(total_true, 1.0))

    non_zero_mask = y_true > 0
    mae_non_zero = abs_err[non_zero_mask].mean() if non_zero_mask.any() else 0.0

    sse = float(np.sum(err ** 2))
    sst = float(np.sum((y_true - y_true.mean()) ** 2))
    r2 = 1.0 - _safe_div(sse, sst) if sst > 0 else 0.0

    mape_non_zero = (
        float(np.mean(abs_err[non_zero_mask] / np.maximum(y_true[non_zero_mask], 1e-9)))
        if non_zero_mask.any()
        else 0.0
    )

    return {
        "mae": _round4(mae),
        "rmse": _round4(rmse),
        "bias": _round4(bias),
        "wape": _round4(wape),
        "r2": _round4(r2),
        "mape_non_zero": _round4(mape_non_zero),
        "mae_non_zero": _round4(mae_non_zero),
        "sum_true": _round4(total_true),
        "sum_pred": _round4(total_pred),
    }


def _event_metrics(y_true: np.ndarray, y_pred: np.ndarray, threshold: float = 0.5) -> Dict[str, float]:
    y_true_bin = (y_true > 0).astype(int)
    y_pred_bin = (y_pred >= threshold).astype(int)

    tp = int(np.sum((y_true_bin == 1) & (y_pred_bin == 1)))
    fp = int(np.sum((y_true_bin == 0) & (y_pred_bin == 1)))
    tn = int(np.sum((y_true_bin == 0) & (y_pred_bin == 0)))
    fn = int(np.sum((y_true_bin == 1) & (y_pred_bin == 0)))

    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    specificity = _safe_div(tn, tn + fp)
    accuracy = _safe_div(tp + tn, tp + tn + fp + fn)
    f1 = _safe_div(2 * precision * recall, precision + recall)
    balanced_accuracy = 0.5 * (recall + specificity)

    return {
        "threshold": _round4(threshold),
        "precision": _round4(precision),
        "recall": _round4(recall),
        "f1": _round4(f1),
        "specificity": _round4(specificity),
        "balanced_accuracy": _round4(balanced_accuracy),
        "accuracy": _round4(accuracy),
        "event_rate_true": _round4(float(y_true_bin.mean())),
        "event_rate_pred": _round4(float(y_pred_bin.mean())),
    }


def _recall_at_top_k(y_true: np.ndarray, y_pred: np.ndarray, k_ratio: float = 0.1) -> Dict[str, float]:
    n = len(y_true)
    if n == 0:
        return {"k": 0, "k_ratio": _round4(k_ratio), "recall": 0.0}

    k = max(1, int(round(n * k_ratio)))
    true_top_idx = np.argpartition(y_true, -k)[-k:]
    pred_top_idx = np.argpartition(y_pred, -k)[-k:]
    overlap = len(set(true_top_idx.tolist()) & set(pred_top_idx.tolist()))
    return {
        "k": int(k),
        "k_ratio": _round4(k_ratio),
        "recall": _round4(_safe_div(overlap, k)),
    }


def _discover_model_dirs(root: Path) -> List[Path]:
    candidates: List[Path] = []
    if not root.exists():
        return candidates

    for path in sorted(root.iterdir()):
        if not path.is_dir():
            continue
        summary_path = path / "training_summary.json"
        pred_path = path / "test_predictions.csv"
        if summary_path.exists() and pred_path.exists() and path.name.startswith("collision_j1_"):
            candidates.append(path)
    return candidates


def _predictor_columns(df: pd.DataFrame, target: str) -> List[Tuple[str, str]]:
    ordered = [
        ("model", f"{target}_pred"),
        ("baseline_weekday", f"{target}_baseline_weekday"),
        ("baseline_rolling7", f"{target}_baseline_rolling7"),
        ("baseline_legacy", f"{target}_baseline"),
    ]
    return [(name, col) for name, col in ordered if col in df.columns]


def _validation_top(summary: Dict[str, Any], target: str, top_n: int = 3) -> List[Dict[str, Any]]:
    target_blob = ((summary.get("metrics") or {}).get(target) or {})
    rows = target_blob.get("validation_candidates") or []
    if not isinstance(rows, list):
        return []
    valid = [row for row in rows if isinstance(row, dict) and row.get("status") == "ok"]
    valid_sorted = sorted(valid, key=lambda row: (row.get("mae", float("inf")), row.get("rmse", float("inf"))))
    return [
        {
            "name": row.get("name"),
            "mae": row.get("mae"),
            "rmse": row.get("rmse"),
        }
        for row in valid_sorted[:top_n]
    ]


def build_report_for_model_dir(model_dir: Path, top_k_ratio: float) -> Dict[str, Any]:
    summary_path = model_dir / "training_summary.json"
    pred_path = model_dir / "test_predictions.csv"

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    preds = pd.read_csv(pred_path)

    target_reports: Dict[str, Any] = {}
    for target in TARGETS:
        true_col = f"{target}_true"
        if true_col not in preds.columns:
            continue

        y_true = preds[true_col].to_numpy(dtype=float)
        predictor_reports: Dict[str, Any] = {}

        for predictor_name, predictor_col in _predictor_columns(preds, target):
            y_pred = np.clip(preds[predictor_col].to_numpy(dtype=float), 0.0, None)
            row_report: Dict[str, Any] = {
                "regression": _regression_metrics(y_true, y_pred),
                "top_k_recall": _recall_at_top_k(y_true, y_pred, k_ratio=top_k_ratio),
            }
            if target in EVENT_TARGETS:
                row_report["event"] = _event_metrics(y_true, y_pred, threshold=0.5)
            predictor_reports[predictor_name] = row_report

        target_reports[target] = {
            "selected_candidate": ((summary.get("metrics") or {}).get(target) or {}).get("selected_candidate"),
            "validation_top": _validation_top(summary, target, top_n=3),
            "test_predictors": predictor_reports,
        }

    aggregate_wape = {}
    for predictor in ["model", "baseline_rolling7", "baseline_weekday", "baseline_legacy"]:
        predictor_wapes = []
        for target in TARGETS:
            target_blob = (target_reports.get(target) or {}).get("test_predictors") or {}
            if predictor in target_blob:
                predictor_wapes.append(target_blob[predictor]["regression"]["wape"])
        if predictor_wapes:
            aggregate_wape[predictor] = _round4(float(np.mean(predictor_wapes)))

    return {
        "model_dir": str(model_dir),
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "data": summary.get("data"),
        "config": summary.get("config"),
        "targets": target_reports,
        "aggregate_mean_wape": aggregate_wape,
    }


def _print_report(report: Dict[str, Any]) -> None:
    print("")
    print(f"=== {report['model_dir']} ===")
    data = report.get("data") or {}
    print(
        "Rows train/test: "
        f"{data.get('n_rows_train', 'n/a')}/{data.get('n_rows_test', 'n/a')} | "
        f"Dates: {data.get('date_min', 'n/a')} -> {data.get('date_max', 'n/a')}"
    )

    agg = report.get("aggregate_mean_wape") or {}
    if agg:
        ordered = sorted(agg.items(), key=lambda kv: kv[1])
        summary_text = " | ".join([f"{name}={value:.4f}" for name, value in ordered])
        print(f"Aggregate mean WAPE (lower is better): {summary_text}")

    for target in TARGETS:
        target_blob = (report.get("targets") or {}).get(target) or {}
        if not target_blob:
            continue

        selected = target_blob.get("selected_candidate") or "n/a"
        print(f"- Target {target}: selected={selected}")

        validation_top = target_blob.get("validation_top") or []
        if validation_top:
            top_text = " | ".join(
                [f"{row['name']} {row['mae']:.4f}/{row['rmse']:.4f}" for row in validation_top]
            )
            print(f"  validation top3 (MAE/RMSE): {top_text}")

        predictors = target_blob.get("test_predictors") or {}
        for predictor_name, scores in predictors.items():
            reg = scores.get("regression") or {}
            print(
                f"  test {predictor_name:17s} "
                f"MAE={reg.get('mae', 0):.4f} RMSE={reg.get('rmse', 0):.4f} "
                f"WAPE={reg.get('wape', 0):.4f} BIAS={reg.get('bias', 0):.4f}"
            )
            if "event" in scores:
                event = scores["event"]
                print(
                    f"       event@0.5 F1={event['f1']:.4f} Recall={event['recall']:.4f} "
                    f"Precision={event['precision']:.4f}"
                )
            topk = scores.get("top_k_recall") or {}
            print(
                f"       top{k_top(topk)} recall={topk.get('recall', 0):.4f}"
            )


def k_top(top_k_blob: Dict[str, Any]) -> str:
    k = int(top_k_blob.get("k", 0))
    ratio = float(top_k_blob.get("k_ratio", 0.0)) * 100.0
    return f"{k} ({ratio:.0f}%)"


def _write_flat_csv(report: Dict[str, Any], output_path: Path) -> None:
    rows: List[Dict[str, Any]] = []
    model_dir = report.get("model_dir")
    targets = report.get("targets") or {}
    for target_name, target_blob in targets.items():
        predictors = target_blob.get("test_predictors") or {}
        for predictor_name, predictor_blob in predictors.items():
            reg = predictor_blob.get("regression") or {}
            topk = predictor_blob.get("top_k_recall") or {}
            event = predictor_blob.get("event") or {}
            rows.append(
                {
                    "model_dir": model_dir,
                    "target": target_name,
                    "predictor": predictor_name,
                    "mae": reg.get("mae"),
                    "rmse": reg.get("rmse"),
                    "wape": reg.get("wape"),
                    "bias": reg.get("bias"),
                    "r2": reg.get("r2"),
                    "top_k_recall": topk.get("recall"),
                    "event_f1": event.get("f1"),
                    "event_recall": event.get("recall"),
                    "event_precision": event.get("precision"),
                }
            )
    pd.DataFrame(rows).to_csv(output_path, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate validation/test report for collision J+1 model artifacts."
    )
    parser.add_argument(
        "--models-root",
        default=str(DEFAULT_MODELS_ROOT),
        help="Root directory containing collision model folders (default: data/models).",
    )
    parser.add_argument(
        "--model-dirs",
        nargs="*",
        default=None,
        help="Optional explicit list of model directories. If omitted, auto-discovery is used.",
    )
    parser.add_argument(
        "--top-k-ratio",
        type=float,
        default=0.1,
        help="Fraction of top days used for peak recall (default: 0.1).",
    )
    parser.add_argument(
        "--write-json",
        action="store_true",
        help="Write validation_report.json in each model directory.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Write validation_test_scores.csv in each model directory.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    models_root = Path(args.models_root)
    top_k_ratio = float(args.top_k_ratio)

    if args.model_dirs:
        model_dirs = [Path(p) for p in args.model_dirs]
    else:
        model_dirs = _discover_model_dirs(models_root)

    if not model_dirs:
        raise SystemExit("No model directories found with training_summary.json + test_predictions.csv")

    reports: List[Dict[str, Any]] = []
    for model_dir in model_dirs:
        report = build_report_for_model_dir(model_dir, top_k_ratio=top_k_ratio)
        reports.append(report)
        _print_report(report)

        if args.write_json:
            json_path = model_dir / "validation_report.json"
            json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        if args.write_csv:
            csv_path = model_dir / "validation_test_scores.csv"
            _write_flat_csv(report, output_path=csv_path)

    print("")
    print("=== Global ranking (model predictor only, mean WAPE) ===")
    rows = []
    for report in reports:
        model_wape = (report.get("aggregate_mean_wape") or {}).get("model")
        if model_wape is not None:
            rows.append((report.get("model_dir"), float(model_wape)))
    if not rows:
        print("No 'model' predictor found in reports.")
        return
    for rank, (model_dir, score) in enumerate(sorted(rows, key=lambda x: x[1]), start=1):
        print(f"{rank}. {model_dir}: {score:.4f}")


if __name__ == "__main__":
    main()
