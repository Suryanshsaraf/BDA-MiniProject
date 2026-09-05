"""
Module 6C — Model Evaluation: Compute Metrics, Confusion Matrix, and Feature Rankings.

This script:
1. Loads the trained model artifact from storage.
2. Validates predictions against the test split.
3. Prints Confusion Matrix and Classification Metrics (Accuracy, AUC, Precision, Recall, F1).
4. Prints Feature Importance ranking table.
"""

import sys
import os
import json
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    ANALYSIS_RESULTS_DIR,
    LOCAL_MODELS_DIR,
    setup_logging
)

logger = setup_logging("ModelEvaluation")


def run_evaluation():
    """Load model evaluation results and print detailed report."""
    logger.info("=== Starting Module 6C: Model Evaluation ===")
    eval_file = ANALYSIS_RESULTS_DIR / "model_evaluation.json"

    if not eval_file.exists():
        logger.warning(f"{eval_file} not found. Running training first...")
        from ml.random_forest import run_training
        run_training()

    with open(eval_file, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    cm = metrics["confusion_matrix"]
    total = cm["true_positive"] + cm["false_positive"] + cm["true_negative"] + cm["false_negative"]

    print("\n" + "=" * 65)
    print("        CRIME SEVERITY PREDICTION: MODEL EVALUATION REPORT")
    print("=" * 65)
    print(f"  Model Architecture  : Random Forest Classifier (Spark MLlib / Ensemble)")
    print(f"  Total Test Samples  : {total}")
    print(f"  Accuracy            : {metrics['accuracy'] * 100:.2f}%")
    print(f"  AUC-ROC             : {metrics['auc_roc']:.4f}")
    print(f"  Precision           : {metrics['precision'] * 100:.2f}%")
    print(f"  Recall              : {metrics['recall'] * 100:.2f}%")
    print(f"  F1-Score            : {metrics['f1_score']:.4f}")
    print("-" * 65)
    print("  CONFUSION MATRIX:")
    print(f"    {'':<20} | Predicted Negative | Predicted Positive |")
    print(f"    {'Actual Negative':<20} | {cm['true_negative']:<18} | {cm['false_positive']:<18} |")
    print(f"    {'Actual Positive':<20} | {cm['false_negative']:<18} | {cm['true_positive']:<18} |")
    print("-" * 65)
    print("  FEATURE IMPORTANCE RANKING:")
    for rank, item in enumerate(metrics["feature_importances"], start=1):
        feat = item["feature"]
        imp = item["importance"]
        bar = "█" * int(imp * 40)
        print(f"    {rank}. {feat:<25} : {imp:.4f} {bar}")
    print("=" * 65 + "\n")

    logger.info("=== Model Evaluation Complete ===")
    return True


if __name__ == "__main__":
    run_evaluation()
