"""
Training script for the Instacart reorder prediction model.

Usage:
    python ml/train.py [--data data/train.csv] [--output models/]

Outputs:
    models/model.joblib   - trained RandomForestClassifier
    models/scaler.joblib  - fitted StandardScaler
"""

import argparse
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

FEATURES = [
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "add_to_cart_order",
    "department_id",
    "aisle_id",
]
TARGET = "reordered"


def load_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    missing = [c for c in FEATURES + [TARGET] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in dataset: {missing}")
    return df


def train(data_path: str, output_dir: str) -> dict:
    print(f"Loading data from {data_path} ...")
    df = load_data(data_path)
    print(f"  Rows: {len(df):,}  |  Reorder rate: {df[TARGET].mean():.2%}")

    X = df[FEATURES].values
    y = df[TARGET].values

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # ── Scale ─────────────────────────────────────────────────────
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    # ── Train ─────────────────────────────────────────────────────
    print("Training RandomForestClassifier ...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=8,
        min_samples_leaf=10,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train_scaled, y_train)

    # ── Evaluate ──────────────────────────────────────────────────
    y_pred = model.predict(X_val_scaled)
    y_prob = model.predict_proba(X_val_scaled)[:, 1]
    roc_auc = roc_auc_score(y_val, y_prob)
    print(f"\nValidation ROC-AUC: {roc_auc:.4f}")
    print(classification_report(y_val, y_pred, target_names=["Not Reordered", "Reordered"]))

    # ── Save artifacts ────────────────────────────────────────────
    os.makedirs(output_dir, exist_ok=True)
    model_path = os.path.join(output_dir, "model.joblib")
    scaler_path = os.path.join(output_dir, "scaler.joblib")
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)
    print(f"\nSaved model  → {model_path}")
    print(f"Saved scaler → {scaler_path}")

    return {"roc_auc": round(roc_auc, 4), "n_train": len(X_train), "n_val": len(X_val)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Instacart reorder model")
    parser.add_argument("--data", default="data/train.csv", help="Path to training CSV")
    parser.add_argument("--output", default="models/", help="Directory to save artifacts")
    args = parser.parse_args()

    metrics = train(args.data, args.output)
    print(f"\nDone. Metrics: {metrics}")
