"""
Train the Instacart reorder prediction model.

Usage:
    python scripts/train.py [--data data/instacart_sample.csv] [--output model/saved_model/]
"""
import argparse
import os
import pickle

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

FEATURES = ["order_dow", "order_hour_of_day", "days_since_prior"]
TARGET = "reordered"
MODEL_VERSION = "v1.0"


def train(data_path: str, output_dir: str) -> dict:
    print(f"Loading data from {data_path} ...")
    df = pd.read_csv(data_path)
    missing = [c for c in FEATURES + [TARGET] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    X = df[FEATURES].values
    y = df[TARGET].values
    print(f"  Rows: {len(df):,}  |  Reorder rate: {y.mean():.2%}")

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s = scaler.transform(X_val)

    print("Training RandomForestClassifier ...")
    model = RandomForestClassifier(
        n_estimators=100, max_depth=6, min_samples_leaf=5,
        random_state=42, n_jobs=-1
    )
    model.fit(X_train_s, y_train)

    preds = model.predict(X_val_s)
    probs = model.predict_proba(X_val_s)[:, 1]
    acc = accuracy_score(y_val, preds)
    try:
        auc = roc_auc_score(y_val, probs)
    except Exception:
        auc = None
    print(f"  Accuracy: {acc:.4f}" + (f"  ROC-AUC: {auc:.4f}" if auc else ""))

    os.makedirs(output_dir, exist_ok=True)
    artifact = {"model": model, "scaler": scaler, "features": FEATURES, "version": MODEL_VERSION}
    out_path = os.path.join(output_dir, "model.pkl")
    with open(out_path, "wb") as f:
        pickle.dump(artifact, f)
    print(f"Saved model artifact → {out_path}")
    return {"accuracy": round(acc, 4)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Instacart reorder model")
    parser.add_argument("--data", default="data/instacart_sample.csv")
    parser.add_argument("--output", default="model/saved_model/")
    args = parser.parse_args()
    train(args.data, args.output)
