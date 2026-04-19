import os
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Any, Optional

import joblib
import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database import Base, engine, get_db
from models import Prediction

# ── Model state ──────────────────────────────────────────────────
_model: Any = None
_scaler: Any = None
MODEL_VERSION = os.getenv("MODEL_VERSION", "1.0.0")
MODELS_DIR = os.getenv("MODELS_DIR", "/app/models")

FEATURES = [
    "order_dow", "order_hour_of_day", "days_since_prior_order",
    "add_to_cart_order", "department_id", "aisle_id",
]


def _load_artifacts() -> None:
    """Load model and scaler from disk into module-level state."""
    global _model, _scaler
    model_path = f"{MODELS_DIR}/model.joblib"
    scaler_path = f"{MODELS_DIR}/scaler.joblib"
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model not found at {model_path}")
    _model = joblib.load(model_path)
    _scaler = joblib.load(scaler_path)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model artifacts once at API startup."""
    Base.metadata.create_all(bind=engine)
    _load_artifacts()
    yield


app = FastAPI(
    title="Instacart Reorder Prediction API",
    version="1.0.0",
    description="Predicts whether a product will be reordered.",
    lifespan=lifespan,
)


# ── Schemas ──────────────────────────────────────────────────────
class OrderFeatures(BaseModel):
    order_dow: int = Field(..., ge=0, le=6, description="Day of week (0=Sunday)")
    order_hour_of_day: int = Field(..., ge=0, le=23, description="Hour of day")
    days_since_prior_order: float = Field(..., ge=0, le=30, description="Days since last order")
    add_to_cart_order: int = Field(..., ge=1, le=100, description="Position in cart")
    department_id: int = Field(..., ge=1, le=21, description="Product department")
    aisle_id: int = Field(..., ge=1, le=134, description="Product aisle")

    model_config = {"json_schema_extra": {"example": {
        "order_dow": 2, "order_hour_of_day": 14,
        "days_since_prior_order": 7.0, "add_to_cart_order": 3,
        "department_id": 4, "aisle_id": 24,
    }}}


class PredictRequest(BaseModel):
    features: list[OrderFeatures] = Field(..., min_length=1)


class SinglePrediction(BaseModel):
    reordered: bool
    probability: float
    features: dict


class PredictResponse(BaseModel):
    predictions: list[SinglePrediction]
    count: int
    model_version: str


class PastPrediction(BaseModel):
    id: int
    predicted_at: str
    order_dow: int
    order_hour_of_day: int
    days_since_prior_order: float
    add_to_cart_order: int
    department_id: int
    aisle_id: int
    reordered: bool
    probability: float
    model_version: str
    source: str


# ── Helpers ──────────────────────────────────────────────────────
def _to_df(features: list[OrderFeatures]) -> pd.DataFrame:
    """Convert a list of OrderFeatures to a DataFrame."""
    return pd.DataFrame([f.model_dump() for f in features])


def _predict(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Return (predictions, probabilities) for a feature DataFrame."""
    X = _scaler.transform(df[FEATURES])
    probs = _model.predict_proba(X)[:, 1]
    preds = (probs >= 0.5).astype(int)
    return preds, probs


def _save_predictions(
    db: Session,
    df: pd.DataFrame,
    preds: np.ndarray,
    probs: np.ndarray,
    source: str,
) -> None:
    """Persist all predictions to the database."""
    for i, row in df.iterrows():
        record = Prediction(
            order_dow=int(row["order_dow"]),
            order_hour_of_day=int(row["order_hour_of_day"]),
            days_since_prior_order=float(row["days_since_prior_order"]),
            add_to_cart_order=int(row["add_to_cart_order"]),
            department_id=int(row["department_id"]),
            aisle_id=int(row["aisle_id"]),
            reordered=bool(preds[i]),
            probability=round(float(probs[i]), 4),
            model_version=MODEL_VERSION,
            source=source,
        )
        db.add(record)
    db.commit()


# ── Endpoints ────────────────────────────────────────────────────
@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    """Liveness probe for Docker health check."""
    return {"status": "healthy"}


@app.post(
    "/predict",
    response_model=PredictResponse,
    status_code=status.HTTP_200_OK,
)
def predict(
    body: PredictRequest,
    source: str = Query("webapp", description="Source: webapp | scheduled"),
    db: Session = Depends(get_db),
):
    """Predict reorder probability for one or more orders."""
    if _model is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded.",
        )
    df = _to_df(body.features)
    preds, probs = _predict(df)
    _save_predictions(db, df, preds, probs, source)

    results = [
        SinglePrediction(
            reordered=bool(preds[i]),
            probability=round(float(probs[i]), 4),
            features=body.features[i].model_dump(),
        )
        for i in range(len(preds))
    ]
    return PredictResponse(
        predictions=results, count=len(results), model_version=MODEL_VERSION
    )


@app.get(
    "/past-predictions",
    response_model=list[PastPrediction],
    status_code=status.HTTP_200_OK,
)
def past_predictions(
    start_date: Optional[date] = Query(None, description="Filter from date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Filter to date (YYYY-MM-DD)"),
    source: Optional[str] = Query(
        "all", description="Source filter: webapp | scheduled | all"
    ),
    limit: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    """Return past predictions with optional date and source filtering."""
    query = db.query(Prediction)

    if start_date:
        query = query.filter(Prediction.predicted_at >= datetime.combine(start_date, datetime.min.time()))
    if end_date:
        query = query.filter(Prediction.predicted_at <= datetime.combine(end_date, datetime.max.time()))
    if source and source != "all":
        query = query.filter(Prediction.source == source)

    rows = query.order_by(Prediction.predicted_at.desc()).limit(limit).all()

    return [
        PastPrediction(
            id=r.id,
            predicted_at=r.predicted_at.isoformat(),
            order_dow=r.order_dow,
            order_hour_of_day=r.order_hour_of_day,
            days_since_prior_order=r.days_since_prior_order,
            add_to_cart_order=r.add_to_cart_order,
            department_id=r.department_id,
            aisle_id=r.aisle_id,
            reordered=r.reordered,
            probability=r.probability,
            model_version=r.model_version,
            source=r.source,
        )
        for r in rows
    ]
