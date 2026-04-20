import os
import pickle
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, List, Optional

import numpy as np
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database import PredictionRecord, get_db, init_db

# ── Model state ──────────────────────────────────────────────────
_artifact: Any = None  # dict with keys: model, scaler, features, version
MODEL_PATH = os.getenv("MODEL_PATH", "/code/model/saved_model/model.pkl")

FEATURES = ["order_dow", "order_hour_of_day", "days_since_prior"]


def _load_model() -> None:
    global _artifact
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(f"Model not found at {MODEL_PATH}")
    with open(MODEL_PATH, "rb") as f:
        _artifact = pickle.load(f)
    print(f"Model loaded from {MODEL_PATH} (version={_artifact.get('version','?')})")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    init_db()
    yield


app = FastAPI(
    title="Instacart Reorder Prediction API",
    version="1.0.0",
    description="Predicts whether a user will reorder.",
    lifespan=lifespan,
)


# ── Schemas ──────────────────────────────────────────────────────
class OrderFeatures(BaseModel):
    order_dow: int = Field(..., ge=0, le=6, description="Day of week (0=Sunday)")
    order_hour_of_day: int = Field(..., ge=0, le=23, description="Hour of day")
    days_since_prior: float = Field(..., ge=0, le=30, description="Days since last order")

    model_config = {"json_schema_extra": {"example": {
        "order_dow": 2, "order_hour_of_day": 14, "days_since_prior": 7.0,
    }}}


class SinglePredictionRequest(BaseModel):
    user_id: int = Field(..., description="User ID")
    features: OrderFeatures
    source: str = Field("webapp", description="Source: webapp | scheduled")


class BatchPredictionRequest(BaseModel):
    predictions: List[SinglePredictionRequest] = Field(..., min_length=1)


class PredictionResponse(BaseModel):
    user_id: int
    reordered: bool
    probability: float
    model_version: str
    source: str
    created_at: Optional[datetime] = None


class BatchPredictionResponse(BaseModel):
    predictions: List[PredictionResponse]
    count: int
    model_version: str


class PastPredictionSchema(BaseModel):
    id: int
    user_id: int
    input_features: Any
    prediction_result: int
    probability: float
    source: str
    model_version: str
    created_at: datetime

    class Config:
        from_attributes = True


# ── Helpers ──────────────────────────────────────────────────────
def _predict_one(features: OrderFeatures) -> tuple[bool, float]:
    if _artifact is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Model not loaded.")
    model = _artifact["model"]
    scaler = _artifact["scaler"]
    X = pd.DataFrame([[features.order_dow, features.order_hour_of_day, features.days_since_prior]],
                     columns=FEATURES)
    X_scaled = scaler.transform(X)
    prob = float(model.predict_proba(X_scaled)[0, 1])
    return prob >= 0.5, round(prob, 4)


# ── Endpoints ────────────────────────────────────────────────────
@app.get("/health", status_code=status.HTTP_200_OK)
def health():
    return {"status": "healthy"}


@app.post("/predict", response_model=BatchPredictionResponse, status_code=status.HTTP_200_OK)
def predict(
    body: BatchPredictionRequest,
    db: Session = Depends(get_db),
):
    """Batch prediction endpoint — accepts 1 or more requests."""
    version = _artifact["version"] if _artifact else "v1.0"
    results = []

    for req in body.predictions:
        reordered, prob = _predict_one(req.features)
        record = PredictionRecord(
            user_id=req.user_id,
            input_features=req.features.model_dump(),
            prediction_result=int(reordered),
            probability=prob,
            source=req.source,
            model_version=version,
        )
        db.add(record)
        db.flush()
        results.append(PredictionResponse(
            user_id=req.user_id,
            reordered=reordered,
            probability=prob,
            model_version=version,
            source=req.source,
            created_at=record.created_at,
        ))

    db.commit()
    return BatchPredictionResponse(predictions=results, count=len(results), model_version=version)


@app.get("/past-predictions", response_model=List[PastPredictionSchema], status_code=status.HTTP_200_OK)
def past_predictions(
    user_id: Optional[int] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    source: Optional[str] = Query("all"),
    limit: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    query = db.query(PredictionRecord)
    if user_id is not None:
        query = query.filter(PredictionRecord.user_id == user_id)
    if start_date:
        query = query.filter(PredictionRecord.created_at >= start_date)
    if end_date:
        query = query.filter(PredictionRecord.created_at <= end_date)
    if source and source != "all":
        query = query.filter(PredictionRecord.source == source)
    return query.order_by(PredictionRecord.created_at.desc()).limit(limit).all()


@app.get("/")
def root():
    return {"message": "Instacart Reorder Prediction API", "status": "ready"}
