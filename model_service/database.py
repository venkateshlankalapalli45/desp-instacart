import os
from datetime import datetime
from typing import Generator

from sqlalchemy import Column, DateTime, Float, Integer, JSON, String, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://admin:secure_password123@db:5432/prediction_db",
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class PredictionRecord(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=False)
    input_features = Column(JSON, nullable=False)
    prediction_result = Column(Integer, nullable=False)
    probability = Column(Float, nullable=False)
    source = Column(String, default="webapp", nullable=False)
    model_version = Column(String, default="v1.0", nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    try:
        Base.metadata.create_all(bind=engine)
        print("Database tables initialised.")
    except Exception as exc:
        print(f"Database init failed: {exc}")
