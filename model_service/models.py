from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class Prediction(Base):
    """ORM model for storing Instacart reorder predictions."""

    __tablename__ = "predictions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    predicted_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )
    # Input features
    order_dow: Mapped[int] = mapped_column(Integer)
    order_hour_of_day: Mapped[int] = mapped_column(Integer)
    days_since_prior_order: Mapped[float] = mapped_column(Float)
    add_to_cart_order: Mapped[int] = mapped_column(Integer)
    department_id: Mapped[int] = mapped_column(Integer)
    aisle_id: Mapped[int] = mapped_column(Integer)
    # Output
    reordered: Mapped[bool] = mapped_column(Integer)
    probability: Mapped[float] = mapped_column(Float)
    model_version: Mapped[str] = mapped_column(String(32), default="1.0.0")
    source: Mapped[str] = mapped_column(String(16), default="webapp", index=True)
