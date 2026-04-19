"""Tests for Pydantic schema validation in model_service/main.py"""
import sys
import os

import pytest
from pydantic import ValidationError

# Add model_service to path so we can import its schemas directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "model_service"))

from main import OrderFeatures, PredictRequest  # noqa: E402


class TestOrderFeatures:
    def test_valid_features(self):
        f = OrderFeatures(
            order_dow=2,
            order_hour_of_day=14,
            days_since_prior_order=7.0,
            add_to_cart_order=3,
            department_id=4,
            aisle_id=24,
        )
        assert f.order_dow == 2
        assert f.aisle_id == 24

    def test_valid_features_boundary_values(self):
        # Min boundaries
        f = OrderFeatures(
            order_dow=0, order_hour_of_day=0,
            days_since_prior_order=0.0, add_to_cart_order=1,
            department_id=1, aisle_id=1,
        )
        assert f.order_dow == 0
        # Max boundaries
        f = OrderFeatures(
            order_dow=6, order_hour_of_day=23,
            days_since_prior_order=30.0, add_to_cart_order=100,
            department_id=21, aisle_id=134,
        )
        assert f.aisle_id == 134

    def test_invalid_order_dow_too_high(self):
        with pytest.raises(ValidationError):
            OrderFeatures(
                order_dow=7,  # max is 6
                order_hour_of_day=14,
                days_since_prior_order=7.0,
                add_to_cart_order=3,
                department_id=4,
                aisle_id=24,
            )

    def test_invalid_order_dow_negative(self):
        with pytest.raises(ValidationError):
            OrderFeatures(
                order_dow=-1,
                order_hour_of_day=14,
                days_since_prior_order=7.0,
                add_to_cart_order=3,
                department_id=4,
                aisle_id=24,
            )

    def test_invalid_hour_too_high(self):
        with pytest.raises(ValidationError):
            OrderFeatures(
                order_dow=2,
                order_hour_of_day=24,  # max is 23
                days_since_prior_order=7.0,
                add_to_cart_order=3,
                department_id=4,
                aisle_id=24,
            )

    def test_invalid_days_since_out_of_range(self):
        with pytest.raises(ValidationError):
            OrderFeatures(
                order_dow=2,
                order_hour_of_day=14,
                days_since_prior_order=31.0,  # max is 30
                add_to_cart_order=3,
                department_id=4,
                aisle_id=24,
            )

    def test_missing_required_field(self):
        with pytest.raises(ValidationError):
            OrderFeatures(
                order_dow=2,
                order_hour_of_day=14,
                # days_since_prior_order missing
                add_to_cart_order=3,
                department_id=4,
                aisle_id=24,
            )

    def test_invalid_department_id_too_high(self):
        with pytest.raises(ValidationError):
            OrderFeatures(
                order_dow=2,
                order_hour_of_day=14,
                days_since_prior_order=7.0,
                add_to_cart_order=3,
                department_id=22,  # max is 21
                aisle_id=24,
            )

    def test_invalid_aisle_id_too_high(self):
        with pytest.raises(ValidationError):
            OrderFeatures(
                order_dow=2,
                order_hour_of_day=14,
                days_since_prior_order=7.0,
                add_to_cart_order=3,
                department_id=4,
                aisle_id=135,  # max is 134
            )


class TestPredictRequest:
    def _valid_feature(self):
        return {
            "order_dow": 2, "order_hour_of_day": 14,
            "days_since_prior_order": 7.0, "add_to_cart_order": 3,
            "department_id": 4, "aisle_id": 24,
        }

    def test_valid_single_item(self):
        req = PredictRequest(features=[self._valid_feature()])
        assert len(req.features) == 1

    def test_valid_batch(self):
        req = PredictRequest(features=[self._valid_feature()] * 5)
        assert len(req.features) == 5

    def test_empty_features_list_raises(self):
        with pytest.raises(ValidationError):
            PredictRequest(features=[])

    def test_invalid_feature_in_batch_raises(self):
        bad = self._valid_feature()
        bad["order_dow"] = 99  # out of range
        with pytest.raises(ValidationError):
            PredictRequest(features=[self._valid_feature(), bad])
