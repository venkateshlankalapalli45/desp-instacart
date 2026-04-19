"""
conftest.py — shared pytest fixtures and configuration.
"""
import sys
import os

# Ensure scripts/ and model_service/ are importable from tests/
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, os.path.join(ROOT, "model_service"))
