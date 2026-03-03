# tests/test_metrics.py
import pytest
from src.analysis.metrics import compute_ppm, compute_form, compute_minutes_stability

def test_compute_ppm():
    assert compute_ppm(50, 10) == 5.0
    assert compute_ppm(0, 0) == 0

# More tests would mock database connections