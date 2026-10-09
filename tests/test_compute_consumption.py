"""Unit tests for procesado/compute_consumption.py.

These tests use synthetic pandas DataFrames only (no real DB/API access),
so they are safe to run in CI.
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from procesado.compute_consumption import (
    append_minute_consumption,
    compute_minute_consumption,
    determine_counter_max,
    detect_counter_resets,
    distribute_negative_compensations,
)


def _minute_index(n):
    return pd.date_range("2026-01-01 00:00:00", periods=n, freq="min")


class TestDetermineCounterMax:
    def test_power_of_ten_above_value(self):
        assert determine_counter_max(5_000_000) == 10_000_000

    def test_exact_power_of_ten(self):
        assert determine_counter_max(10_000_000) == 10_000_000

    def test_non_positive_defaults_to_ten_million(self):
        assert determine_counter_max(0) == 10_000_000
        assert determine_counter_max(-5) == 10_000_000


class TestComputeMinuteConsumption:
    def test_consumption_is_next_minus_current(self):
        idx = _minute_index(3)
        df = pd.DataFrame({"A_TOT": [100.0, 150.0, 170.0]}, index=idx)

        out = compute_minute_consumption(df)

        assert out["A_TOT_cons"].iloc[0] == 50.0
        assert out["A_TOT_cons"].iloc[1] == 20.0
        assert np.isnan(out["A_TOT_cons"].iloc[2])  # no next minute


class TestAppendMinuteConsumption:
    def test_anomalous_jump_forces_zero_consumption(self):
        idx = _minute_index(3)
        df = pd.DataFrame(
            {
                "A_TOT": [100.0, 2_000_000.0, 2_000_050.0],
                "A_TOT_is_anomalous_jump": [False, True, False],
            },
            index=idx,
        )

        out = append_minute_consumption(df)

        # Row 1 is flagged as anomalous jump -> its consumption is forced to 0
        assert out["A_TOT_cons"].iloc[0] == 1_999_900.0
        assert out["A_TOT_cons"].iloc[1] == 0.0


class TestDetectCounterResets:
    def test_real_reset_near_zero_is_corrected(self):
        idx = _minute_index(10)
        totals = [9_999_990.0, 9_999_995.0, 5.0] + [5.0 + i for i in range(7)]
        df = pd.DataFrame({"A_TOT": totals}, index=idx)
        df["A_TOT_anom"] = np.nan

        result = detect_counter_resets(df, near_zero_threshold=1000)

        # The reset at index 1->2 should be marked with an estimated correction
        assert not result["A_TOT_anom"].isna().all()
        corrected_value = result["A_TOT_anom"].dropna().iloc[0]
        assert corrected_value > 0

    def test_anomalous_jump_not_near_zero_is_not_corrected(self):
        idx = _minute_index(5)
        # Big negative jump but NOT near zero afterwards -> should NOT be treated as reset
        totals = [5_000_000.0, 3_000_000.0, 3_000_010.0, 3_000_020.0, 3_000_030.0]
        df = pd.DataFrame({"A_TOT": totals}, index=idx)
        df["A_TOT_anom"] = np.nan

        result = detect_counter_resets(df, near_zero_threshold=1000)

        assert result["A_TOT_anom"].isna().all()


class TestDistributeNegativeCompensations:
    def test_negative_then_positive_is_distributed_over_zero_run(self):
        idx = _minute_index(5)
        df = pd.DataFrame(
            {
                "A_TOT": [0.0, 0.0, 0.0, 0.0, 10.0],
                "A_TOT_cons": [0.0, 0.0, -5.0, 8.0, np.nan],
            },
            index=idx,
        )

        anom = distribute_negative_compensations(df)

        # net = -5 + 8 = 3, distributed over the zero-run preceding position 2 (indices 0,1,2)
        distributed = anom["A_TOT_anom"].dropna()
        assert pytest.approx(distributed.sum(), rel=1e-6) == 3.0

    def test_no_negative_positive_pair_yields_no_anomalies(self):
        idx = _minute_index(3)
        df = pd.DataFrame(
            {
                "A_TOT": [0.0, 10.0, 20.0],
                "A_TOT_cons": [10.0, 10.0, np.nan],
            },
            index=idx,
        )

        anom = distribute_negative_compensations(df)

        assert anom["A_TOT_anom"].isna().all()
