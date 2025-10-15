from datetime import datetime

import pandas as pd
import pytest


def test_valid_date_parsing():
    """Test date parsing functionality"""
    test_date = "2024-01-01"
    parsed = pd.to_datetime(test_date)
    assert parsed.year == 2024
    assert parsed.month == 1
    assert parsed.day == 1


def test_yoy_calculation():
    """Test year-over-year calculation logic"""
    current_value = 100
    previous_value = 90
    expected_yoy = ((current_value - previous_value) / previous_value) * 100
    assert round(expected_yoy, 2) == 11.11


def test_missing_value_handling():
    """Test handling of missing values"""
    df = pd.DataFrame({"value": ["100", "200", ".", "400"]})
    df["value_numeric"] = df["value"].apply(lambda x: None if x == "." else float(x))
    assert df["value_numeric"].isna().sum() == 1
    assert df["value_numeric"].notna().sum() == 3


def test_data_type_conversion():
    """Test proper data type conversions"""
    test_data = {"date": ["2024-01-01", "2024-01-02"], "value": ["100.5", "200.3"]}
    df = pd.DataFrame(test_data)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = df["value"].astype(float)

    assert df["date"].dtype == "datetime64[ns]"
    assert df["value"].dtype == "float64"
