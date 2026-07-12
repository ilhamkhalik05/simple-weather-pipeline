import os
import pytest
import polars as pl
from datetime import datetime
from src.pipeline_manager import WeatherELTPipeline


@pytest.fixture(scope="module")
def setup_test_environment(tmp_path_factory):
    """
    Pytest fixture to create an isolated temporary directory for testing.
    This ensures our tests don't overwrite the actual production data/ folder.
    Runs once for the entire module.
    """
    # Create a temporary base directory
    test_base_dir = tmp_path_factory.mktemp("test_data")

    # Initialize pipeline with the temporary directory
    pipeline = WeatherELTPipeline(base_data_dir=str(test_base_dir))

    # Create mock raw data based on the structure returned by extract.py
    mock_raw_data = pl.DataFrame(
        {
            "location": ["Jakarta", "Jakarta", "Jakarta", "Jakarta"],
            # Mix of valid dates and deliberate duplicate for testing uniqueness
            "date": [
                datetime(2026, 7, 10, 10, 0),
                datetime(2026, 7, 10, 11, 0),
                datetime(2026, 7, 10, 12, 0),
                datetime(2026, 7, 10, 12, 0),  # Duplicate!
            ],
            "temperature_2m": [32.5, 33.0, 34.2, 34.2],
            "relative_humidity_2m": [60.5, 58.0, 55.2, 55.2],
            "precipitation": [0.0, 0.0, 2.5, 2.5],
            "precipitation_probability": [10.0, 20.0, 80.0, 80.0],
            # Float type to simulate API response before casting
            "weather_code": [2.0, 3.0, 61.0, 61.0],
        }
    )

    return pipeline, mock_raw_data, str(test_base_dir)


def test_bronze_layer_ingestion(setup_test_environment):
    """
    BRONZE LAYER TEST: Raw Integrity & Audit Trail
    Ensures data lands safely and audit metadata is attached.
    """
    pipeline, mock_data, test_dir = setup_test_environment

    # Act
    pipeline.load_bronze_layer(mock_data)

    # Assert
    bronze_file = os.path.join(test_dir, "bronze", "raw_weather.parquet")
    assert os.path.exists(bronze_file), "Bronze Parquet file was not created."

    df_bronze = pl.read_parquet(bronze_file)
    assert len(df_bronze) == 4, "Bronze layer should match exact input row count."
    assert (
        "extracted_at" in df_bronze.columns
    ), "Audit column 'extracted_at' is missing."


def test_silver_layer_conforming(setup_test_environment):
    """
    SILVER LAYER TEST: Data Quality & Schema Conforming
    Ensures types are casted correctly and duplicates are removed.
    """
    pipeline, _, test_dir = setup_test_environment

    # Act
    pipeline.process_silver_layer()

    # Assert
    silver_file = os.path.join(test_dir, "silver", "cleaned_weather.parquet")
    assert os.path.exists(silver_file), "Silver Parquet file was not created."

    df_silver = pl.read_parquet(silver_file)

    # 1. Uniqueness Check (Deduplication)
    assert len(df_silver) == 3, "Silver layer failed to remove duplicate rows."

    # 2. Type Casting Check
    assert (
        df_silver["weather_code"].dtype == pl.Int32
    ), "Weather code failed cast to Int32."
    assert (
        df_silver["temperature_2m"].dtype == pl.Float32
    ), "Temperature failed cast to Float32."

    # 3. Value Range Check (Data Quality)
    humidity_max = df_silver["relative_humidity_2m"].max()
    humidity_min = df_silver["relative_humidity_2m"].min()
    assert (
        0 <= humidity_min and humidity_max <= 100
    ), "Humidity values out of logical bounds (0-100)."


def test_gold_layer_dimensional_modeling(setup_test_environment):
    """
    GOLD LAYER TEST: Business Logic & Star Schema Integrity
    Ensures dimensions and facts are built accurately.
    """
    pipeline, _, test_dir = setup_test_environment

    # Act
    pipeline.process_gold_layer()

    # Assert 1: File Presence
    gold_files = [
        "dim_location.parquet",
        "dim_date.parquet",
        "fact_weather_hourly.parquet",
        "fact_weather_daily_summary.parquet",
    ]
    for file in gold_files:
        assert os.path.exists(
            os.path.join(test_dir, "gold", file)
        ), f"Missing Gold file: {file}"

    # Assert 2: Dim Date Logic
    df_dim_date = pl.read_parquet(os.path.join(test_dir, "gold", "dim_date.parquet"))
    assert "date_key" in df_dim_date.columns, "Surrogate date_key missing."
    assert df_dim_date["date_key"].dtype == pl.Int32, "date_key must be an Integer."
    # Check if 2026-07-10 translates to integer 20260710
    assert df_dim_date["date_key"][0] == 20260710, "date_key generation logic failed."

    # Assert 3: Aggregation Logic (Fact Daily Summary)
    df_daily = pl.read_parquet(
        os.path.join(test_dir, "gold", "fact_weather_daily_summary.parquet")
    )
    assert len(df_daily) == 1, "Should aggregate to exactly 1 day based on mock data."
    assert df_daily["max_temperature"][0] == pytest.approx(
        34.2
    ), "Max temperature aggregation is incorrect."
    assert df_daily["min_temperature"][0] == pytest.approx(
        32.5
    ), "Min temperature aggregation is incorrect."
