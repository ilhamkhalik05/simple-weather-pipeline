import pandas as pd
from src.extract import process_weather_response


def test_process_weather_response(mock_openmeteo_response):
    """
    GIVEN a mocked Open-Meteo API response object,
    WHEN passed into the process_weather_response function,
    THEN it should return a valid Pandas DataFrame with correct schema and shape.
    """
    # Act: Run the target function using the mock fixture from conftest.py
    df = process_weather_response(mock_openmeteo_response)

    # Assert 1: Type check
    assert isinstance(df, pd.DataFrame), "Output must be a Pandas DataFrame"

    # Assert 2: Shape check (We mocked 2 data points in conftest)
    assert len(df) == 2, "DataFrame should contain exactly 2 rows based on mock data"

    # Assert 3: Schema (Columns) presence check
    expected_columns = [
        "location",
        "date",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "precipitation_probability",
        "weather_code",
    ]
    for col in expected_columns:
        assert col in df.columns, f"Missing expected column: {col}"

    # Assert 4: Data Quality check
    assert df["location"].iloc[0] == "Jakarta", "Location hardcoded value mismatched"
    assert df["temperature_2m"].iloc[0] == 28.5, "Temperature mapping failed"
    assert df["weather_code"].iloc[1] == 61, "Weather code mapping failed"
