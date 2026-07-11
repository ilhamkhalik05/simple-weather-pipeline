import openmeteo_requests
import requests_cache
import polars as pl
import numpy as np
from retry_requests import retry
from loguru import logger

# Target datasource url
API_URL = "https://api.open-meteo.com/v1/forecast"

TARGET_LOCATION = {
    "location": "Jakarta",
    "latitude": -6.2146,
    "longitude": 106.8451,
}


def fetch_weather_data() -> object:
    logger.info(
        f"Initiating Open-Meteo API client for {TARGET_LOCATION['location']}..."
    )

    # Cache and retry session setup for robust API calls
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    params = {
        **TARGET_LOCATION,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "precipitation_probability",
            "weather_code",
        ],
    }

    try:
        responses = openmeteo.weather_api(API_URL, params=params)
        logger.success("Weather data fetched successfully from API.")
        return responses[0]  # Return the first location fetched
    except Exception as e:
        logger.error(f"Failed to fetch data from Open-Meteo API: {e}")
        raise


def process_weather_response(response: object) -> pl.DataFrame:
    """
    Transforms the raw Open-Meteo API response into a Polars DataFrame.
    """
    logger.info("Transforming raw API response directly into Polars DataFrame...")

    # Get hourly weather forecast data
    hourly = response.Hourly()

    # Generate Numpy array for UNIX timestamps based on API metadata
    time_array = np.arange(hourly.Time(), hourly.TimeEnd(), hourly.Interval())

    # Construct structured dictionary directly from Open-Meteo Numpy arrays
    hourly_data = {
        "date": time_array,  # UNIX Timestamps
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
        "precipitation": hourly.Variables(2).ValuesAsNumpy(),
        "precipitation_probability": hourly.Variables(3).ValuesAsNumpy(),
        "weather_code": hourly.Variables(4).ValuesAsNumpy(),
    }

    df_weather = pl.DataFrame(hourly_data)

    # Cast UNIX timestamp to Datetime (UTC) and append the location string
    df_weather = df_weather.with_columns(
        [
            pl.from_epoch(pl.col("date"), time_unit="s").dt.replace_time_zone("UTC"),
            pl.lit(TARGET_LOCATION["location"]).alias("location"),
        ]
    )

    logger.info(f"Transformation complete. Generated {len(df_weather)} rows of data.")

    return df_weather
