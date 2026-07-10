import pandas as pd
import openmeteo_requests
import requests_cache
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
        return responses[0]
    except Exception as e:
        logger.error(f"Failed to fetch data from Open-Meteo API: {e}")
        raise


def process_weather_response(response: object) -> pd.DataFrame:
    logger.info("Transforming raw API response into Pandas DataFrame...")

    hourly = response.Hourly()

    # Construct structured dictionary from Numpy arrays
    hourly_data = {
        "location": TARGET_LOCATION["location"],
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        ),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
        "precipitation": hourly.Variables(2).ValuesAsNumpy(),
        "precipitation_probability": hourly.Variables(3).ValuesAsNumpy(),
        "weather_code": hourly.Variables(4).ValuesAsNumpy(),
    }

    df_weather = pd.DataFrame(data=hourly_data)
    logger.info(f"Transformation complete. Generated {len(df_weather)} rows of data.")

    return df_weather
