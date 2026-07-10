import pytest
from unittest.mock import MagicMock
import numpy as np


@pytest.fixture
def mock_openmeteo_response():
    """
    Creates a simulated (mock) response object that perfectly mimics
    the structure returned by the openmeteo_requests client.
    This prevents our unit tests from actually hitting the internet API.
    """
    mock_response = MagicMock()
    mock_hourly = mock_response.Hourly.return_value

    # Simulate Time Series Metadata (UNIX Timestamps)
    mock_hourly.Time.return_value = 1700000000  # Start Time
    mock_hourly.TimeEnd.return_value = 1700007200  # End Time (2 hours later)
    mock_hourly.Interval.return_value = 3600  # 1 hour interval

    # Simulate Variable Arrays (Numpy arrays mimicking API output)
    # Variable 0: temperature_2m
    mock_var_temp = MagicMock()
    mock_var_temp.ValuesAsNumpy.return_value = np.array([28.5, 29.0])

    # Variable 1: relative_humidity_2m
    mock_var_hum = MagicMock()
    mock_var_hum.ValuesAsNumpy.return_value = np.array([80.0, 78.5])

    # Variable 2: precipitation
    mock_var_precip = MagicMock()
    mock_var_precip.ValuesAsNumpy.return_value = np.array([0.0, 1.2])

    # Variable 3: precipitation_probability
    mock_var_prob = MagicMock()
    mock_var_prob.ValuesAsNumpy.return_value = np.array([10.0, 60.0])

    # Variable 4: weather_code
    mock_var_code = MagicMock()
    mock_var_code.ValuesAsNumpy.return_value = np.array([1, 61])

    # Map the variables to the mocked Hourly object
    def mock_variables_side_effect(index):
        variables_map = {
            0: mock_var_temp,
            1: mock_var_hum,
            2: mock_var_precip,
            3: mock_var_prob,
            4: mock_var_code,
        }
        return variables_map[index]

    mock_hourly.Variables.side_effect = mock_variables_side_effect

    return mock_response
