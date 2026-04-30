from pathlib import Path
import sys

import pandas as pd
from fastapi.testclient import TestClient
import pytest


BACKEND_DIR = Path(__file__).resolve().parents[1] / "frontend" / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main as backend_main


class _FakeWeatherDataMerger:
    def __init__(self, dataset):
        self.dataset = dataset

    async def merge_weather_data(
        self,
        lat,
        lon,
        start_date,
        end_date,
        rain_source="both",
        include_solar=True,
        include_met=True,
    ):
        assert rain_source == "nasa_power"
        assert include_solar is False
        assert include_met is True

        return pd.DataFrame(
            {
                "time": pd.date_range("2025-12-01", periods=3, freq="D"),
                "RAIN": [1.2, 0.0, 3.4],
            }
        )


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(backend_main, "open_zarr", lambda: object())
    monkeypatch.setattr(backend_main, "WeatherDataMerger", _FakeWeatherDataMerger)
    return TestClient(backend_main.app)


@pytest.mark.parametrize("variable", ["RAIN", "RAIN1"])
def test_timeseries_variable_nasa_s3_rain_series(client, variable):
    response = client.post(
        "/api/data/timeseries-variable",
        params={
            "lat": -3.1493,
            "lon": 21.5332,
            "start_date": "2025-12-01",
            "end_date": "2025-12-31",
            "variable": variable,
            "source": "nasa_s3",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["variable"] == variable
    assert body["values"] == [1.2, 0.0, 3.4]