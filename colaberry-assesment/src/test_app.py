import pytest
from app import app
from model import Weather, WeatherStatistics, Session
from datetime import date


@pytest.fixture
def client():
    with app.test_client() as client:
        yield client


@pytest.fixture
def setup_db():
    session = Session()
    session.query(Weather).delete()
    session.query(WeatherStatistics).delete()
    session.commit()

    # Add some mock data for testing

    session.add(
        Weather(
            weather_station="USC00335297",
            date=date(2020, 1, 1),
            max_temp=25.5,
            min_temp=18.0,
            precipitation_amount=5.3,
        )
    )

    session.add(
        WeatherStatistics(
            year=2020,
            weather_station="USC00335297",
            avg_max_temp=25.5,
            avg_min_temp=18.0,
            total_precipitation=5.3,
        )
    )

    session.commit()
    session.close()


def test_get_weather(client, setup_db):
    # Test fetching weather data with filtering

    response = client.get("/api/weather?station_id=USC00335297&date=2020-01-01")
    assert response.status_code == 200
    data = response.get_json()
    assert len(data) == 1
    assert data[0]["weather_station"] == "USC00335297"


def test_get_weather_stats(client, setup_db):
    # Test fetching weather statistics data

    response = client.get("/api/weather/stats?station_id=USC00335297&year=2020")
    assert response.status_code == 200
    data = response.get_json()
    assert len(data) == 1
    assert data[0]["weather_station"] == "USC00335297"


if __name__ == "__main__":
    pytest.main()
