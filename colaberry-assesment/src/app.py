from flask import Flask, jsonify, request
from model import Weather, WeatherStatistics, Session
from flasgger import Swagger

app = Flask(__name__)
Swagger(app)

# Pagination function to handle page and per_page


def paginate(query, page, per_page):
    paginated_query = query.offset((page - 1) * per_page).limit(per_page)
    return paginated_query


@app.route("/")
def home():
    return "Welcome to the Weather API! Use /api/weather or /api/weather/stats to access data."


@app.route("/api/weather", methods=["GET"])
def get_weather():
    """
    Retrieve weather data
    ---
    parameters:
      - name: date
        in: query
        type: string
        description: Filter by date (YYYY-MM-DD)
      - name: station_id
        in: query
        type: string
        description: Filter by weather station ID
      - name: page
        in: query
        type: integer
        default: 1
        description: Page number for pagination
      - name: per_page
        in: query
        type: integer
        default: 10
        description: Number of records per page
    responses:
      200:
        description: A list of weather data
        examples:
          application/json: [
              {
                  "weather_station": "ST001",
                  "date": "2023-01-01",
                  "max_temp": 35.0,
                  "min_temp": 20.0,
                  "precipitation_amount": 5.0
              }
          ]
    """
    session = Session()

    # Get filter parameters from the query string

    date = request.args.get("date")
    station_id = request.args.get("station_id")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 10))

    query = session.query(Weather)

    if date:
        query = query.filter(Weather.date == date)
    if station_id:
        query = query.filter(Weather.weather_station == station_id)
    # Apply pagination

    query = paginate(query, page, per_page)

    weather_data = query.all()

    result = [
        {
            "weather_station": record.weather_station,
            "date": record.date,
            "max_temp": record.max_temp,
            "min_temp": record.min_temp,
            "precipitation_amount": record.precipitation_amount,
        }
        for record in weather_data
    ]

    session.close()

    return jsonify(result)


@app.route("/api/weather/stats", methods=["GET"])
def get_weather_stats():
    """
    Retrieve weather statistics
    ---
    parameters:
      - name: year
        in: query
        type: string
        description: Filter by year
      - name: station_id
        in: query
        type: string
        description: Filter by weather station ID
      - name: page
        in: query
        type: integer
        default: 1
        description: Page number for pagination
      - name: per_page
        in: query
        type: integer
        default: 10
        description: Number of records per page
    responses:
      200:
        description: A list of weather statistics
        examples:
          application/json: [
              {
                  "year": 2023,
                  "weather_station": "ST001",
                  "avg_max_temp": 34.0,
                  "avg_min_temp": 18.0,
                  "total_precipitation": 120.5
              }
          ]
    """
    session = Session()

    # Get filter parameters from the query string

    year = request.args.get("year")
    station_id = request.args.get("station_id")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 10))

    query = session.query(WeatherStatistics)

    if year:
        query = query.filter(WeatherStatistics.year == year)
    if station_id:
        query = query.filter(WeatherStatistics.weather_station == station_id)
    # Apply pagination

    query = paginate(query, page, per_page)

    stats_data = query.all()

    result = [
        {
            "year": record.year,
            "weather_station": record.weather_station,
            "avg_max_temp": record.avg_max_temp,
            "avg_min_temp": record.avg_min_temp,
            "total_precipitation": record.total_precipitation,
        }
        for record in stats_data
    ]

    session.close()

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
