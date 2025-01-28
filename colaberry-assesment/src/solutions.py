import os
import pandas as pd
from model import Weather, WeatherStatistics, Session, engine
from datetime import datetime

# Constants
FOLDER_PATH = "../wx_data"
COLUMN_NAMES = ["weather_station", "date", "max_temp", "min_temp", "precipitation_amount"]


def extract_data(folder_path):
    """
    Extract data from all text files in the specified folder and combine them into a single DataFrame.

    Args:
        folder_path (str): Path to the folder containing text files.

    Returns:
        pd.DataFrame: Combined DataFrame with all weather data.
    """
    combined_df = pd.DataFrame()
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".txt"):
            file_path = os.path.join(folder_path, file_name)
            weather_station = os.path.splitext(file_name)[0]  # Extract station name
            df = pd.read_csv(
                file_path, sep=r"\s+", engine="python", names=COLUMN_NAMES[1:], header=None
            )
            df["weather_station"] = weather_station
            combined_df = pd.concat([combined_df, df], ignore_index=True)
    # Convert 'date' column to datetime format
    combined_df["date"] = pd.to_datetime(combined_df["date"], format="%Y%m%d")
    return combined_df


def ingest_weather_data(session, weather_df):
    """
    Ingest weather data into the Weather table.

    Args:
        session (Session): Database session.
        weather_df (pd.DataFrame): DataFrame containing weather data.

    Returns:
        int: Number of records inserted.
    """
    weather_data = []
    for _, row in weather_df.iterrows():
        existing_record = session.query(Weather).filter_by(
            weather_station=row["weather_station"], date=row["date"]
        ).first()
        if not existing_record:
            weather_data.append(
                Weather(
                    weather_station=row["weather_station"],
                    date=row["date"],
                    max_temp=row["max_temp"],
                    min_temp=row["min_temp"],
                    precipitation_amount=row["precipitation_amount"],
                )
            )
    if weather_data:
        session.add_all(weather_data)
        session.commit()
    return len(weather_data)


def calculate_statistics(session):
    """
    Calculate yearly weather statistics and update the WeatherStatistics table.

    Args:
        session (Session): Database session.
    """
    query = session.query(
        Weather.weather_station,
        Weather.date,
        Weather.max_temp,
        Weather.min_temp,
        Weather.precipitation_amount,
    )
    weather_data = pd.read_sql(query.statement, engine)
    weather_data["year"] = pd.to_datetime(weather_data["date"]).dt.year

    aggregated_data = (
        weather_data.groupby(["year", "weather_station"])
        .agg(
            avg_max_temp=("max_temp", "mean"),
            avg_min_temp=("min_temp", "mean"),
            total_precipitation=("precipitation_amount", "sum"),
        )
        .reset_index()
    )

    for _, row in aggregated_data.iterrows():
        existing_record = session.query(WeatherStatistics).filter(
            WeatherStatistics.year == row["year"],
            WeatherStatistics.weather_station == row["weather_station"],
        ).first()
        if existing_record:
            existing_record.avg_max_temp = row["avg_max_temp"]
            existing_record.avg_min_temp = row["avg_min_temp"]
            existing_record.total_precipitation = row["total_precipitation"]
        else:
            new_record = WeatherStatistics(
                year=row["year"],
                weather_station=row["weather_station"],
                avg_max_temp=row["avg_max_temp"],
                avg_min_temp=row["avg_min_temp"],
                total_precipitation=row["total_precipitation"],
            )
            session.add(new_record)
    session.commit()


def main():
    """
    Main function to handle the data extraction, ingestion, and analytics process.
    """
    start_time = datetime.now()
    print(f"Data Extraction and Ingestion started on: {start_time}")

    # Step 1: Extract data
    weather_data = extract_data(FOLDER_PATH)

    # Step 2: Ingest data into the database
    session = Session()
    records_inserted = ingest_weather_data(session, weather_data)
    print(f"{records_inserted} records inserted into the Weather table.")

    # Step 3: Calculate and update weather statistics
    calculate_statistics(session)

    # Close the session
    session.close()

    end_time = datetime.now()
    print(f"Data Ingestion and Analytics completed on: {end_time}")


if __name__ == "__main__":
    main()
