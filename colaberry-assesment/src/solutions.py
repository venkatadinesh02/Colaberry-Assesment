import os
import pandas as pd
from model import Weather, WeatherStatistics, Session, engine
from datetime import datetime

# Specify the folder containing the text files

folder_path = "../wx_data"

# Initialize an empty DataFrame to hold all the data

column_names = [
    "weather_station",
    "date",
    "max_temp",
    "min_temp",
    "precipitation_amount",
]
combined_df = pd.DataFrame()

# Extracting the data from each file

start_time = datetime.now()
print(f"Data Extraction and Ingestion started on : {start_time}")
for file_name in os.listdir(folder_path):
    if file_name.endswith(".txt"):  # Process only .txt files
        file_path = os.path.join(folder_path, file_name)

        # Get weather station name (file name without extension)

        weather_station = os.path.splitext(file_name)[0]

        # Read the file and insert the weather station name into the DataFrame

        df = pd.read_csv(
            file_path, sep=r"\s+", engine="python", names=column_names[1:], header=None
        )
        df["weather_station"] = weather_station  # Add the weather station column

        # Append to the combined DataFrame

        combined_df = pd.concat([combined_df, df], ignore_index=True)
# Convert the 'date' column from yyyymmdd to datetime (yyyy-mm-dd)

combined_df["date"] = pd.to_datetime(combined_df["date"], format="%Y%m%d")


# Solution2: Data Ingested into weather table using weather ORM Model

session = Session()

# Convert DataFrame rows to Weather instances

weather_data = []
for _, row in combined_df.iterrows():
    # Check if a record for the same combination of weather_station and date already exists (Duplication Check)

    existing_record = (
        session.query(Weather)
        .filter_by(weather_station=row["weather_station"], date=row["date"])
        .first()
    )

    # Only add if it does not exist

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
# Add the data to the session (only new records)

if weather_data:
    session.add_all(weather_data)
    # Commit the transaction to insert the data

    session.commit()
end_time = datetime.now()
print(f"Data Ingestion Completed on : {end_time}")
records_inserted = len(weather_data)
print(f"{records_inserted} records Inderted")

# # Close the session
# session.close()


# Solution3: Data Analytics

# Group the data by year and weather station and calculate the statistics

query = session.query(
    Weather.weather_station,
    Weather.date,
    Weather.max_temp,
    Weather.min_temp,
    Weather.precipitation_amount,
)

weather_data = pd.read_sql(query.statement, engine)

# Extract the year from the 'date' column

weather_data["year"] = pd.to_datetime(weather_data["date"]).dt.year

# Calculate the statistics

aggregated_data = (
    weather_data.groupby(["year", "weather_station"])
    .agg(
        avg_max_temp=("max_temp", "mean"),
        avg_min_temp=("min_temp", "mean"),
        total_precipitation=("precipitation_amount", "sum"),
    )
    .reset_index()
)

# Update or insert the aggregated data efficiently

for _, row in aggregated_data.iterrows():
    # Check if the record already exists

    existing_record = (
        session.query(WeatherStatistics)
        .filter(
            WeatherStatistics.year == row["year"],
            WeatherStatistics.weather_station == row["weather_station"],
        )
        .first()
    )

    if existing_record:
        # If the record exists, update the existing record

        existing_record.avg_max_temp = (
            row["avg_max_temp"] if pd.notnull(row["avg_max_temp"]) else None
        )
        existing_record.avg_min_temp = (
            row["avg_min_temp"] if pd.notnull(row["avg_min_temp"]) else None
        )
        existing_record.total_precipitation = (
            row["total_precipitation"]
            if pd.notnull(row["total_precipitation"])
            else None
        )
    else:
        # If the record doesn't exist, insert a new one

        new_record = WeatherStatistics(
            year=row["year"],
            weather_station=row["weather_station"],
            avg_max_temp=(
                row["avg_max_temp"] if pd.notnull(row["avg_max_temp"]) else None
            ),
            avg_min_temp=(
                row["avg_min_temp"] if pd.notnull(row["avg_min_temp"]) else None
            ),
            total_precipitation=(
                row["total_precipitation"]
                if pd.notnull(row["total_precipitation"])
                else None
            ),
        )
        session.add(new_record)
# Commit the changes

session.commit()

# Close the session

session.close()
