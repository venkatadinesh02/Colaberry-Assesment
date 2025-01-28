from sqlalchemy import create_engine, Column, Integer, Float, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

# Solution 1 : Created a Weather ORM model


class Weather(Base):
    __tablename__ = "weather"

    id = Column(Integer, primary_key=True, autoincrement=True)
    weather_station = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation_amount = Column(Float)


class WeatherStatistics(Base):
    __tablename__ = "weather_statistics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    weather_station = Column(String, nullable=False)
    avg_max_temp = Column(Float)
    avg_min_temp = Column(Float)
    total_precipitation = Column(Float)


# Database connection

DATABASE_URL = "sqlite:///weather.db"  # Local SQLite database
engine = create_engine(DATABASE_URL, echo=True)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
