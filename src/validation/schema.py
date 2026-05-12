from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional


class WeatherData(BaseModel):
    """
    Pydantic model for strict weather data validation.
    Every field is validated before entering the Kafka pipeline.
    """
    city: str = Field(..., min_length=1, max_length=100)
    country: str = Field(..., min_length=2, max_length=10)
    temperature: float = Field(..., ge=-90.0, le=60.0)
    feels_like: float = Field(..., ge=-90.0, le=60.0)
    humidity: int = Field(..., ge=0, le=100)
    pressure: int = Field(..., ge=800, le=1100)
    weather_description: str = Field(..., min_length=1, max_length=255)
    wind_speed: float = Field(..., ge=0.0)
    visibility: Optional[int] = Field(default=0, ge=0)
    recorded_at: datetime

    @field_validator("city")
    @classmethod
    def city_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("City name cannot be empty or whitespace")
        return v.strip().title()

    @field_validator("weather_description")
    @classmethod
    def description_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Weather description cannot be empty")
        return v.strip().lower()

    @field_validator("temperature", "feels_like")
    @classmethod
    def temperature_must_be_realistic(cls, v: float) -> float:
        if v < -90 or v > 60:
            raise ValueError(f"Temperature {v} is outside realistic range (-90 to 60 Celsius)")
        return round(v, 2)

    @field_validator("wind_speed")
    @classmethod
    def wind_speed_must_be_positive(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Wind speed cannot be negative")
        return round(v, 2)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class WeatherDataDB(WeatherData):
    """
    Extended model for database insertion.
    Adds inserted_at timestamp automatically.
    """
    inserted_at: datetime = Field(default_factory=datetime.utcnow)