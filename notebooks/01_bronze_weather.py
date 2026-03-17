# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: Open-Meteo Weather Ingestion
# MAGIC
# MAGIC Fetches hourly weather data for 10 European capitals from the Open-Meteo API
# MAGIC and writes raw records to Delta Lake at `dbfs:/delta/bronze/weather/`.
# MAGIC
# MAGIC **Idempotent:** Re-running on the same day overwrites only today's partition.
# MAGIC **Audited:** Every run writes a row to `dbfs:/delta/logs/bronze_audit/`.

# COMMAND ----------

import logging
import time
from datetime import datetime, date
from typing import Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
MAX_RETRIES: int = 3
TIMEOUT_SECONDS: int = 30
BRONZE_WEATHER_PATH: str = "dbfs:/delta/bronze/weather"
AUDIT_LOG_PATH: str = "dbfs:/delta/logs/bronze_audit"
PARTITION_COLS: list[str] = ["country", "ingested_date"]

# Open-Meteo requires no authentication — completely free.
OPEN_METEO_URL: str = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS: str = "temperature_2m,weathercode,windspeed_10m,precipitation"

# Source identifier written to every Bronze row for lineage tracking.
SOURCE_NAME: str = "open_meteo"

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Schema — enforced on every Bronze write to catch upstream API changes early.
# --------------------------------------------------------------------------- #
WEATHER_SCHEMA = StructType([
    StructField("city",           StringType(),    nullable=False),
    StructField("country",        StringType(),    nullable=False),
    StructField("latitude",       DoubleType(),    nullable=False),
    StructField("longitude",      DoubleType(),    nullable=False),
    StructField("timezone",       StringType(),    nullable=False),
    StructField("hour",           TimestampType(), nullable=False),
    StructField("temperature_2m", DoubleType(),    nullable=True),
    StructField("windspeed_10m",  DoubleType(),    nullable=True),
    StructField("weathercode",    IntegerType(),   nullable=True),
    StructField("precipitation",  DoubleType(),    nullable=True),
    StructField("ingested_at",    TimestampType(), nullable=False),
    StructField("ingested_date",  DateType(),      nullable=False),
    StructField("source",         StringType(),    nullable=False),
])

AUDIT_SCHEMA = StructType([
    StructField("source",        StringType(),    nullable=False),
    StructField("city",          StringType(),    nullable=False),
    StructField("row_count",     IntegerType(),   nullable=False),
    StructField("ingested_at",   TimestampType(), nullable=False),
    StructField("status",        StringType(),    nullable=False),
    StructField("error_message", StringType(),    nullable=True),
])

# --------------------------------------------------------------------------- #
# City list — sourced from regions.csv; duplicated here to avoid runtime
# dependency on dbt seed during ingestion.
# --------------------------------------------------------------------------- #
CITIES: list[dict] = [
    {"city": "London",    "country": "GB", "latitude": 51.5074, "longitude": -0.1278, "timezone": "Europe/London"},
    {"city": "Berlin",    "country": "DE", "latitude": 52.5200, "longitude": 13.4050, "timezone": "Europe/Berlin"},
    {"city": "Paris",     "country": "FR", "latitude": 48.8566, "longitude":  2.3522, "timezone": "Europe/Paris"},
    {"city": "Madrid",    "country": "ES", "latitude": 40.4168, "longitude": -3.7038, "timezone": "Europe/Madrid"},
    {"city": "Rome",      "country": "IT", "latitude": 41.9028, "longitude": 12.4964, "timezone": "Europe/Rome"},
    {"city": "Amsterdam", "country": "NL", "latitude": 52.3676, "longitude":  4.9041, "timezone": "Europe/Amsterdam"},
    {"city": "Warsaw",    "country": "PL", "latitude": 52.2297, "longitude": 21.0122, "timezone": "Europe/Warsaw"},
    {"city": "Vienna",    "country": "AT", "latitude": 48.2082, "longitude": 16.3738, "timezone": "Europe/Vienna"},
    {"city": "Stockholm", "country": "SE", "latitude": 59.3293, "longitude": 18.0686, "timezone": "Europe/Stockholm"},
    {"city": "Lisbon",    "country": "PT", "latitude": 38.7169, "longitude": -9.1399, "timezone": "Europe/Lisbon"},
]

# --------------------------------------------------------------------------- #
# API helpers
# --------------------------------------------------------------------------- #

def get_weather_data(city: str, lat: float, lon: float, timezone: str) -> dict:
    """Fetch hourly forecast from Open-Meteo for a single city with exponential backoff."""
    params = {
        "latitude":     lat,
        "longitude":    lon,
        "hourly":       HOURLY_VARS,
        "timezone":     timezone,
        "forecast_days": 1,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(OPEN_METEO_URL, params=params, timeout=TIMEOUT_SECONDS)
            resp.raise_for_status()
            logger.info(f"[{city}] API call succeeded (attempt {attempt})")
            return resp.json()
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES:
                logger.error(f"[{city}] All {MAX_RETRIES} attempts failed: {exc}")
                raise
            sleep_sec = 2 ** attempt  # 2s, 4s, 8s
            logger.warning(f"[{city}] Attempt {attempt} failed: {exc}. Retrying in {sleep_sec}s")
            time.sleep(sleep_sec)


def parse_weather_response(
    city: str, country: str, lat: float, lon: float,
    timezone: str, raw: dict, ingested_at: datetime
) -> list[dict]:
    """Flatten Open-Meteo hourly arrays into a list of row dicts."""
    hourly = raw.get("hourly", {})
    times        = hourly.get("time", [])
    temperatures = hourly.get("temperature_2m", [])
    windspeed    = hourly.get("windspeed_10m", [])
    weathercodes = hourly.get("weathercode", [])
    precipitation = hourly.get("precipitation", [])

    today = ingested_at.date()
    rows: list[dict] = []
    for i, ts_str in enumerate(times):
        rows.append({
            "city":           city,
            "country":        country,
            "latitude":       lat,
            "longitude":      lon,
            "timezone":       timezone,
            "hour":           datetime.fromisoformat(ts_str),
            "temperature_2m": temperatures[i] if i < len(temperatures) else None,
            "windspeed_10m":  windspeed[i]    if i < len(windspeed)    else None,
            "weathercode":    int(weathercodes[i]) if i < len(weathercodes) and weathercodes[i] is not None else None,
            "precipitation":  precipitation[i] if i < len(precipitation) else None,
            "ingested_at":    ingested_at,
            "ingested_date":  today,
            "source":         SOURCE_NAME,
        })
    return rows


# --------------------------------------------------------------------------- #
# Delta write helpers
# --------------------------------------------------------------------------- #

def write_bronze_weather(spark: SparkSession, rows: list[dict], ingested_date: date) -> int:
    """Write parsed rows to the Bronze Delta table, overwriting today's partition only."""
    df = spark.createDataFrame(rows, schema=WEATHER_SCHEMA)
    row_count = df.count()

    # replaceWhere overwrites only today's partition — all other dates are untouched.
    # This makes the notebook safe to re-run on the same day without duplicating data.
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingested_date = '{ingested_date}'")
        .partitionBy(*PARTITION_COLS)
        .save(BRONZE_WEATHER_PATH)
    )
    logger.info(f"Wrote {row_count} rows to {BRONZE_WEATHER_PATH}")
    return row_count


def register_hive_table(spark: SparkSession) -> None:
    """Register the Delta table in the Hive Metastore so dbt sources can query it by name."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS default.bronze_weather
        USING DELTA
        LOCATION '{BRONZE_WEATHER_PATH}'
    """)
    logger.info("Registered default.bronze_weather in Hive Metastore")


def write_audit_row(
    spark: SparkSession,
    city: str,
    row_count: int,
    ingested_at: datetime,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """Append a single audit row to the Bronze audit log."""
    audit_row = [{
        "source":        SOURCE_NAME,
        "city":          city,
        "row_count":     row_count,
        "ingested_at":   ingested_at,
        "status":        status,
        "error_message": error_message,
    }]
    audit_df = spark.createDataFrame(audit_row, schema=AUDIT_SCHEMA)
    (
        audit_df.write
        .format("delta")
        .mode("append")
        .save(AUDIT_LOG_PATH)
    )


# --------------------------------------------------------------------------- #
# Main execution
# --------------------------------------------------------------------------- #

def main() -> None:
    """Ingest weather data for all 10 cities and write to Bronze Delta."""
    spark = SparkSession.builder.getOrCreate()
    ingested_at = datetime.utcnow()
    ingested_date = ingested_at.date()
    all_rows: list[dict] = []

    logger.info(f"Starting weather ingestion run at {ingested_at.isoformat()}")

    for city_cfg in CITIES:
        city = city_cfg["city"]
        try:
            raw = get_weather_data(
                city=city,
                lat=city_cfg["latitude"],
                lon=city_cfg["longitude"],
                timezone=city_cfg["timezone"],
            )
            rows = parse_weather_response(
                city=city,
                country=city_cfg["country"],
                lat=city_cfg["latitude"],
                lon=city_cfg["longitude"],
                timezone=city_cfg["timezone"],
                raw=raw,
                ingested_at=ingested_at,
            )
            all_rows.extend(rows)
            logger.info(f"[{city}] Parsed {len(rows)} hourly records")
        except Exception as exc:
            # Log failure per city but continue — partial data is better than no data.
            logger.error(f"[{city}] Failed to fetch/parse: {exc}")
            write_audit_row(spark, city, 0, ingested_at, "FAILED", str(exc))

    if not all_rows:
        raise RuntimeError("No rows collected from any city — aborting Bronze write.")

    total_rows = write_bronze_weather(spark, all_rows, ingested_date)
    register_hive_table(spark)

    # Write a summary audit row for the full run.
    write_audit_row(spark, "ALL_CITIES", total_rows, ingested_at, "SUCCESS")
    logger.info(f"Weather ingestion complete. Total rows written: {total_rows}")


main()
