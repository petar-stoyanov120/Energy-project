# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze: Energy Ingestion (UK Carbon Intensity + SMARD)
# MAGIC
# MAGIC Fetches energy data from two free, no-auth-required sources:
# MAGIC
# MAGIC | Source | Zones | Data |
# MAGIC |---|---|---|
# MAGIC | UK Carbon Intensity API | GB (London) | Carbon intensity, renewable %, fossil-free % |
# MAGIC | SMARD (Bundesnetzagentur) | DE (Berlin), AT (Vienna) | Day-ahead electricity price (€/MWh) |
# MAGIC
# MAGIC **Note:** FR, ES, IT, NL, PL, SE, PT have no energy rows until ENTSO-E access is granted.
# MAGIC The fact table uses left joins so those cities still appear with weather data only.
# MAGIC
# MAGIC **No API keys required.** Both sources are completely free and open.
# MAGIC
# MAGIC **Idempotent:** Re-running on the same day overwrites only today's partition.
# MAGIC **Audited:** Every run appends rows to `dbfs:/delta/logs/bronze_audit/`.

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
BRONZE_ENERGY_PATH: str = "dbfs:/delta/bronze/energy"
AUDIT_LOG_PATH: str = "dbfs:/delta/logs/bronze_audit"
PARTITION_COLS: list[str] = ["country", "ingested_date"]

# UK Carbon Intensity API — no auth, completely free.
UK_INTENSITY_URL: str = "https://api.carbonintensity.org.uk/intensity"
UK_GENERATION_URL: str = "https://api.carbonintensity.org.uk/generation"

# SMARD (German Federal Network Agency) — no auth, completely free.
# Two-step fetch: (1) get available timestamps, (2) get latest data chunk.
SMARD_BASE: str = "https://www.smard.de/app/chart_data"
SMARD_DAY_AHEAD_PRICE_FILTER: int = 410  # Day-ahead electricity price (€/MWh)

SOURCE_UK: str = "uk_carbon_intensity"
SOURCE_SMARD: str = "smard"

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Schema — matches downstream Silver model expectations.
# day_ahead_price_eur_mwh is new vs the Electricity Maps version.
# --------------------------------------------------------------------------- #
ENERGY_SCHEMA = StructType([
    StructField("zone",                    StringType(),    nullable=False),
    StructField("country",                 StringType(),    nullable=False),
    StructField("city",                    StringType(),    nullable=False),
    StructField("carbon_intensity",        DoubleType(),    nullable=True),
    StructField("fossil_free_pct",         DoubleType(),    nullable=True),
    StructField("renewable_pct",           DoubleType(),    nullable=True),
    StructField("day_ahead_price_eur_mwh", DoubleType(),    nullable=True),
    StructField("measurement_at",          TimestampType(), nullable=True),
    StructField("ingested_at",             TimestampType(), nullable=False),
    StructField("ingested_date",           DateType(),      nullable=False),
    StructField("source",                  StringType(),    nullable=False),
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
# Zones — only zones with a live data source.
# Other cities (FR, ES, IT, NL, PL, SE, PT) appear in the fact table via
# weather data alone; their energy columns will be NULL until ENTSO-E arrives.
# --------------------------------------------------------------------------- #
ZONES: list[dict] = [
    {"city": "London", "country": "GB", "zone": "GB", "source": SOURCE_UK},
    {"city": "Berlin", "country": "DE", "zone": "DE", "source": SOURCE_SMARD},
    {"city": "Vienna", "country": "AT", "zone": "AT", "source": SOURCE_SMARD},
]

# Fuels the UK Carbon Intensity API considers renewable / fossil-free.
UK_RENEWABLE_FUELS: set[str]    = {"wind", "solar", "hydro", "biomass"}
UK_FOSSIL_FREE_FUELS: set[str]  = {"wind", "solar", "hydro", "biomass", "nuclear"}

# --------------------------------------------------------------------------- #
# Generic retry wrapper
# --------------------------------------------------------------------------- #

def fetch_with_retry(url: str, label: str) -> dict:
    """GET a JSON endpoint with exponential backoff. Raises after MAX_RETRIES."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=TIMEOUT_SECONDS)
            resp.raise_for_status()
            logger.info(f"[{label}] GET {url} succeeded (attempt {attempt})")
            return resp.json()
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES:
                logger.error(f"[{label}] All {MAX_RETRIES} attempts failed: {exc}")
                raise
            sleep_sec = 2 ** attempt  # 2s, 4s, 8s
            logger.warning(f"[{label}] Attempt {attempt} failed: {exc}. Retrying in {sleep_sec}s")
            time.sleep(sleep_sec)

# --------------------------------------------------------------------------- #
# UK Carbon Intensity API helpers
# --------------------------------------------------------------------------- #

def get_uk_energy_row(ingested_at: datetime) -> dict:
    """Fetch carbon intensity + generation mix for Great Britain and return a row dict."""
    intensity_json  = fetch_with_retry(UK_INTENSITY_URL,  label="GB-intensity")
    generation_json = fetch_with_retry(UK_GENERATION_URL, label="GB-generation")

    intensity_data = intensity_json.get("data", [{}])[0]
    gen_data       = generation_json.get("data", {})

    # Build a fuel → percentage lookup from the generation mix list.
    gen_mix: dict[str, float] = {
        item["fuel"]: item["perc"]
        for item in gen_data.get("generationmix", [])
    }

    renewable_pct   = sum(gen_mix.get(f, 0.0) for f in UK_RENEWABLE_FUELS)
    fossil_free_pct = sum(gen_mix.get(f, 0.0) for f in UK_FOSSIL_FREE_FUELS)

    # Prefer actual over forecast; both may be present.
    intensity_block = intensity_data.get("intensity", {})
    carbon_intensity = intensity_block.get("actual") or intensity_block.get("forecast")

    # Parse measurement timestamp from the "from" field.
    ts_str = intensity_data.get("from")
    measurement_at = (
        datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if ts_str else ingested_at
    )

    logger.info(
        f"[GB] carbon_intensity={carbon_intensity} gCO2/kWh  "
        f"renewable={renewable_pct:.1f}%  fossil_free={fossil_free_pct:.1f}%"
    )

    return {
        "zone":                    "GB",
        "country":                 "GB",
        "city":                    "London",
        "carbon_intensity":        float(carbon_intensity) if carbon_intensity is not None else None,
        "fossil_free_pct":         float(fossil_free_pct),
        "renewable_pct":           float(renewable_pct),
        "day_ahead_price_eur_mwh": None,   # UK market uses GBP — out of scope for EUR pipeline
        "measurement_at":          measurement_at,
        "ingested_at":             ingested_at,
        "ingested_date":           ingested_at.date(),
        "source":                  SOURCE_UK,
    }

# --------------------------------------------------------------------------- #
# SMARD API helpers
# --------------------------------------------------------------------------- #

def get_smard_day_ahead_price(region: str) -> tuple[Optional[float], Optional[datetime]]:
    """Fetch the latest day-ahead electricity price (€/MWh) from SMARD for a region.

    SMARD returns weekly chunks of hourly data. We:
    1. Fetch the index to find the latest available chunk timestamp.
    2. Fetch that chunk and scan backwards to find the most recent non-null price.
    """
    # Step 1 — get list of available chunk timestamps.
    index_url = (
        f"{SMARD_BASE}/{SMARD_DAY_AHEAD_PRICE_FILTER}/{region}"
        f"/index_hour.json"
    )
    index_data = fetch_with_retry(index_url, label=f"{region}-smard-index")
    timestamps = index_data.get("timestamps", [])

    if not timestamps:
        logger.warning(f"[{region}] SMARD index returned no timestamps.")
        return None, None

    # Step 2 — fetch the latest chunk.
    last_ts = timestamps[-1]
    data_url = (
        f"{SMARD_BASE}/{SMARD_DAY_AHEAD_PRICE_FILTER}/{region}"
        f"/{SMARD_DAY_AHEAD_PRICE_FILTER}_{region}_hour_{last_ts}.json"
    )
    chunk_data = fetch_with_retry(data_url, label=f"{region}-smard-chunk")
    series = chunk_data.get("series", [])

    # Scan backwards to find the most recent non-null value.
    # SMARD publishes day-ahead prices for tomorrow, so the latest entry is usually valid.
    for ts_ms, value in reversed(series):
        if value is not None:
            measurement_at = datetime.utcfromtimestamp(ts_ms / 1000)
            logger.info(f"[{region}] Day-ahead price: {value:.2f} €/MWh at {measurement_at}")
            return float(value), measurement_at

    logger.warning(f"[{region}] SMARD series had no non-null price values.")
    return None, None


def build_smard_row(
    city: str, country: str, zone: str,
    price: Optional[float], measurement_at: Optional[datetime],
    ingested_at: datetime,
) -> dict:
    """Build a Bronze energy row for a SMARD-sourced zone."""
    return {
        "zone":                    zone,
        "country":                 country,
        "city":                    city,
        "carbon_intensity":        None,   # SMARD does not provide carbon intensity
        "fossil_free_pct":         None,   # Will be added when ENTSO-E is connected
        "renewable_pct":           None,   # Will be added when ENTSO-E is connected
        "day_ahead_price_eur_mwh": price,
        "measurement_at":          measurement_at or ingested_at,
        "ingested_at":             ingested_at,
        "ingested_date":           ingested_at.date(),
        "source":                  SOURCE_SMARD,
    }

# --------------------------------------------------------------------------- #
# Delta write helpers
# --------------------------------------------------------------------------- #

def write_bronze_energy(spark: SparkSession, rows: list[dict], ingested_date: date) -> int:
    """Write parsed energy rows to the Bronze Delta table, overwriting today's partition."""
    df = spark.createDataFrame(rows, schema=ENERGY_SCHEMA)
    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingested_date = '{ingested_date}'")
        .partitionBy(*PARTITION_COLS)
        .save(BRONZE_ENERGY_PATH)
    )
    logger.info(f"Wrote {row_count} rows to {BRONZE_ENERGY_PATH}")
    return row_count


def register_hive_table(spark: SparkSession) -> None:
    """Register the Delta table in the Hive Metastore so dbt sources can query it by name."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS default.bronze_energy
        USING DELTA
        LOCATION '{BRONZE_ENERGY_PATH}'
    """)
    logger.info("Registered default.bronze_energy in Hive Metastore")


def write_audit_row(
    spark: SparkSession,
    city: str,
    row_count: int,
    ingested_at: datetime,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """Append a single audit row to the Bronze audit log."""
    source = SOURCE_UK if city == "London" else SOURCE_SMARD
    audit_row = [{
        "source":        source,
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
    """Ingest energy data from UK Carbon Intensity API and SMARD."""
    spark = SparkSession.builder.getOrCreate()
    ingested_at   = datetime.utcnow()
    ingested_date = ingested_at.date()
    all_rows: list[dict] = []

    logger.info(f"Starting energy ingestion run at {ingested_at.isoformat()}")
    logger.info("Sources: UK Carbon Intensity API (GB) + SMARD (DE, AT)")

    # ------------------------------------------------------------------ #
    # Source 1: UK Carbon Intensity API — London / GB
    # ------------------------------------------------------------------ #
    try:
        row = get_uk_energy_row(ingested_at)
        all_rows.append(row)
        write_audit_row(spark, "London", 1, ingested_at, "SUCCESS")
    except Exception as exc:
        logger.error(f"[GB] UK Carbon Intensity ingestion failed: {exc}")
        write_audit_row(spark, "London", 0, ingested_at, "FAILED", str(exc))

    # ------------------------------------------------------------------ #
    # Source 2: SMARD — Berlin / DE
    # ------------------------------------------------------------------ #
    try:
        price_de, measurement_at_de = get_smard_day_ahead_price("DE")
        row_de = build_smard_row(
            city="Berlin", country="DE", zone="DE",
            price=price_de, measurement_at=measurement_at_de,
            ingested_at=ingested_at,
        )
        all_rows.append(row_de)
        write_audit_row(spark, "Berlin", 1, ingested_at, "SUCCESS")
    except Exception as exc:
        logger.error(f"[DE] SMARD ingestion failed: {exc}")
        write_audit_row(spark, "Berlin", 0, ingested_at, "FAILED", str(exc))

    # ------------------------------------------------------------------ #
    # Source 2: SMARD — Vienna / AT
    # AT data availability on SMARD is limited — treat failures as non-fatal.
    # ------------------------------------------------------------------ #
    try:
        price_at, measurement_at_at = get_smard_day_ahead_price("AT")
        row_at = build_smard_row(
            city="Vienna", country="AT", zone="AT",
            price=price_at, measurement_at=measurement_at_at,
            ingested_at=ingested_at,
        )
        all_rows.append(row_at)
        write_audit_row(spark, "Vienna", 1, ingested_at, "SUCCESS")
    except Exception as exc:
        # AT is best-effort — log warning rather than error; DE is the primary SMARD zone.
        logger.warning(f"[AT] SMARD ingestion failed (non-fatal): {exc}")
        write_audit_row(spark, "Vienna", 0, ingested_at, "FAILED", str(exc))

    if not all_rows:
        raise RuntimeError("No rows collected from any zone — aborting Bronze write.")

    total_rows = write_bronze_energy(spark, all_rows, ingested_date)
    register_hive_table(spark)

    write_audit_row(spark, "ALL_ZONES", total_rows, ingested_at, "SUCCESS")
    logger.info(f"Energy ingestion complete. Total rows written: {total_rows}")


main()
