# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Setup: Create All Unity Catalog Tables
# MAGIC
# MAGIC One-time DDL notebook that provisions every managed Delta table used by this pipeline.
# MAGIC Safe to re-run — all statements use `CREATE TABLE IF NOT EXISTS`.
# MAGIC
# MAGIC **Run order:** Run this notebook **once** before the first ingestion run or `dbt run`.
# MAGIC
# MAGIC Tables created:
# MAGIC | Layer | Tables |
# MAGIC |---|---|
# MAGIC | Bronze | `bronze_weather`, `bronze_energy`, `bronze_audit`, `pipeline_run_log` |
# MAGIC | Silver | `stg_weather_hourly`, `stg_energy_intensity` |
# MAGIC | Gold | `dim_weather_condition`, `dim_time`, `dim_location`, `fact_energy_readings`, `agg_daily_energy`, `rpt_weather_energy_correlation` |

# COMMAND ----------
# Runtime configuration — catalog and schema from widget bar.
# Defaults match the standard Unity Catalog layout for this project.
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "weather_energy_dev")

CATALOG: str = dbutils.widgets.get("catalog")
SCHEMA:  str = dbutils.widgets.get("schema")

# COMMAND ----------

import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

QUALIFIED: str = f"{CATALOG}.{SCHEMA}"
logger.info(f"Target catalog/schema: {QUALIFIED}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0 — Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUALIFIED}")
logger.info(f"Schema {QUALIFIED} ready.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 — Bronze Tables

# COMMAND ----------

# bronze_weather — raw hourly weather from Open-Meteo, partitioned by country + date.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.bronze_weather (
    city           STRING  NOT NULL,
    country        STRING  NOT NULL,
    latitude       DOUBLE  NOT NULL,
    longitude      DOUBLE  NOT NULL,
    timezone       STRING  NOT NULL,
    hour           TIMESTAMP NOT NULL,
    temperature_2m DOUBLE,
    windspeed_10m  DOUBLE,
    weathercode    INT,
    precipitation  DOUBLE,
    ingested_at    TIMESTAMP NOT NULL,
    ingested_date  DATE      NOT NULL,
    source         STRING    NOT NULL
)
USING DELTA
PARTITIONED BY (country, ingested_date)
""")
logger.info(f"Table {QUALIFIED}.bronze_weather ready.")

# COMMAND ----------

# bronze_energy — raw energy/carbon data from UK Carbon Intensity API and SMARD.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.bronze_energy (
    zone                    STRING    NOT NULL,
    country                 STRING    NOT NULL,
    city                    STRING    NOT NULL,
    carbon_intensity        DOUBLE,
    fossil_free_pct         DOUBLE,
    renewable_pct           DOUBLE,
    day_ahead_price_eur_mwh DOUBLE,
    measurement_at          TIMESTAMP,
    ingested_at             TIMESTAMP NOT NULL,
    ingested_date           DATE      NOT NULL,
    source                  STRING    NOT NULL
)
USING DELTA
PARTITIONED BY (country, ingested_date)
""")
logger.info(f"Table {QUALIFIED}.bronze_energy ready.")

# COMMAND ----------

# bronze_audit — one row per city per ingestion run for lineage and alerting.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.bronze_audit (
    source        STRING    NOT NULL,
    city          STRING    NOT NULL,
    row_count     INT       NOT NULL,
    ingested_at   TIMESTAMP NOT NULL,
    status        STRING    NOT NULL,
    error_message STRING
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.bronze_audit ready.")

# COMMAND ----------

# pipeline_run_log — orchestrator run history; used for idempotency guard.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.pipeline_run_log (
    run_id     STRING    NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at   TIMESTAMP,
    status     STRING    NOT NULL,
    error      STRING
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.pipeline_run_log ready.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 — Silver Tables
# MAGIC
# MAGIC Empty stubs with the correct schema. dbt incremental merge will populate these.
# MAGIC Pre-creating them avoids a dbt "table not found" error on the very first `dbt run`.

# COMMAND ----------

# stg_weather_hourly — deduplicated hourly weather with surrogate key.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.stg_weather_hourly (
    weather_id       STRING    NOT NULL,
    city             STRING    NOT NULL,
    country          STRING    NOT NULL,
    latitude         DOUBLE    NOT NULL,
    longitude        DOUBLE    NOT NULL,
    timezone         STRING    NOT NULL,
    observation_hour TIMESTAMP NOT NULL,
    temperature_2m   DOUBLE,
    windspeed_10m    DOUBLE,
    weathercode      INT,
    precipitation    DOUBLE,
    _source          STRING,
    _ingested_at     TIMESTAMP,
    _loaded_at       TIMESTAMP
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.stg_weather_hourly ready.")

# COMMAND ----------

# stg_energy_intensity — deduplicated energy/carbon with surrogate key.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.stg_energy_intensity (
    energy_id                STRING    NOT NULL,
    zone                     STRING    NOT NULL,
    country                  STRING    NOT NULL,
    city                     STRING    NOT NULL,
    carbon_intensity_gco2eq  DOUBLE,
    fossil_free_pct          DOUBLE,
    renewable_pct            DOUBLE,
    day_ahead_price_eur_mwh  DOUBLE,
    measurement_hour         TIMESTAMP,
    _source                  STRING,
    _ingested_at             TIMESTAMP,
    _loaded_at               TIMESTAMP
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.stg_energy_intensity ready.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 — Gold Tables
# MAGIC
# MAGIC Empty stubs. dbt will replace non-incremental tables (`dim_*`, `agg_*`, `rpt_*`)
# MAGIC with `CREATE OR REPLACE TABLE` on first run. The incremental `fact_energy_readings`
# MAGIC will merge into this empty stub correctly.

# COMMAND ----------

# dim_weather_condition — static WMO code lookup (20 rows).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.dim_weather_condition (
    weathercode        INT    NOT NULL,
    condition_label    STRING NOT NULL,
    condition_category STRING NOT NULL,
    severity           STRING NOT NULL
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.dim_weather_condition ready.")

# COMMAND ----------

# dim_time — generated hourly time spine 2024–2026 (~17,520 rows).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.dim_time (
    time_key        STRING    NOT NULL,
    full_timestamp  TIMESTAMP NOT NULL,
    year            INT       NOT NULL,
    month           INT       NOT NULL,
    day             INT       NOT NULL,
    hour            INT       NOT NULL,
    day_of_week     STRING    NOT NULL,
    day_of_week_num INT       NOT NULL,
    is_weekend      BOOLEAN   NOT NULL,
    quarter         INT       NOT NULL,
    month_name      STRING    NOT NULL
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.dim_time ready.")

# COMMAND ----------

# dim_location — current SCD2 city/region dimension (10 rows).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.dim_location (
    location_key   STRING    NOT NULL,
    city           STRING    NOT NULL,
    country        STRING    NOT NULL,
    latitude       DOUBLE    NOT NULL,
    longitude      DOUBLE    NOT NULL,
    timezone       STRING    NOT NULL,
    electricity_zone STRING  NOT NULL,
    dbt_valid_from TIMESTAMP,
    dbt_valid_to   TIMESTAMP,
    dbt_updated_at TIMESTAMP
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.dim_location ready.")

# COMMAND ----------

# fact_energy_readings — central hourly fact (incremental merge).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.fact_energy_readings (
    fact_id                 STRING    NOT NULL,
    location_key            STRING    NOT NULL,
    time_key                STRING    NOT NULL,
    weathercode_key         INT,
    temperature_2m          DOUBLE,
    windspeed_10m           DOUBLE,
    precipitation           DOUBLE,
    carbon_intensity        DOUBLE,
    fossil_free_pct         DOUBLE,
    renewable_pct           DOUBLE,
    day_ahead_price_eur_mwh DOUBLE,
    weather_ingested_at     TIMESTAMP,
    energy_ingested_at      TIMESTAMP,
    _loaded_at              TIMESTAMP
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.fact_energy_readings ready.")

# COMMAND ----------

# agg_daily_energy — daily rollup per city for dashboards.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.agg_daily_energy (
    city                   STRING  NOT NULL,
    reading_date           DATE    NOT NULL,
    avg_temperature        DOUBLE,
    min_temperature        DOUBLE,
    max_temperature        DOUBLE,
    total_precipitation_mm DOUBLE,
    avg_windspeed          DOUBLE,
    avg_carbon_intensity   DOUBLE,
    avg_renewable_pct      DOUBLE,
    avg_fossil_free_pct    DOUBLE,
    reading_count          INT,
    missing_energy_readings INT
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.agg_daily_energy ready.")

# COMMAND ----------

# rpt_weather_energy_correlation — monthly cross-domain correlation report.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED}.rpt_weather_energy_correlation (
    city                   STRING  NOT NULL,
    year                   INT     NOT NULL,
    month                  INT     NOT NULL,
    avg_temperature        DOUBLE,
    min_temperature        DOUBLE,
    max_temperature        DOUBLE,
    avg_carbon_intensity   DOUBLE,
    avg_renewable_pct      DOUBLE,
    avg_fossil_free_pct    DOUBLE,
    avg_precipitation      DOUBLE,
    dominant_condition     STRING,
    correlation_label      STRING
)
USING DELTA
""")
logger.info(f"Table {QUALIFIED}.rpt_weather_energy_correlation ready.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 — Verification

# COMMAND ----------

tables_df = spark.sql(f"SHOW TABLES IN {QUALIFIED}")
tables_df.show(truncate=False)

print("\n── Row counts per table ──")
all_tables = [
    "bronze_weather", "bronze_energy", "bronze_audit", "pipeline_run_log",
    "stg_weather_hourly", "stg_energy_intensity",
    "dim_weather_condition", "dim_time", "dim_location",
    "fact_energy_readings", "agg_daily_energy", "rpt_weather_energy_correlation",
]
for tbl in all_tables:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {QUALIFIED}.{tbl}").collect()[0]["n"]
    print(f"  {tbl:<40} {count:>8} rows")

print(f"\n✓ All {len(all_tables)} tables provisioned in {QUALIFIED}")
print("Next steps:")
print("  1. Run 01_bronze_weather and 02_bronze_energy to populate Bronze")
print("  2. Run: dbt seed && dbt snapshot && dbt run --select silver.* gold.*")
print("  3. Run: python scripts/export_to_postgres.py  (or scripts/create_tables.py)")
