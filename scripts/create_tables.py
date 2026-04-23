"""
create_tables.py
----------------
Creates all pipeline tables in both Databricks (Unity Catalog Delta) and
PostgreSQL in a single local run.

Databricks side: driven via Spark Connect (databricks-connect) — the same
connection used by export_to_postgres.py.  All statements use
CREATE TABLE IF NOT EXISTS so this script is fully idempotent.

PostgreSQL side: driven via psycopg2 with proper column types (FLOAT8,
TIMESTAMP, DATE, etc.) instead of the TEXT-only fallback in the export script.

Prerequisites:
  pip install psycopg2-binary databricks-connect

Usage:
  export PG_PASSWORD=your_postgres_password
  python scripts/create_tables.py

Environment variables:
  PG_PASSWORD       — required, never hardcoded
  PG_HOST           — optional, defaults to localhost
  PG_PORT           — optional, defaults to 5432
  PG_DATABASE       — optional, defaults to weather_energy_db
  PG_USER           — optional, defaults to postgres
  PG_SCHEMA         — optional, defaults to weather_energy
  DATABRICKS_SCHEMA — optional, defaults to weather_energy_dev
  DATABRICKS_CATALOG— optional, defaults to main
"""

import logging
import os
import sys

import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Configuration — loaded from environment variables only.
# --------------------------------------------------------------------------- #
PG_CONFIG: dict = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DATABASE", "weather_energy_db"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD"),
}
PG_SCHEMA: str       = os.getenv("PG_SCHEMA",          "weather_energy")
DBT_SCHEMA: str      = os.getenv("DATABRICKS_SCHEMA",   "weather_energy_dev")
DBT_CATALOG: str     = os.getenv("DATABRICKS_CATALOG",  "main")
QUALIFIED: str       = f"{DBT_CATALOG}.{DBT_SCHEMA}"


# --------------------------------------------------------------------------- #
# Databricks DDL — one entry per table.
# Keys: table name → CREATE TABLE IF NOT EXISTS body (columns + USING DELTA).
# Partition clauses are appended separately where needed.
# --------------------------------------------------------------------------- #

# fmt: off
DATABRICKS_DDL: list[dict] = [
    # ── Bronze ─────────────────────────────────────────────────────────────── #
    {
        "table": "bronze_weather",
        "ddl": """
            city           STRING    NOT NULL,
            country        STRING    NOT NULL,
            latitude       DOUBLE    NOT NULL,
            longitude      DOUBLE    NOT NULL,
            timezone       STRING    NOT NULL,
            hour           TIMESTAMP NOT NULL,
            temperature_2m DOUBLE,
            windspeed_10m  DOUBLE,
            weathercode    INT,
            precipitation  DOUBLE,
            ingested_at    TIMESTAMP NOT NULL,
            ingested_date  DATE      NOT NULL,
            source         STRING    NOT NULL
        """,
        "partition": "country, ingested_date",
    },
    {
        "table": "bronze_energy",
        "ddl": """
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
        """,
        "partition": "country, ingested_date",
    },
    {
        "table": "bronze_audit",
        "ddl": """
            source        STRING    NOT NULL,
            city          STRING    NOT NULL,
            row_count     INT       NOT NULL,
            ingested_at   TIMESTAMP NOT NULL,
            status        STRING    NOT NULL,
            error_message STRING
        """,
        "partition": None,
    },
    {
        "table": "pipeline_run_log",
        "ddl": """
            run_id     STRING    NOT NULL,
            started_at TIMESTAMP NOT NULL,
            ended_at   TIMESTAMP,
            status     STRING    NOT NULL,
            error      STRING
        """,
        "partition": None,
    },
    # ── Silver ─────────────────────────────────────────────────────────────── #
    {
        "table": "stg_weather_hourly",
        "ddl": """
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
        """,
        "partition": None,
    },
    {
        "table": "stg_energy_intensity",
        "ddl": """
            energy_id               STRING    NOT NULL,
            zone                    STRING    NOT NULL,
            country                 STRING    NOT NULL,
            city                    STRING    NOT NULL,
            carbon_intensity_gco2eq DOUBLE,
            fossil_free_pct         DOUBLE,
            renewable_pct           DOUBLE,
            day_ahead_price_eur_mwh DOUBLE,
            measurement_hour        TIMESTAMP,
            _source                 STRING,
            _ingested_at            TIMESTAMP,
            _loaded_at              TIMESTAMP
        """,
        "partition": None,
    },
    # ── Gold — Dimensions ───────────────────────────────────────────────────── #
    {
        "table": "dim_weather_condition",
        "ddl": """
            weathercode        INT    NOT NULL,
            condition_label    STRING NOT NULL,
            condition_category STRING NOT NULL,
            severity           STRING NOT NULL
        """,
        "partition": None,
    },
    {
        "table": "dim_time",
        "ddl": """
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
        """,
        "partition": None,
    },
    {
        "table": "dim_location",
        "ddl": """
            location_key     STRING    NOT NULL,
            city             STRING    NOT NULL,
            country          STRING    NOT NULL,
            latitude         DOUBLE    NOT NULL,
            longitude        DOUBLE    NOT NULL,
            timezone         STRING    NOT NULL,
            electricity_zone STRING    NOT NULL,
            dbt_valid_from   TIMESTAMP,
            dbt_valid_to     TIMESTAMP,
            dbt_updated_at   TIMESTAMP
        """,
        "partition": None,
    },
    # ── Gold — Fact ─────────────────────────────────────────────────────────── #
    {
        "table": "fact_energy_readings",
        "ddl": """
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
        """,
        "partition": None,
    },
    # ── Gold — Aggregates ───────────────────────────────────────────────────── #
    {
        "table": "agg_daily_energy",
        "ddl": """
            city                    STRING NOT NULL,
            reading_date            DATE   NOT NULL,
            avg_temperature         DOUBLE,
            min_temperature         DOUBLE,
            max_temperature         DOUBLE,
            total_precipitation_mm  DOUBLE,
            avg_windspeed           DOUBLE,
            avg_carbon_intensity    DOUBLE,
            avg_renewable_pct       DOUBLE,
            avg_fossil_free_pct     DOUBLE,
            reading_count           INT,
            missing_energy_readings INT
        """,
        "partition": None,
    },
    {
        "table": "rpt_weather_energy_correlation",
        "ddl": """
            city                 STRING NOT NULL,
            year                 INT    NOT NULL,
            month                INT    NOT NULL,
            avg_temperature      DOUBLE,
            min_temperature      DOUBLE,
            max_temperature      DOUBLE,
            avg_carbon_intensity DOUBLE,
            avg_renewable_pct    DOUBLE,
            avg_fossil_free_pct  DOUBLE,
            avg_precipitation    DOUBLE,
            dominant_condition   STRING,
            correlation_label    STRING
        """,
        "partition": None,
    },
]
# fmt: on


# --------------------------------------------------------------------------- #
# PostgreSQL column type mapping — mirrors the Delta schema with proper types.
# --------------------------------------------------------------------------- #

# Maps Spark/Delta type keywords → PostgreSQL types.
_TYPE_MAP: dict[str, str] = {
    "STRING":    "TEXT",
    "DOUBLE":    "FLOAT8",
    "INT":       "INTEGER",
    "BOOLEAN":   "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "DATE":      "DATE",
}


def _delta_col_to_pg(col_def_line: str) -> str:
    """Convert a single 'col_name TYPE [NOT NULL]' line to PostgreSQL syntax."""
    parts = col_def_line.strip().split()
    if len(parts) < 2:
        return col_def_line.strip()
    col_name = parts[0]
    delta_type = parts[1].upper()
    constraints = " ".join(parts[2:])  # e.g. "NOT NULL"
    pg_type = _TYPE_MAP.get(delta_type, "TEXT")
    line = f'"{col_name}" {pg_type}'
    if constraints:
        line += f" {constraints}"
    return line


def _build_pg_ddl(table: str, delta_col_block: str) -> str:
    """Build a PostgreSQL CREATE TABLE IF NOT EXISTS statement."""
    pg_cols = [
        _delta_col_to_pg(line)
        for line in delta_col_block.strip().splitlines()
        if line.strip()
    ]
    col_block = ",\n    ".join(pg_cols)
    return (
        f'CREATE TABLE IF NOT EXISTS "{PG_SCHEMA}"."{table}" (\n'
        f"    {col_block}\n"
        f");"
    )


# --------------------------------------------------------------------------- #
# Databricks helpers
# --------------------------------------------------------------------------- #

def create_databricks_tables(spark: SparkSession) -> None:
    """Create all Delta tables in Unity Catalog using CREATE TABLE IF NOT EXISTS."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUALIFIED}")
    logger.info(f"Schema {QUALIFIED} ensured.")

    for entry in DATABRICKS_DDL:
        tbl = entry["table"]
        partition_clause = (
            f"PARTITIONED BY ({entry['partition']})" if entry["partition"] else ""
        )
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {QUALIFIED}.{tbl} (\n"
            f"{entry['ddl']}\n"
            f") USING DELTA {partition_clause}"
        )
        spark.sql(ddl)
        logger.info(f"[Databricks] Table {QUALIFIED}.{tbl} ready.")

    # Verification: list tables.
    rows = spark.sql(f"SHOW TABLES IN {QUALIFIED}").collect()
    logger.info(f"[Databricks] {len(rows)} tables visible in {QUALIFIED}.")


# --------------------------------------------------------------------------- #
# PostgreSQL helpers
# --------------------------------------------------------------------------- #

def get_pg_connection() -> psycopg2.extensions.connection:
    """Open a PostgreSQL connection. Raises if PG_PASSWORD is missing."""
    if not PG_CONFIG["password"]:
        raise EnvironmentError(
            "PG_PASSWORD environment variable is not set.\n"
            "Run: export PG_PASSWORD=your_password"
        )
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    return conn


def create_postgres_tables(conn: psycopg2.extensions.connection) -> None:
    """Create the PostgreSQL schema and all tables with proper column types."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(PG_SCHEMA)
            )
        )
        logger.info(f"[PostgreSQL] Schema '{PG_SCHEMA}' ensured.")

        for entry in DATABRICKS_DDL:
            tbl = entry["table"]
            pg_ddl = _build_pg_ddl(tbl, entry["ddl"])
            cur.execute(pg_ddl)
            logger.info(f"[PostgreSQL] Table {PG_SCHEMA}.{tbl} ready.")

    conn.commit()

    # Verification: count tables in schema.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (PG_SCHEMA,),
        )
        count = cur.fetchone()[0]
    logger.info(f"[PostgreSQL] {count} tables visible in schema '{PG_SCHEMA}'.")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> None:
    """Create all tables in Databricks and PostgreSQL."""
    logger.info("=== create_tables.py ===")
    logger.info(f"Databricks target : {QUALIFIED}")
    logger.info(f"PostgreSQL target  : {PG_CONFIG['host']}:{PG_CONFIG['port']}"
                f"/{PG_CONFIG['database']}.{PG_SCHEMA}")

    # ── Databricks ────────────────────────────────────────────────────────── #
    logger.info("--- Databricks: creating Delta tables ---")
    spark = SparkSession.builder.getOrCreate()
    create_databricks_tables(spark)

    # ── PostgreSQL ────────────────────────────────────────────────────────── #
    logger.info("--- PostgreSQL: creating tables ---")
    conn = get_pg_connection()
    try:
        create_postgres_tables(conn)
    finally:
        conn.close()

    logger.info("=== Done. All tables provisioned in both systems. ===")
    logger.info("Next steps:")
    logger.info("  1. Run notebooks 01 and 02 in Databricks to populate Bronze")
    logger.info("  2. dbt seed && dbt snapshot && dbt run --select silver.* gold.*")
    logger.info("  3. python scripts/export_to_postgres.py  — sync data to PostgreSQL")


if __name__ == "__main__":
    main()
