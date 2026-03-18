"""
export_to_postgres.py
---------------------
Reads Gold Delta tables from Databricks DBFS and writes them to a local
PostgreSQL database using psycopg2 + pandas.

No JDBC required — this runs locally after Gold models are built in Databricks.

Prerequisites:
  pip install psycopg2-binary pandas databricks-connect

Usage:
  export PG_PASSWORD=your_postgres_password
  python scripts/export_to_postgres.py

Environment variables:
  PG_PASSWORD  — required, PostgreSQL password (never hardcoded)
  PG_HOST      — optional, defaults to localhost
  PG_PORT      — optional, defaults to 5432
  PG_DATABASE  — optional, defaults to weather_energy_db
  PG_USER      — optional, defaults to postgres
  PG_SCHEMA    — optional, defaults to weather_energy
  DATABRICKS_SCHEMA — optional, dbt schema where Gold tables live, defaults to weather_energy_dev
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

import pandas as pd
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
# PG_PASSWORD is mandatory and must never be hardcoded.
# --------------------------------------------------------------------------- #
PG_CONFIG: dict = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_DATABASE", "weather_energy_db"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD"),   # Required — no default.
}

PG_SCHEMA: str       = os.getenv("PG_SCHEMA",         "weather_energy")
DBT_SCHEMA: str      = os.getenv("DATABRICKS_SCHEMA",  "weather_energy_dev")

# Gold tables to export, in dependency order.
GOLD_TABLES: list[str] = [
    "dim_weather_condition",
    "dim_time",
    "dim_location",
    "fact_energy_readings",
    "agg_daily_energy",
    "rpt_weather_energy_correlation",
]


# --------------------------------------------------------------------------- #
# PostgreSQL helpers
# --------------------------------------------------------------------------- #

def get_pg_connection() -> psycopg2.extensions.connection:
    """Open and return a PostgreSQL connection. Raises if PG_PASSWORD is missing."""
    if not PG_CONFIG["password"]:
        raise EnvironmentError(
            "PG_PASSWORD environment variable is not set. "
            "Run: export PG_PASSWORD=your_password"
        )
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    return conn


def ensure_schema(conn: psycopg2.extensions.connection) -> None:
    """Create the PostgreSQL schema if it doesn't already exist."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(PG_SCHEMA)
            )
        )
    conn.commit()
    logger.info(f"Schema '{PG_SCHEMA}' is ready.")


def write_dataframe_to_pg(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    table_name: str,
) -> int:
    """Truncate and reload a single PostgreSQL table from a pandas DataFrame.

    Uses TRUNCATE + INSERT for simplicity. For large tables, consider
    psycopg2.extras.execute_values or COPY FROM STDIN for better performance.
    """
    qualified_table = f"{PG_SCHEMA}.{table_name}"

    with conn.cursor() as cur:
        # Truncate to replace stale data on each export run.
        cur.execute(
            sql.SQL("TRUNCATE TABLE IF EXISTS {}.{} RESTART IDENTITY CASCADE").format(
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(table_name),
            )
        )

        # Create table from DataFrame schema if it doesn't exist.
        # pandas.DataFrame.to_sql would need SQLAlchemy — we do it manually to
        # avoid the extra dependency.
        cols = list(df.columns)
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols)

        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(PG_SCHEMA),
                sql.Identifier(table_name),
                sql.SQL(col_defs),
            )
        )

        if df.empty:
            logger.warning(f"[{table_name}] DataFrame is empty — table truncated, nothing inserted.")
            conn.commit()
            return 0

        # Batch insert all rows.
        rows = [tuple(str(v) if v is not None else None for v in row) for row in df.itertuples(index=False)]
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = (
            f'INSERT INTO "{PG_SCHEMA}"."{table_name}" ({", ".join(f"{chr(34)}{c}{chr(34)}" for c in cols)}) '
            f"VALUES ({placeholders})"
        )
        cur.executemany(insert_sql, rows)

    conn.commit()
    return len(df)


# --------------------------------------------------------------------------- #
# Spark helpers
# --------------------------------------------------------------------------- #

def read_gold_table(spark: SparkSession, table_name: str) -> pd.DataFrame:
    """Read a Gold Delta table from Databricks and return it as a pandas DataFrame.

    Reads from the Hive Metastore table registered by dbt under DBT_SCHEMA.
    """
    full_table = f"main.{DBT_SCHEMA}.{table_name}"
    logger.info(f"Reading {full_table} from Databricks...")
    df_spark = spark.table(full_table)
    row_count = df_spark.count()
    logger.info(f"[{table_name}] {row_count} rows read from Databricks.")
    return df_spark.toPandas()


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> None:
    """Export all Gold tables from Databricks to PostgreSQL."""
    started_at = datetime.utcnow()
    logger.info(f"Export started at {started_at.isoformat()}")
    logger.info(f"Source: Databricks schema '{DBT_SCHEMA}'")
    logger.info(f"Target: PostgreSQL {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}.{PG_SCHEMA}")

    spark = SparkSession.builder.getOrCreate()
    conn  = get_pg_connection()

    try:
        ensure_schema(conn)

        total_rows_exported = 0
        for table_name in GOLD_TABLES:
            try:
                pdf = read_gold_table(spark, table_name)
                rows_written = write_dataframe_to_pg(conn, pdf, table_name)
                total_rows_exported += rows_written
                logger.info(f"[{table_name}] Exported {rows_written} rows to PostgreSQL.")
            except Exception as exc:
                logger.error(f"[{table_name}] Export failed: {exc}")
                conn.rollback()
                raise

    finally:
        conn.close()

    duration_sec = (datetime.utcnow() - started_at).seconds
    logger.info(f"Export complete. {total_rows_exported} total rows in {duration_sec}s.")
    logger.info("Connect Metabase to PostgreSQL at localhost:5432 to visualise the data.")


if __name__ == "__main__":
    main()
