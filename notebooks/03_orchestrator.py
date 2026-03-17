# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Orchestrator
# MAGIC
# MAGIC Runs the Bronze ingestion notebooks in sequence with idempotency protection.
# MAGIC
# MAGIC **Idempotency guard:** Checks the audit log for a `RUNNING` status from the
# MAGIC last 2 hours before starting. If one exists, the run is aborted to prevent
# MAGIC concurrent duplicate runs (e.g. if triggered twice by accident).
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. `01_bronze_weather.py` — ~1 minute (Open-Meteo, 10 cities)
# MAGIC 2. `02_bronze_energy.py` — ~1 minute (UK Carbon Intensity + SMARD, no rate limiting)
# MAGIC
# MAGIC **Timeouts:**
# MAGIC - Weather notebook: 300 seconds (5 minutes)
# MAGIC - Energy notebook:  300 seconds (5 minutes — no rate-limit sleep needed)

# COMMAND ----------

import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
AUDIT_LOG_PATH: str = "dbfs:/delta/logs/bronze_audit"
RUN_LOG_PATH:   str = "dbfs:/delta/logs/pipeline_runs"

WEATHER_NOTEBOOK:  str = "./01_bronze_weather"
ENERGY_NOTEBOOK:   str = "./02_bronze_energy"
WEATHER_TIMEOUT:   int = 300   # seconds
ENERGY_TIMEOUT:    int = 300   # seconds — UK Carbon Intensity + SMARD have no rate-limit sleep

# How far back to look when checking for a concurrent RUNNING status.
RUNNING_CHECK_WINDOW_HOURS: int = 2

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Run log schema — separate from bronze_audit; tracks orchestrator-level runs.
# --------------------------------------------------------------------------- #
RUN_LOG_SCHEMA = StructType([
    StructField("run_id",     StringType(),    nullable=False),
    StructField("started_at", TimestampType(), nullable=False),
    StructField("ended_at",   TimestampType(), nullable=True),
    StructField("status",     StringType(),    nullable=False),  # RUNNING | SUCCESS | FAILED
    StructField("error",      StringType(),    nullable=True),
])

# --------------------------------------------------------------------------- #
# Idempotency helpers
# --------------------------------------------------------------------------- #

def is_pipeline_running(spark: SparkSession) -> bool:
    """Return True if a pipeline run started within the last N hours is still RUNNING.

    Prevents concurrent or double-triggered runs from corrupting Bronze tables.
    """
    try:
        df = spark.read.format("delta").load(RUN_LOG_PATH)
        cutoff = datetime.utcnow() - timedelta(hours=RUNNING_CHECK_WINDOW_HOURS)
        running_count = (
            df.filter(
                (col("status") == "RUNNING") &
                (col("started_at") >= lit(cutoff))
            ).count()
        )
        return running_count > 0
    except Exception:
        # If the run log doesn't exist yet (first ever run), there's nothing running.
        return False


def write_run_status(
    spark: SparkSession, run_id: str, started_at: datetime,
    status: str, ended_at: datetime = None, error: str = None
) -> None:
    """Append or update the orchestrator run log."""
    row = [{
        "run_id":     run_id,
        "started_at": started_at,
        "ended_at":   ended_at,
        "status":     status,
        "error":      error,
    }]
    df = spark.createDataFrame(row, schema=RUN_LOG_SCHEMA)
    df.write.format("delta").mode("append").save(RUN_LOG_PATH)


# --------------------------------------------------------------------------- #
# Main execution
# --------------------------------------------------------------------------- #

def main() -> None:
    """Run the full Bronze ingestion pipeline with idempotency and audit logging."""
    spark = SparkSession.builder.getOrCreate()
    started_at = datetime.utcnow()
    run_id = started_at.strftime("%Y%m%d_%H%M%S")

    logger.info(f"Orchestrator starting. run_id={run_id}")

    # --- Idempotency guard ---------------------------------------------------
    if is_pipeline_running(spark):
        msg = (
            f"A pipeline run is already in RUNNING status within the last "
            f"{RUNNING_CHECK_WINDOW_HOURS}h. Aborting to prevent duplicate ingestion."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    # Mark this run as RUNNING before executing any notebooks.
    write_run_status(spark, run_id, started_at, status="RUNNING")

    try:
        # --- Step 1: Weather ingestion ---------------------------------------
        logger.info(f"Running {WEATHER_NOTEBOOK} (timeout={WEATHER_TIMEOUT}s)...")
        dbutils.notebook.run(WEATHER_NOTEBOOK, timeout_seconds=WEATHER_TIMEOUT)
        logger.info("Weather notebook completed successfully.")

        # --- Step 2: Energy ingestion ----------------------------------------
        logger.info(f"Running {ENERGY_NOTEBOOK} (timeout={ENERGY_TIMEOUT}s)...")
        logger.info("Sources: UK Carbon Intensity API (GB) + SMARD (DE, AT). No rate-limit sleep.")
        dbutils.notebook.run(ENERGY_NOTEBOOK, timeout_seconds=ENERGY_TIMEOUT)
        logger.info("Energy notebook completed successfully.")

        # --- Mark success ----------------------------------------------------
        ended_at = datetime.utcnow()
        write_run_status(spark, run_id, started_at, status="SUCCESS", ended_at=ended_at)
        duration_min = (ended_at - started_at).seconds // 60
        logger.info(f"Pipeline completed successfully in ~{duration_min} minutes. run_id={run_id}")

    except Exception as exc:
        ended_at = datetime.utcnow()
        error_msg = str(exc)
        logger.error(f"Pipeline FAILED: {error_msg}")
        write_run_status(spark, run_id, started_at, status="FAILED", ended_at=ended_at, error=error_msg)
        raise


main()
