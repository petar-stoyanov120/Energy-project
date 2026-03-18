# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Orchestrator
# MAGIC
# MAGIC Runs the Bronze ingestion notebooks in sequence with idempotency protection.
# MAGIC
# MAGIC **Idempotency guard:** Checks the pipeline run log for a `RUNNING` status from the
# MAGIC last 2 hours before starting. If one exists, the run is aborted to prevent
# MAGIC concurrent duplicate runs (e.g. if triggered twice by accident).
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. `01_bronze_weather.py` — ~1 minute (Open-Meteo, 10 cities)
# MAGIC 2. `02_bronze_energy.py` — ~1 minute (UK Carbon Intensity + SMARD, no rate limiting)
# MAGIC
# MAGIC **Note:** `%run` is used instead of `dbutils.notebook.run()` — the latter is not
# MAGIC supported on Databricks Free Edition serverless compute.

# COMMAND ----------
# Runtime configuration — set catalog and schema in the widget bar before running.
# Sub-notebooks (01, 02) define the same widgets with the same defaults, so they
# will use the correct Unity Catalog location automatically when %run'd.
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema",  "weather_energy_dev")

CATALOG: str = dbutils.widgets.get("catalog")
SCHEMA:  str = dbutils.widgets.get("schema")

# COMMAND ----------

import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
# Run log schema — tracks orchestrator-level runs.
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
        df = spark.table(f"{CATALOG}.{SCHEMA}.pipeline_run_log")
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
    """Append a row to the orchestrator run log managed table."""
    row = [{
        "run_id":     run_id,
        "started_at": started_at,
        "ended_at":   ended_at,
        "status":     status,
        "error":      error,
    }]
    df = spark.createDataFrame(row, schema=RUN_LOG_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.pipeline_run_log")

# COMMAND ----------
# --- Orchestration start: idempotency check + mark RUNNING ---

spark      = SparkSession.builder.getOrCreate()
started_at = datetime.utcnow()
run_id     = started_at.strftime("%Y%m%d_%H%M%S")

logger.info(f"Orchestrator starting. run_id={run_id}")

if is_pipeline_running(spark):
    msg = (
        f"A pipeline run is already in RUNNING status within the last "
        f"{RUNNING_CHECK_WINDOW_HOURS}h. Aborting to prevent duplicate ingestion."
    )
    logger.error(msg)
    raise RuntimeError(msg)

write_run_status(spark, run_id, started_at, status="RUNNING")
logger.info(f"Marked run {run_id} as RUNNING.")

# COMMAND ----------
# MAGIC %run ./01_bronze_weather

# COMMAND ----------
# MAGIC %run ./02_bronze_energy

# COMMAND ----------
# --- Mark pipeline as SUCCESS ---

ended_at     = datetime.utcnow()
duration_min = (ended_at - started_at).seconds // 60
write_run_status(spark, run_id, started_at, status="SUCCESS", ended_at=ended_at)
logger.info(f"Pipeline completed successfully in ~{duration_min} minutes. run_id={run_id}")
