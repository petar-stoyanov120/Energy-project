# CLAUDE.md — Weather & Energy Analytics Pipeline

## 1. Project Overview
End-to-end data engineering portfolio project ingesting real-time weather and energy data
across European cities. Processes raw API JSON through a Medallion architecture (Bronze →
Silver → Gold) using PySpark on Databricks Free Edition and dbt Core for transformation.
Serves analytical insights via PostgreSQL and Metabase.

**Key features:** Multi-source ingestion · SCD Type 2 dimensions · dbt incremental models ·
Star schema gold layer · Cross-domain analytics (weather ↔ energy correlation)

---

## 2. Tech Stack
- **Runtime:** Databricks Free Edition (x2.small fixed cluster — no customization)
- **Compute:** PySpark 3.x + Spark SQL
- **Transformation:** dbt Core (dbt-databricks adapter) in VS Code
- **Storage:** Delta Lake on native DBFS (`dbfs:/delta/bronze/`, `dbfs:/delta/silver/`, `dbfs:/delta/gold/`)
- **Serving DB:** PostgreSQL (local)
- **BI:** Metabase standalone JAR (free, no Docker required)
- **APIs:** Open-Meteo (no auth) · Electricity Maps (free tier, API key required)
- **Secrets:** Databricks `dbutils.secrets` — never hardcoded
- **Version control:** GitHub repo connected to Databricks workspace

> **Storage fallback:** If `dbfs:/delta/` paths are inaccessible, fall back to Unity Catalog
> managed volumes (`/Volumes/<catalog>/<schema>/<volume>/`) or managed Delta tables registered
> directly in the Hive Metastore without an explicit LOCATION clause.

---

## 3. Architecture Overview

```
APIs (Open-Meteo, Electricity Maps)
        ↓  PySpark notebooks
  BRONZE — raw JSON → Delta Lake on DBFS (partitioned by country, ingested_date)
        ↓  dbt + PySpark
  SILVER — cleaned, typed, deduped, SCD2 dims
        ↓  dbt models + Spark SQL
  GOLD   — star schema (fact_energy_readings, dim_location, dim_time, dim_weather_condition)
        ↓  psycopg2 + pandas export
  SERVING — PostgreSQL → Metabase
```

**Storage paths:**
```
dbfs:/delta/bronze/weather/     partitioned by country, ingested_date
dbfs:/delta/bronze/energy/      partitioned by country, ingested_date
dbfs:/delta/silver/             managed by dbt
dbfs:/delta/gold/               managed by dbt
dbfs:/delta/logs/bronze_audit/  audit log (unpartitioned)
```

**Folder structure:**
```
weather_energy_project/
├── notebooks/
│   ├── 01_bronze_weather.py      # Open-Meteo ingestion
│   ├── 02_bronze_energy.py       # Electricity Maps ingestion
│   └── 03_orchestrator.py        # Runs notebooks in sequence, logs run
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml              # LOCAL ONLY — never commit
│   ├── models/
│   │   ├── silver/               # stg_weather_hourly, stg_energy_intensity
│   │   └── gold/                 # fact_energy_readings, dim_*, agg_*, rpt_*
│   ├── snapshots/                # SCD2 snap_dim_location
│   ├── seeds/                    # regions.csv (city, country, lat, lon, timezone, zone)
│   ├── tests/                    # Custom data quality tests
│   └── schema.yml                # Sources, models, column-level tests
├── scripts/
│   └── export_to_postgres.py     # psycopg2 + pandas Gold → PostgreSQL
├── .claude/rules/                # Modular rules (see Section 7)
└── README.md
```

**WAT Framework:**
- **Workflows** (`notebooks/`): Bronze ingestion SOPs — one notebook per source, idempotent
- **Agent** (Claude Code): Orchestrates notebooks, writes dbt models, validates row counts
- **Tools** (`dbt_project/models/`): Reusable SQL transforms called from Silver/Gold workflows

---

## 4. Coding Conventions
- **Indentation:** 4 spaces for Python · 2 spaces for SQL/YAML/Markdown
- **Python names:** `snake_case` functions (`get_weather_data`, `write_bronze_delta`) ·
  `PascalCase` classes (`WeatherIngester`, `AuditLogger`) ·
  `UPPERCASE` constants (`MAX_RETRIES = 3`, `TIMEOUT_SECONDS = 30`, `PARTITION_COLS = ["country"]`)
- **SQL/dbt names:** `snake_case` throughout · prefix by layer (`stg_`, `dim_`, `fact_`, `agg_`, `rpt_`)
- **Async/IO:** Always wrap API calls in `try/except requests.exceptions.RequestException`
- **Retries:** Exponential backoff — `attempt 1: 2s · attempt 2: 4s · attempt 3: 8s` — then raise
- **Logging:** Use Python `logging` module — log ingestion start, row count, duration, errors
- **Type hints:** Required on all function signatures — `def get_weather(city: str, lat: float) -> dict:`
- **Comments:** Explain WHY, not WHAT. One-line docstring on every function.
- **Secrets:** Always `dbutils.secrets.get(scope="weather_energy", key="<key>")` — zero exceptions

---

## 5. Common Commands

```bash
# Run full pipeline (Databricks notebook)
/run   → Execute 03_orchestrator.py in Databricks UI or via API

# dbt commands (run from dbt_project/ directory)
/dbt-run-silver  → dbt run --select silver.*
/dbt-run-gold    → dbt run --select gold.*
/dbt-test        → dbt test --select silver.* gold.*
/dbt-seed        → dbt seed  (reload regions.csv)
/dbt-snapshot    → dbt snapshot  (refresh SCD2 dim_location)
/dbt-docs        → dbt docs generate && dbt docs serve

# Validation
/audit           → SELECT * FROM default.bronze_audit ORDER BY ingested_at DESC LIMIT 20
/row-counts      → Run row count check across all three layers
                   Bronze: spark.read.format("delta").load("dbfs:/delta/bronze/weather").count()
                   Silver: SELECT COUNT(*) FROM weather_energy_dev.stg_weather_hourly
                   Gold:   SELECT COUNT(*) FROM weather_energy_dev.fact_energy_readings

# Maintenance
/clear-bronze    → TRUNCATE bronze layer — requires explicit confirmation first
/clear-context   → Restart Databricks cluster to clear Spark cache
```

---

## 6. Constraints — Claude MUST NEVER

**Security:**
- Never hardcode API keys, tokens, or passwords anywhere in code or notebooks
- Never log API keys, tokens, email addresses, or raw credential strings
- Never commit `.env` files or `profiles.yml` with real credentials to GitHub

**Data integrity:**
- Never write to Silver without first validating Bronze row count > 0
- Never overwrite Gold fact table without taking a Delta snapshot (`DESCRIBE HISTORY`)
- Never skip deduplication in Silver (always apply `ROW_NUMBER() QUALIFY` window)
- Never write to a Delta table without specifying `partitionBy` on first write

**dbt:**
- Never create a dbt model without adding it to `schema.yml` with at least `not_null` + `unique` tests
- Never use `dbt run --full-refresh` on Gold in production without explicit user confirmation
- Never hardcode date filters — always use `{{ var('start_date') }}` or `is_incremental()` macro

**API / ingestion:**
- Never call Electricity Maps API more than 1 request/min (free tier rate limit)
- Never ingest without writing a row to `bronze_audit` (columns: `source`, `city`, `row_count`, `ingested_at`, `status`)
- Never skip schema enforcement with `StructType` on Bronze writes

**Infrastructure:**
- Never use Azure services (ADF, ADLS, AKS) — Databricks Free Edition only
- Never use `/mnt/` paths — always use native `dbfs:/delta/` paths (or managed volumes as fallback)
- Never attempt to mount external storage (Azure ADLS, S3, GCS) — not supported on Free Edition
- Never run orchestrator if previous run status in run log is `RUNNING` (implement idempotency)
- Never write files outside `dbfs:/delta/bronze/`, `dbfs:/delta/silver/`, `dbfs:/delta/gold/`, `dbfs:/delta/logs/`

---

## 7. Context & References

| Topic | Location |
|---|---|
| Error handling patterns | `.claude/rules/error-handling.md` |
| dbt testing standards | `.claude/rules/testing.md` |
| API call conventions | `.claude/rules/api-design.md` |
| Security & secrets | `.claude/rules/security.md` |
| Deployment & serving | `.claude/rules/deployment.md` |
| Bronze ingestion example | `notebooks/01_bronze_weather.py` |
| Silver dbt model example | `dbt_project/models/silver/stg_weather_hourly.sql` |
| Star schema definition | `dbt_project/models/gold/fact_energy_readings.sql` |
| City/region seed data | `dbt_project/seeds/regions.csv` |
| Architecture diagram | `README.md` (Section: Architecture) |

> This is a living document. Update when new APIs, models, or conventions are added.
> Commit every change to GitHub alongside the code it documents.
