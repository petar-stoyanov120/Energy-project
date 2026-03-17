# Weather & Energy Analytics Pipeline

An end-to-end data engineering portfolio project that ingests real-time weather and
carbon intensity data for 10 European capitals, processes it through a Medallion
architecture (Bronze → Silver → Gold), and serves analytical insights via PostgreSQL
and Metabase.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Data Model](#4-data-model)
5. [Folder Structure](#5-folder-structure)
6. [Prerequisites & One-Time Setup](#6-prerequisites--one-time-setup)
7. [Step-by-Step Setup Guide](#7-step-by-step-setup-guide)
8. [Running the Pipeline](#8-running-the-pipeline)
9. [Common Commands](#9-common-commands)
10. [Verification & Row Count Checks](#10-verification--row-count-checks)
11. [Troubleshooting](#11-troubleshooting)
12. [How to Extend the Project](#12-how-to-extend-the-project)

---

## 1. Project Overview

### What it does

- **Ingests** hourly weather forecasts from [Open-Meteo](https://open-meteo.com/) (free, no auth)
  and real-time carbon intensity from [Electricity Maps](https://electricitymaps.com/) (free tier)
- **Processes** raw data through three Delta Lake layers: Bronze (raw) → Silver (cleaned) → Gold (star schema)
- **Transforms** data using dbt Core with incremental models and SCD Type 2 dimensions
- **Exports** Gold tables to PostgreSQL for BI analysis in Metabase

### Cities covered

| City | Country | Electricity Zone |
|---|---|---|
| London | GB | GB |
| Berlin | DE | DE |
| Paris | FR | FR |
| Madrid | ES | ES |
| Rome | IT | IT |
| Amsterdam | NL | NL |
| Warsaw | PL | PL |
| Vienna | AT | AT |
| Stockholm | SE | SE |
| Lisbon | PT | PT |

### Key analytics enabled

- Carbon intensity trends per city over time
- Temperature vs. renewable energy percentage correlation
- Daily weather summaries with energy profile classification
- Weekend vs. weekday energy consumption patterns

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│                                                                     │
│  Open-Meteo API          Electricity Maps API                       │
│  (no auth, free)         (free tier, API key)                       │
│  hourly weather          carbon intensity / renewable %             │
└──────────────┬───────────────────────┬──────────────────────────────┘
               │                       │
               ▼                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE — Raw Ingestion                           │
│                                                                     │
│  01_bronze_weather.py        02_bronze_energy.py                    │
│  PySpark + StructType        PySpark + StructType                   │
│  Idempotent (replaceWhere)   Rate-limited (61s/zone)                │
│                                                                     │
│  dbfs:/delta/bronze/weather/    dbfs:/delta/bronze/energy/          │
│  partitioned by country, ingested_date                              │
│                                                                     │
│  Audit log → dbfs:/delta/logs/bronze_audit/                         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  dbt run --select silver.*
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SILVER — Cleaned & Deduplicated                  │
│                                                                     │
│  stg_weather_hourly              stg_energy_intensity               │
│  Incremental merge on weather_id  Incremental merge on energy_id    │
│  QUALIFY deduplication           QUALIFY deduplication              │
│  SHA-256 surrogate keys          SHA-256 surrogate keys             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  dbt snapshot + dbt run --select gold.*
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GOLD — Star Schema                               │
│                                                                     │
│  dim_location (SCD2)    dim_time       dim_weather_condition        │
│  dim_weather_condition  (17,520 rows)  (WMO 4677 lookup)            │
│                                                                     │
│  fact_energy_readings   agg_daily_energy   rpt_weather_energy_corr  │
│  (grain: city × hour)   (grain: city × day) (grain: city × month)  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  python scripts/export_to_postgres.py
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SERVING LAYER                                    │
│                                                                     │
│  PostgreSQL (local)                                                 │
│  Schema: weather_energy                                             │
│                                                                     │
│  Metabase (localhost:3000)                                          │
│  Free open-source BI, connects directly to PostgreSQL               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Runtime | Databricks Free Edition | x2.small fixed cluster |
| Compute | PySpark 3.x + Spark SQL | Available on Databricks Free |
| Storage | Delta Lake on DBFS | `dbfs:/delta/` — no external mounts |
| Transformation | dbt Core + dbt-databricks | Run locally via VS Code |
| Weather API | Open-Meteo | Free, no auth required |
| Energy API | Electricity Maps v3 | Free tier, API key required |
| Serving DB | PostgreSQL (local) | Any version ≥ 12 |
| BI | Metabase OSS (JAR) | Free, no Docker, `java -jar metabase.jar` |
| IDE | VS Code + dbt Power User | dbt extension for IntelliSense |
| Version control | GitHub | Connected to Databricks workspace |

---

## 4. Data Model

### Bronze layer

Raw, schema-enforced Delta tables. Never modified after ingestion.

```
bronze_weather (partition: country, ingested_date)
├── city, country, latitude, longitude, timezone
├── hour (observation timestamp)
├── temperature_2m, windspeed_10m, weathercode, precipitation
└── ingested_at, ingested_date, source

bronze_energy (partition: country, ingested_date)
├── zone, country, city
├── carbon_intensity, fossil_free_pct, renewable_pct, measurement_at
└── ingested_at, ingested_date, source

bronze_audit (unpartitioned — audit log)
└── source, city, row_count, ingested_at, status, error_message
```

### Silver layer

Cleaned, deduplicated, typed. Grain: one row per city per hour (weather) / zone per hour (energy).

```
stg_weather_hourly
├── weather_id (SHA-256 PK: city|observation_hour)
├── city, country, latitude, longitude, timezone
├── observation_hour, temperature_2m, windspeed_10m, weathercode, precipitation
└── _source, _ingested_at, _loaded_at

stg_energy_intensity
├── energy_id (SHA-256 PK: zone|ingested_at)
├── zone, country, city
├── carbon_intensity_gco2eq, fossil_free_pct, renewable_pct, measurement_hour
└── _source, _ingested_at, _loaded_at
```

### Gold layer — Star Schema

```
                    ┌─────────────────┐
                    │  dim_location   │◄── SCD2 via snap_dim_location
                    │  location_key   │
                    │  city, country  │
                    │  lat, lon, tz   │
                    │  electricity_zone│
                    └────────┬────────┘
                             │
┌─────────────┐    ┌─────────▼───────────────┐    ┌──────────────────────┐
│  dim_time   │◄───┤  fact_energy_readings   ├───►│ dim_weather_condition│
│  time_key   │    │  fact_id (PK)           │    │  weathercode (PK)    │
│  year/month │    │  location_key (FK)      │    │  condition_label     │
│  day/hour   │    │  time_key (FK)          │    │  condition_category  │
│  is_weekend │    │  weathercode_key (FK)   │    │  severity            │
└─────────────┘    │  temperature_2m         │    └──────────────────────┘
                   │  windspeed_10m          │
                   │  precipitation          │
                   │  carbon_intensity       │
                   │  fossil_free_pct        │
                   │  renewable_pct          │
                   └─────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
    ┌─────────▼──────────┐    ┌─────────────▼──────────────┐
    │  agg_daily_energy  │    │ rpt_weather_energy_corr    │
    │  grain: city×day   │    │ grain: city×month          │
    │  avg/max/min temp  │    │ avg_temp, avg_carbon_int   │
    │  total precip      │    │ avg_renewable_pct          │
    │  avg carbon/renew  │    │ temp_carbon_profile label  │
    └────────────────────┘    └────────────────────────────┘
```

**Dimension details:**

| Dimension | Rows | Materialization | Notes |
|---|---|---|---|
| `dim_location` | 10 | table | SCD2 — current records only |
| `dim_time` | ~17,520 | table | 2024–2025 at hourly grain |
| `dim_weather_condition` | 21 | table | WMO 4677 static lookup |

**Fact table grain:** one row per city per hour.

---

## 5. Folder Structure

```
weather_energy_project/
│
├── notebooks/                         # Databricks PySpark notebooks
│   ├── 01_bronze_weather.py           # Open-Meteo ingestion (10 cities)
│   ├── 02_bronze_energy.py            # Electricity Maps ingestion (10 zones, ~10 min)
│   └── 03_orchestrator.py            # Sequenced runner with idempotency guard
│
├── dbt_project/                       # dbt Core project (run locally)
│   ├── dbt_project.yml                # Project name, paths, materialization defaults
│   ├── profiles.yml                   # LOCAL ONLY — Databricks connection (gitignored)
│   ├── .gitignore                     # Excludes profiles.yml, target/, logs/
│   ├── schema.yml                     # All sources, models, column-level tests
│   │
│   ├── seeds/
│   │   └── regions.csv                # 10 European cities with lat/lon/timezone/zone
│   │
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_weather_hourly.sql     # Incremental, deduplicated weather Silver
│   │   │   └── stg_energy_intensity.sql   # Incremental, deduplicated energy Silver
│   │   └── gold/
│   │       ├── dim_location.sql           # Current-state city dimension (from SCD2 snapshot)
│   │       ├── dim_time.sql               # Hourly time spine 2024–2025
│   │       ├── dim_weather_condition.sql  # WMO code lookup table
│   │       ├── fact_energy_readings.sql   # Central fact, city × hour grain
│   │       ├── agg_daily_energy.sql       # Daily rollup per city
│   │       └── rpt_weather_energy_correlation.sql  # Monthly cross-domain report
│   │
│   ├── snapshots/
│   │   └── snap_dim_location.sql      # SCD Type 2 on regions seed
│   │
│   └── tests/
│       └── assert_bronze_not_empty.sql  # Singular test — fails if Bronze is empty
│
├── scripts/
│   └── export_to_postgres.py          # Reads Gold from Databricks, writes to PostgreSQL
│
├── .claude/
│   └── rules/                         # Context rules for Claude Code sessions
│       ├── error-handling.md
│       ├── testing.md
│       ├── api-design.md
│       ├── security.md
│       └── deployment.md
│
├── CLAUDE.md                          # Project instructions for Claude Code
└── README.md                          # This file
```

---

## 6. Prerequisites & One-Time Setup

### Required accounts (all free)

- [Databricks Free Edition](https://www.databricks.com/try-databricks) — sign up and create a workspace
- [Electricity Maps](https://electricitymaps.com/free-tier-api) — register for a free API key
- Open-Meteo — no registration needed

### Required software (local machine)

| Software | Version | Purpose |
|---|---|---|
| Python | 3.9+ | Running dbt and export script |
| Java | 11+ | Running Metabase JAR |
| PostgreSQL | 12+ | Serving layer database |
| Git | Any | Version control |
| VS Code | Any | IDE with dbt extension |

### Install Python packages

```bash
pip install dbt-core dbt-databricks psycopg2-binary pandas requests databricks-cli
```

### Create PostgreSQL database

```sql
-- Run in psql as a superuser (e.g. postgres user)
CREATE DATABASE weather_energy_db;
\c weather_energy_db
CREATE SCHEMA weather_energy;
```

### Create Databricks secret scope

The secret scope must be created in Databricks before running any notebook.

**Step 1 — Open the secret scope creation URL in your browser:**
```
https://<your-workspace-host>/#secrets/createScope
```

**Step 2 — Configure:**
- Scope name: `weather_energy`
- Manage principal: `Creator` (required on Free Edition — Azure Key Vault not available)

**Step 3 — Add the Electricity Maps API key via CLI:**
```bash
# Configure the CLI with your workspace URL and personal access token
databricks configure --token

# Add the secret (you'll be prompted to paste the key — it won't be visible)
databricks secrets put --scope weather_energy --key electricity_maps_key
```

### Get your Databricks personal access token

1. Click your avatar (top right) in the Databricks workspace
2. Settings → Developer → Access tokens → Generate new token
3. Copy the `dapiXXX...` value — you'll need it for `profiles.yml`

### Get your Databricks HTTP path

1. SQL Warehouses (left sidebar) → click your warehouse
2. "Connection details" tab
3. Copy "Server hostname" and "HTTP path"

---

## 7. Step-by-Step Setup Guide

Follow these steps in order. Each phase must complete before the next begins.

### Phase 1: dbt scaffold (local)

```bash
# From the project root
cd dbt_project

# Edit profiles.yml with your real Databricks credentials (never commit this file)
# Replace <YOUR_WORKSPACE_HOST>, <YOUR_HTTP_PATH>, <YOUR_PERSONAL_ACCESS_TOKEN>

# Verify the connection to Databricks
dbt debug

# Load the regions seed (creates the regions table in Databricks)
dbt seed
```

Expected output from `dbt debug`: all checks pass, connection to Databricks confirmed.

### Phase 2: Bronze ingestion (Databricks)

Import the three notebooks into Databricks from GitHub, or create them manually:

1. Open `notebooks/01_bronze_weather.py` in Databricks → **Run All**
   - Verify: `dbfs:/delta/bronze/weather/` exists with data
   - Verify: `default.bronze_weather` appears in Hive Metastore

2. Open `notebooks/02_bronze_energy.py` in Databricks → **Run All**
   - This takes ~10 minutes due to API rate limiting — this is expected
   - Verify: `dbfs:/delta/bronze/energy/` exists with 10 rows
   - Verify: `default.bronze_energy` appears in Hive Metastore

3. Check the audit log in Databricks:
   ```sql
   SELECT * FROM delta.`dbfs:/delta/logs/bronze_audit` ORDER BY ingested_at DESC LIMIT 20
   ```

### Phase 3: Silver models (local dbt)

```bash
cd dbt_project

# Check Bronze tables are non-empty before running Silver
dbt test --select assert_bronze_not_empty

# Run Silver models
dbt run --select silver.*

# Test Silver
dbt test --select silver.*
```

All tests should pass. Key assertions: `weather_id` and `energy_id` are unique and not null.

### Phase 4: Gold models (local dbt)

```bash
cd dbt_project

# SCD2 snapshot must run before dim_location
dbt snapshot

# Run all Gold models in dependency order (dbt resolves this automatically)
dbt run --select gold.*

# Test Gold
dbt test --select gold.*
```

### Phase 5: Export to PostgreSQL (local)

```bash
# Set your PostgreSQL password as an environment variable
export PG_PASSWORD=your_postgres_password

# Run the export script
python scripts/export_to_postgres.py
```

### Phase 6: Set up Metabase

```bash
# Download Metabase from https://www.metabase.com/start/oss/
# Requires Java 11+ (check: java -version)
java -jar metabase.jar
```

Then open `http://localhost:3000` and follow the setup wizard:
- Database type: PostgreSQL
- Host: `localhost` | Port: `5432`
- Database name: `weather_energy_db`
- Username: `postgres` | Password: your PG_PASSWORD
- Schema: `weather_energy`

---

## 8. Running the Pipeline

### Development (step by step)

Run each notebook and dbt command individually as described in the setup guide.

### Automated (orchestrator)

For a single-command full Bronze run, use the orchestrator notebook in Databricks:

1. Open `03_orchestrator.py` in Databricks
2. Click "Run All"
3. The orchestrator will:
   - Check the idempotency guard (refuses to run if already RUNNING)
   - Run `01_bronze_weather` (weather notebook)
   - Run `02_bronze_energy` (energy notebook, ~10 min)
   - Log the run result to `dbfs:/delta/logs/pipeline_runs/`

After the orchestrator completes, run dbt and export locally:

```bash
cd dbt_project
dbt test --select assert_bronze_not_empty
dbt run --select silver.*  && dbt test --select silver.*
dbt snapshot
dbt run --select gold.*    && dbt test --select gold.*
cd ..
export PG_PASSWORD=your_password
python scripts/export_to_postgres.py
```

---

## 9. Common Commands

### Pipeline

```bash
# Full Bronze run via orchestrator (Databricks UI)
# → Run All on 03_orchestrator.py
```

### dbt (run from dbt_project/ directory)

```bash
dbt debug                                 # Verify Databricks connection
dbt seed                                  # (Re)load regions.csv
dbt snapshot                              # Refresh SCD2 dim_location
dbt run --select silver.*                 # Run Silver models
dbt run --select gold.*                   # Run Gold models
dbt run --select silver.* gold.*          # Run Silver + Gold in one command
dbt test --select silver.* gold.*         # Run all tests
dbt test --select assert_bronze_not_empty # Check Bronze is non-empty
dbt run --full-refresh --select silver.*  # Full rebuild of Silver (dev only)
dbt docs generate && dbt docs serve       # Generate and browse dbt documentation
```

### Validation

```sql
-- Audit log: last 20 ingestion events
SELECT * FROM delta.`dbfs:/delta/logs/bronze_audit` ORDER BY ingested_at DESC LIMIT 20;

-- Bronze row counts
-- (run in a Databricks notebook cell)
-- spark.read.format("delta").load("dbfs:/delta/bronze/weather").count()
-- spark.read.format("delta").load("dbfs:/delta/bronze/energy").count()

-- Silver row counts (Databricks SQL)
SELECT COUNT(*) FROM weather_energy_dev.stg_weather_hourly;
SELECT COUNT(*) FROM weather_energy_dev.stg_energy_intensity;

-- Gold row counts
SELECT COUNT(*) FROM weather_energy_dev.fact_energy_readings;
SELECT COUNT(*) FROM weather_energy_dev.dim_time;
SELECT COUNT(*) FROM weather_energy_dev.dim_location;
```

### Export

```bash
export PG_PASSWORD=your_password
python scripts/export_to_postgres.py
```

### Maintenance

```bash
# Restart Databricks cluster to clear Spark cache
# → Compute → your cluster → Restart

# Clear Bronze (requires explicit confirmation — destructive)
# → In a Databricks notebook:
# spark.sql("DELETE FROM delta.`dbfs:/delta/bronze/weather`")
```

---

## 10. Verification & Row Count Checks

After a successful full run, verify these row counts:

| Table | Expected | Notes |
|---|---|---|
| `bronze_weather` | ~240 | 10 cities × 24 hours |
| `bronze_energy` | 10 | 1 per zone per run |
| `stg_weather_hourly` | ~240 (cumulative) | Grows with each run |
| `stg_energy_intensity` | ~10 (cumulative) | Grows with each run |
| `dim_time` | 17,520 | 2024–2025, hourly |
| `dim_location` | 10 | One per city (current only) |
| `dim_weather_condition` | 21 | Static WMO codes |
| `fact_energy_readings` | ~240 per run | Incremental merge |
| `agg_daily_energy` | 10 per day | 10 cities |
| `rpt_weather_energy_correlation` | 10 per month | 10 cities |

---

## 11. Troubleshooting

### `dbfs:/delta/` write permission denied

Databricks Free Edition may restrict writes to some DBFS paths.

**Fix A — Use managed volumes (Unity Catalog):**
In each notebook, change the path constant:
```python
BRONZE_WEATHER_PATH = "/Volumes/main/weather_energy/bronze/weather"
```
Create the volume first in Databricks UI: Catalog → Create Volume.

**Fix B — Use managed tables (no explicit path):**
```python
df.write.format("delta").mode("overwrite").saveAsTable("default.bronze_weather_raw")
```
Then remove the LOCATION clause from the `register_hive_table()` call.

### `QUALIFY` not supported (older Spark version)

If your cluster uses Spark < 3.3, replace `qualify row_num = 1` with a subquery:

```sql
select * from (
  select *, row_number() over (...) as row_num
  from source
) where row_num = 1
```

### dbt connection error (`dbt debug` fails)

1. Verify the Databricks SQL Warehouse is **running** (not terminated)
2. Check `profiles.yml` — ensure `host` has no `https://` prefix
3. Ensure `http_path` starts with `/sql/1.0/warehouses/`
4. Regenerate your personal access token if it has expired

### Electricity Maps returns 401 Unauthorized

The API key in the Databricks secret scope may be incorrect or expired.

```bash
# Update the secret
databricks secrets put --scope weather_energy --key electricity_maps_key
```

### Bronze tables not visible to dbt sources

The notebooks must have run successfully and registered the Hive Metastore tables.
Check in Databricks SQL:

```sql
SHOW TABLES IN default;
-- Should show: bronze_weather, bronze_energy
```

If missing, re-run the Bronze notebooks — they call `CREATE TABLE IF NOT EXISTS` at the end.

### PostgreSQL export fails: `PG_PASSWORD not set`

```bash
export PG_PASSWORD=your_password
python scripts/export_to_postgres.py
```

### Metabase can't connect to PostgreSQL

- Ensure PostgreSQL is running: `pg_ctl status` or check Windows Services
- Use `localhost` not `127.0.0.1` in Metabase connection settings
- Confirm the schema `weather_energy` exists in `weather_energy_db`

---

## 12. How to Extend the Project

### Add a new city

1. Add a row to `dbt_project/seeds/regions.csv` with city, country, lat, lon, timezone, zone
2. Add the city to the `CITIES` list in `notebooks/01_bronze_weather.py`
3. Add the zone to the `ZONES` list in `notebooks/02_bronze_energy.py`
4. Run `dbt seed` to reload the seed table
5. Run `dbt snapshot` to create a new SCD2 record for the city
6. Re-run the full pipeline

### Add a new API data source

1. Create `notebooks/0N_bronze_<source>.py` following the same pattern as `01_bronze_weather.py`
2. Add a secret key to the `weather_energy` scope if needed
3. Add the new source table to `schema.yml` under `sources`
4. Create `dbt_project/models/silver/stg_<source>.sql`
5. Update `03_orchestrator.py` to include the new notebook
6. Update `CLAUDE.md` Section 2 and Section 7
7. Update this README

### Add a new Gold report

1. Create `dbt_project/models/gold/rpt_<name>.sql`
2. Add the model to `schema.yml` with at least `not_null` tests on key columns
3. Run `dbt run --select gold.rpt_<name>` and `dbt test --select gold.rpt_<name>`
4. The export script automatically picks up any new table in `GOLD_TABLES` list —
   add the table name to `GOLD_TABLES` in `scripts/export_to_postgres.py`

### Extend the time range

Change the `sequence()` end timestamp in `dbt_project/models/gold/dim_time.sql`:

```sql
sequence(
  to_timestamp('2024-01-01 00:00:00'),
  to_timestamp('2026-12-31 23:00:00'),  -- extended to end of 2026
  interval 1 hour
)
```

Then run `dbt run --select dim_time`.

---

## Constraints (what this pipeline must never do)

- Hardcode API keys, tokens, or passwords — always use `dbutils.secrets`
- Use `/mnt/` paths — always use `dbfs:/delta/` (or managed volumes as fallback)
- Mount external storage (Azure ADLS, S3) — not supported on Databricks Free Edition
- Call Electricity Maps API faster than 1 request/minute (free tier rate limit)
- Skip deduplication in Silver models
- Run Gold without first verifying Silver tests pass
- Commit `profiles.yml` or any file containing real credentials to GitHub

---

*Built as a data engineering portfolio project demonstrating Medallion architecture,
PySpark, dbt Core, SCD Type 2, incremental models, and cross-domain analytics.*
