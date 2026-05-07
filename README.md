# Weather & Energy Analytics Pipeline

An end-to-end data engineering project that ingests hourly weather forecasts and
real-time energy data for ten European capitals, processes them through a Medallion
architecture (Bronze → Silver → Gold) on Delta Lake, and serves the results to a
local PostgreSQL database for visualisation in Metabase.

The whole pipeline runs on **Databricks Free Edition serverless compute**, uses only
**free, no-auth public APIs**, and stores everything as **Unity Catalog managed tables**
under `main.weather_energy_dev.*`.

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
13. [Project Constraints](#13-project-constraints)

---

## 1. Project Overview

### What it does

- **Ingests** hourly weather forecasts from [Open-Meteo](https://open-meteo.com/) for
  ten European capitals (no auth required).
- **Ingests** real-time energy data from two free public sources:
  the [UK Carbon Intensity API](https://carbonintensity.org.uk/) for Great Britain
  (carbon intensity, generation mix) and [SMARD](https://www.smard.de/) for Germany
  and Austria (day-ahead electricity price).
- **Processes** raw API JSON through three Delta Lake layers — Bronze (raw,
  schema-enforced), Silver (cleaned and deduplicated), Gold (a star schema fit for BI).
- **Transforms** with dbt Core using incremental merge models, dbt snapshots for
  SCD Type 2, and column-level data quality tests.
- **Exports** the Gold layer to PostgreSQL so Metabase can connect to it without any
  Databricks-specific drivers.

### Cities & energy coverage

All ten cities are covered for **weather**. Energy data is available for the three
cities whose national operators publish open data:

| City | Country | Weather | Energy source |
|---|---|---|---|
| London | GB | ✓ | UK Carbon Intensity API |
| Berlin | DE | ✓ | SMARD (day-ahead price) |
| Vienna | AT | ✓ | SMARD (day-ahead price) |
| Paris | FR | ✓ | — (pending ENTSO-E access) |
| Madrid | ES | ✓ | — (pending ENTSO-E access) |
| Rome | IT | ✓ | — (pending ENTSO-E access) |
| Amsterdam | NL | ✓ | — (pending ENTSO-E access) |
| Warsaw | PL | ✓ | — (pending ENTSO-E access) |
| Stockholm | SE | ✓ | — (pending ENTSO-E access) |
| Lisbon | PT | ✓ | — (pending ENTSO-E access) |

The fact table uses left joins, so cities without an energy row still appear with
weather data and `NULL` energy columns. Once an ENTSO-E developer key is granted, the
remaining seven zones can be added by extending `02_bronze_energy.py`.

### Analytics enabled

- Hourly carbon intensity trend for Great Britain.
- Day-ahead electricity price trend for Germany and Austria.
- Temperature vs. renewable percentage scatter plots (GB only, until ENTSO-E lands).
- Daily weather rollups per city — average / min / max temperature, total precipitation.
- Monthly weather-energy correlation report at city granularity.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│                                                                     │
│  Open-Meteo            UK Carbon Intensity         SMARD            │
│  hourly weather        carbon intensity / mix      day-ahead price  │
│  10 cities · no auth   GB · no auth                DE, AT · no auth │
└──────────────┬─────────────────┬───────────────────────┬────────────┘
               │                 │                       │
               ▼                 ▼                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  BRONZE — Unity Catalog managed tables              │
│                                                                     │
│  main.weather_energy_dev.bronze_weather                             │
│  main.weather_energy_dev.bronze_energy                              │
│    Both partitioned by (country, ingested_date)                     │
│    StructType-enforced schemas · idempotent via replaceWhere        │
│                                                                     │
│  main.weather_energy_dev.bronze_audit       — per-city audit log   │
│  main.weather_energy_dev.pipeline_run_log   — orchestrator runs    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  dbt run --select silver.*
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  SILVER — cleaned, typed, deduplicated              │
│                                                                     │
│  stg_weather_hourly        stg_energy_intensity                     │
│  Incremental merge on      Incremental merge on energy_id           │
│  weather_id                QUALIFY-based deduplication              │
│  SHA-256 surrogate keys    SHA-256 surrogate keys                   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  dbt snapshot + dbt run --select gold.*
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  GOLD — star schema                                 │
│                                                                     │
│  dim_location (SCD2)   dim_time      dim_weather_condition          │
│  10 rows · current      ~17,520 rows  21 rows · WMO 4677 lookup     │
│                                                                     │
│  fact_energy_readings  agg_daily_energy   rpt_weather_energy_corr   │
│  city × hour grain     city × day grain   city × month grain        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  python scripts/export_to_postgres.py
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  SERVING LAYER                                      │
│                                                                     │
│  PostgreSQL (local) · schema: weather_energy                        │
│  Metabase OSS at localhost:3000 · connects directly to PostgreSQL   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Runtime | Databricks Free Edition | Serverless compute only — DBR 18.0, Python 3.12, Spark Connect |
| Compute | PySpark (Spark Connect API) + Spark SQL | No `sparkContext`, no RDD APIs, no `df.cache()` |
| Storage | Delta Lake (Unity Catalog managed tables) | Three-part names — `main.weather_energy_dev.*` |
| Configuration | `dbutils.widgets` | Catalog and schema set at notebook runtime, never hardcoded |
| Transformation | dbt Core + dbt-databricks | Run locally from VS Code against the SQL Warehouse |
| Weather API | Open-Meteo | Free, no authentication |
| Energy APIs | UK Carbon Intensity + SMARD | Both free, no authentication |
| Serving DB | PostgreSQL 12+ | Local install |
| BI | Metabase OSS (JAR) | Free, no Docker, runs as `java -jar metabase.jar` |
| IDE | VS Code (+ dbt Power User extension) | dbt IntelliSense and lineage |
| Version control | GitHub | Repo connected to the Databricks workspace |

---

## 4. Data Model

### Bronze layer

Raw, schema-enforced Delta tables. Never modified after the initial write — only
re-written via `replaceWhere` for the current ingested date.

```
bronze_weather  (partition: country, ingested_date)
├── city, country, latitude, longitude, timezone
├── hour                       — observation timestamp (UTC)
├── temperature_2m, windspeed_10m, weathercode, precipitation
└── ingested_at, ingested_date, source

bronze_energy   (partition: country, ingested_date)
├── zone, country, city
├── carbon_intensity           — gCO2eq/kWh, populated by UK API only
├── fossil_free_pct            — 0–100, populated by UK API only
├── renewable_pct              — 0–100, populated by UK API only
├── day_ahead_price_eur_mwh    — €/MWh, populated by SMARD only (DE, AT)
├── measurement_at             — original API timestamp
└── ingested_at, ingested_date, source

bronze_audit    (no partitioning — append-only log)
└── source, city, row_count, ingested_at, status, error_message

pipeline_run_log (no partitioning — orchestrator-level run tracking)
└── run_id, started_at, ended_at, status, error
```

### Silver layer

Cleaned, deduplicated, typed. Grain: one row per city per hour (weather) and one row
per zone per measurement hour (energy). Both models are incremental and merge on a
SHA-256 surrogate key.

```
stg_weather_hourly
├── weather_id              — SHA-256(city || observation_hour)
├── city, country, latitude, longitude, timezone
├── observation_hour, temperature_2m, windspeed_10m, weathercode, precipitation
└── _source, _ingested_at, _loaded_at

stg_energy_intensity
├── energy_id               — SHA-256(zone || ingested_at)
├── zone, country, city
├── carbon_intensity_gco2eq, fossil_free_pct, renewable_pct
├── day_ahead_price_eur_mwh, measurement_hour
└── _source, _ingested_at, _loaded_at
```

### Gold layer — star schema

```
                    ┌──────────────────┐
                    │  dim_location    │◄── SCD Type 2 via snap_dim_location
                    │  location_key    │    (current rows only)
                    │  city, country   │
                    │  lat, lon, tz    │
                    │  electricity_zone│
                    └────────┬─────────┘
                             │
┌─────────────┐    ┌─────────▼───────────────┐    ┌──────────────────────┐
│  dim_time   │◄───┤  fact_energy_readings   ├───►│ dim_weather_condition│
│  time_key   │    │  fact_id (PK)           │    │  weathercode (PK)    │
│  year/month │    │  location_key (FK)      │    │  condition_label     │
│  day/hour   │    │  time_key (FK)          │    │  condition_category  │
│  is_weekend │    │  weathercode (FK)       │    │  severity            │
└─────────────┘    │  temperature_2m         │    └──────────────────────┘
                   │  windspeed_10m          │
                   │  precipitation          │
                   │  carbon_intensity       │
                   │  fossil_free_pct        │
                   │  renewable_pct          │
                   │  day_ahead_price_eur_mwh│
                   └────────────┬────────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
      ┌─────────▼──────────┐    ┌───────────────▼────────────┐
      │  agg_daily_energy  │    │ rpt_weather_energy_corr    │
      │  grain: city×day   │    │ grain: city×month          │
      │  avg/max/min temp  │    │ avg_temp, avg_carbon_int   │
      │  total precip      │    │ avg_renewable_pct          │
      │  avg carbon/renew  │    │ correlation profile        │
      └────────────────────┘    └────────────────────────────┘
```

| Dimension | Rows | Materialization | Notes |
|---|---|---|---|
| `dim_location` | 10 | table | SCD2 — current rows only |
| `dim_time` | ~17,520 | table | 2024–2025, hourly granularity |
| `dim_weather_condition` | 21 | table | Static WMO 4677 code lookup |

`fact_energy_readings` is incremental with `merge` strategy on `fact_id`, so re-runs
are safe and partial backfills are easy.

---

## 5. Folder Structure

```
weather_energy_project/
│
├── notebooks/                         # Databricks PySpark notebooks
│   ├── 01_bronze_weather.py           # Open-Meteo ingestion (10 cities)
│   ├── 02_bronze_energy.py            # UK Carbon Intensity + SMARD ingestion
│   └── 03_orchestrator.py             # Sequenced runner with idempotency guard
│
├── dbt_project/                       # dbt Core project (run locally)
│   ├── dbt_project.yml                # Project config and per-folder materializations
│   ├── profiles.yml                   # LOCAL ONLY — Databricks connection (gitignored)
│   ├── .gitignore                     # Excludes profiles.yml, target/, logs/
│   ├── schema.yml                     # All sources, models, column-level tests
│   │
│   ├── seeds/
│   │   └── regions.csv                # 10 European cities: lat / lon / timezone / zone
│   │
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_weather_hourly.sql
│   │   │   └── stg_energy_intensity.sql
│   │   └── gold/
│   │       ├── dim_location.sql
│   │       ├── dim_time.sql
│   │       ├── dim_weather_condition.sql
│   │       ├── fact_energy_readings.sql
│   │       ├── agg_daily_energy.sql
│   │       └── rpt_weather_energy_correlation.sql
│   │
│   ├── snapshots/
│   │   └── snap_dim_location.sql      # SCD Type 2 on the regions seed
│   │
│   └── tests/
│       └── assert_bronze_not_empty.sql  # Singular test — fails if Bronze has 0 rows
│
├── scripts/
│   └── export_to_postgres.py          # Reads Gold from Databricks, writes to PostgreSQL
│
├── .claude/rules/                     # Modular rules referenced by CLAUDE.md
│   ├── error-handling.md
│   ├── testing.md
│   ├── api-design.md
│   ├── security.md
│   └── deployment.md
│
├── CLAUDE.md                          # Project conventions for Claude Code sessions
└── README.md                          # This file
```

---

## 6. Prerequisites & One-Time Setup

### Required accounts (all free, no payment method needed)

- **[Databricks Free Edition](https://www.databricks.com/learn/free-edition)** —
  sign up and create a workspace. Free Edition gives you a Unity Catalog metastore
  and serverless compute out of the box.
- **No API accounts required.** Open-Meteo, UK Carbon Intensity, and SMARD are all
  open public APIs — no registration, no keys.

### Required local software

| Software | Version | Purpose |
|---|---|---|
| Python | 3.9+ | Running dbt and the export script |
| Java | 11+ | Running the Metabase JAR |
| PostgreSQL | 12+ | Serving database |
| Git | Any | Version control |
| VS Code | Any recent | IDE — optional but recommended |

### Install Python packages

```bash
pip install dbt-core dbt-databricks psycopg2-binary pandas requests databricks-cli
```

### Create the PostgreSQL database

```sql
-- Run once in psql as a superuser (e.g. the postgres role)
CREATE DATABASE weather_energy_db;
\c weather_energy_db
CREATE SCHEMA weather_energy;
```

### Create the Unity Catalog schema in Databricks

The pipeline writes to `main.weather_energy_dev.*`. Create the schema once before the
first run, either in the Databricks SQL editor or in a notebook cell:

```sql
CREATE SCHEMA IF NOT EXISTS main.weather_energy_dev;
```

Free Edition workspaces are provisioned with the `main` catalog enabled, so no extra
permissions setup is needed.

### Get your Databricks personal access token

1. Click your avatar (top right of the Databricks workspace) → **Settings**.
2. **Developer** → **Access tokens** → **Generate new token**.
3. Copy the `dapi…` value — you'll paste it into `profiles.yml` shortly.

### Get your SQL Warehouse connection details

1. **SQL Warehouses** in the left sidebar → click your warehouse.
2. Open the **Connection details** tab.
3. Copy **Server hostname** and **HTTP path**.

> Free Edition starts a small Serverless SQL Warehouse for you automatically. You don't
> need to create one yourself.

---

## 7. Step-by-Step Setup Guide

Each phase must complete before the next begins.

### Phase 1 — Configure dbt locally

```bash
cd dbt_project
```

Create `dbt_project/profiles.yml` (this file is gitignored — never commit real
credentials):

```yaml
weather_energy:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <YOUR_SERVER_HOSTNAME>          # e.g. dbc-12345.cloud.databricks.com
      http_path: <YOUR_SQL_WAREHOUSE_HTTP_PATH>
      token: <YOUR_PERSONAL_ACCESS_TOKEN>
      catalog: main
      schema: weather_energy_dev
      threads: 2
```

Verify the connection and load the seed:

```bash
dbt debug          # all checks should pass
dbt seed           # creates main.weather_energy_dev.regions
```

### Phase 2 — Bronze ingestion (Databricks)

Import the three notebooks in `notebooks/` into your Databricks workspace (you can
clone the GitHub repo into the workspace's Repos area or upload the files manually).

Each notebook exposes two text widgets — `catalog` and `schema` — at the top of the
notebook. Both default to `main` and `weather_energy_dev`, so you can usually just
click **Run All**.

1. Open `01_bronze_weather.py` → **Run All**.
   - Verify `main.weather_energy_dev.bronze_weather` is populated.
2. Open `02_bronze_energy.py` → **Run All**.
   - Runtime is roughly 30–60 seconds — both energy APIs are fast and unrate-limited.
   - Verify `main.weather_energy_dev.bronze_energy` has three rows (London, Berlin, Vienna).
3. Inspect the audit log:

```sql
SELECT * FROM main.weather_energy_dev.bronze_audit
ORDER BY ingested_at DESC LIMIT 20;
```

### Phase 3 — Silver models (local dbt)

```bash
cd dbt_project

# Confirm Bronze has data before running anything downstream
dbt test --select assert_bronze_not_empty

dbt run  --select silver.*
dbt test --select silver.*
```

All Silver tests should pass. The key contracts are `weather_id` and `energy_id`
being unique and not null.

### Phase 4 — Gold models (local dbt)

```bash
# The SCD2 snapshot must run before dim_location reads from it
dbt snapshot

dbt run  --select gold.*
dbt test --select gold.*
```

dbt resolves the dependency order automatically — dimensions before fact, fact before
the aggregates and the report.

### Phase 5 — Export to PostgreSQL

```bash
# Set the PostgreSQL password as an environment variable (don't commit a .env file)
export PG_PASSWORD=your_postgres_password

# On Windows PowerShell:
# $env:PG_PASSWORD = "your_postgres_password"

python scripts/export_to_postgres.py
```

The script truncates and reloads each Gold table on every run, so it's safe to call
repeatedly.

### Phase 6 — Set up Metabase

```bash
# 1. Download the OSS JAR from https://www.metabase.com/start/oss/
# 2. Java 11+ required (check: java -version)
java -jar metabase.jar
```

Open `http://localhost:3000` and complete the wizard:

- **Database type:** PostgreSQL
- **Host / Port:** `localhost` / `5432`
- **Database name:** `weather_energy_db`
- **Username / Password:** `postgres` / your `PG_PASSWORD`
- **Schema:** `weather_energy`

---

## 8. Running the Pipeline

### Manual run (development)

Run the notebooks individually as in Phase 2 above, then run dbt and the export
locally as in Phases 3–5.

### Automated run (orchestrator)

For a single-click full Bronze refresh, use `03_orchestrator.py` in Databricks:

1. Open `03_orchestrator.py` and click **Run All**.
2. The orchestrator:
   - Looks at `main.weather_energy_dev.pipeline_run_log` and refuses to start if a
     run started in the last two hours is still in `RUNNING` state — this prevents
     accidental double-triggers from corrupting Bronze.
   - Writes a new `RUNNING` row to the log.
   - Calls `01_bronze_weather` via `%run` (Free Edition serverless does not support
     `dbutils.notebook.run`).
   - Calls `02_bronze_energy` via `%run`.
   - Updates the log row to `SUCCESS` (or `FAILED` with the error).

After the orchestrator finishes, run the dbt commands and export script locally:

```bash
cd dbt_project
dbt test --select assert_bronze_not_empty
dbt run  --select silver.*  && dbt test --select silver.*
dbt snapshot
dbt run  --select gold.*    && dbt test --select gold.*
cd ..
export PG_PASSWORD=your_password
python scripts/export_to_postgres.py
```

---

## 9. Common Commands

### dbt (run from `dbt_project/`)

```bash
dbt debug                                 # Verify Databricks connection
dbt seed                                  # (Re)load regions.csv
dbt snapshot                              # Refresh SCD2 snap_dim_location
dbt run  --select silver.*                # Run Silver models
dbt run  --select gold.*                  # Run Gold models
dbt run  --select silver.* gold.*         # Both layers in one go
dbt test --select silver.* gold.*         # All column-level tests
dbt test --select assert_bronze_not_empty # Singular Bronze sanity check
dbt run  --full-refresh --select silver.* # Rebuild Silver from scratch (dev only)
dbt docs generate && dbt docs serve       # Browse model lineage and docs
```

### Validation queries (run in Databricks SQL)

```sql
-- Audit log: last 20 ingestion events
SELECT *
FROM main.weather_energy_dev.bronze_audit
ORDER BY ingested_at DESC
LIMIT 20;

-- Orchestrator runs
SELECT *
FROM main.weather_energy_dev.pipeline_run_log
ORDER BY started_at DESC
LIMIT 10;

-- Row counts across the three layers
SELECT 'bronze_weather'  AS layer, COUNT(*) AS rows FROM main.weather_energy_dev.bronze_weather
UNION ALL SELECT 'bronze_energy',  COUNT(*) FROM main.weather_energy_dev.bronze_energy
UNION ALL SELECT 'stg_weather_hourly',   COUNT(*) FROM main.weather_energy_dev.stg_weather_hourly
UNION ALL SELECT 'stg_energy_intensity', COUNT(*) FROM main.weather_energy_dev.stg_energy_intensity
UNION ALL SELECT 'fact_energy_readings', COUNT(*) FROM main.weather_energy_dev.fact_energy_readings;
```

### Export to PostgreSQL

```bash
export PG_PASSWORD=your_password
python scripts/export_to_postgres.py
```

### Maintenance

```sql
-- Clear today's Bronze partition (rare — only when reproducing a bug)
DELETE FROM main.weather_energy_dev.bronze_weather
WHERE ingested_date = CURRENT_DATE;

-- Inspect Delta history before any destructive change
DESCRIBE HISTORY main.weather_energy_dev.fact_energy_readings;
```

---

## 10. Verification & Row Count Checks

After a clean full run, expect roughly these counts:

| Table | Per run | Notes |
|---|---|---|
| `bronze_weather` | ~240 | 10 cities × 24 hours of forecast |
| `bronze_energy` | 3 | London, Berlin, Vienna |
| `stg_weather_hourly` | ~240 (cumulative) | Grows over time as runs accumulate |
| `stg_energy_intensity` | ~3 (cumulative) | Grows over time |
| `dim_time` | 17,520 | Static — 2024-01-01 to 2025-12-31 hourly |
| `dim_location` | 10 | Current SCD2 rows only |
| `dim_weather_condition` | 21 | Static WMO 4677 lookup |
| `fact_energy_readings` | ~240 (per run) | Incremental merge on `fact_id` |
| `agg_daily_energy` | ~10 per day | One row per city per day |
| `rpt_weather_energy_correlation` | ~10 per month | One row per city per month |

---

## 11. Troubleshooting

### `dbt debug` fails

1. Confirm the SQL Warehouse is **running** in Databricks — Free Edition warehouses
   auto-terminate after idle.
2. The `host` value in `profiles.yml` must not include `https://` — just the hostname.
3. The `http_path` must start with `/sql/1.0/warehouses/`.
4. If the access token has expired, regenerate it under **Settings → Developer → Access tokens**.

### Notebook fails with "Spark Connect does not support sparkContext"

Free Edition serverless uses Spark Connect, which exposes a subset of the PySpark API.
Avoid:

- `spark.sparkContext` and any RDD-level operations
- `df.cache()` / `df.persist()`
- `dbutils.notebook.run()` — use `%run ./other_notebook` instead

If you've extended the codebase and hit this error, search the offending notebook for
those calls and rewrite them with DataFrame-only APIs.

### Bronze tables not visible to dbt

Check that the writes actually landed:

```sql
SHOW TABLES IN main.weather_energy_dev;
```

You should see at least `bronze_weather`, `bronze_energy`, `bronze_audit`, and (after
the orchestrator has run) `pipeline_run_log`. If they're missing, re-run the Bronze
notebooks and inspect the `bronze_audit` table for `FAILED` rows.

### Orchestrator refuses to run with "A pipeline run is already in RUNNING status"

A previous run crashed before it could mark itself as `SUCCESS` or `FAILED`. The guard
window is two hours — either wait it out, or manually update the row:

```sql
UPDATE main.weather_energy_dev.pipeline_run_log
SET status = 'FAILED', error = 'Manually cleared'
WHERE run_id = '<the_run_id>';
```

### PostgreSQL export fails: `PG_PASSWORD environment variable is not set`

The export script reads the password from the environment to avoid hardcoding it:

```bash
export PG_PASSWORD=your_password           # macOS / Linux
$env:PG_PASSWORD = "your_password"         # Windows PowerShell
python scripts/export_to_postgres.py
```

### Metabase can't connect to PostgreSQL

- Make sure PostgreSQL is running (`pg_ctl status`, or check Windows Services).
- Use `localhost` rather than `127.0.0.1` in the Metabase form.
- Confirm the schema `weather_energy` actually exists in `weather_energy_db`.

---

## 12. How to Extend the Project

### Add a new city

1. Add a row to `dbt_project/seeds/regions.csv` with `city`, `country`, `lat`, `lon`,
   `timezone`, and `zone`.
2. Add the city to the `CITIES` list in `notebooks/01_bronze_weather.py`.
3. If energy data is available for that zone, add a fetch block to
   `notebooks/02_bronze_energy.py`.
4. `dbt seed` to reload the seed table.
5. `dbt snapshot` to record the new city in the SCD2 history.
6. Re-run the full pipeline.

### Add a new energy source

1. Create a new fetcher inside `02_bronze_energy.py` (or a new
   `0N_bronze_<source>.py` notebook for a fully separate source).
2. Map its fields onto the existing `ENERGY_SCHEMA` — leave columns null where the
   new source doesn't provide them.
3. Add the source to `schema.yml` if it's a new Bronze table.
4. Update `03_orchestrator.py` so the new notebook is `%run` in sequence.
5. Update Section 1 (the cities table) and Section 7 of this README.

### Add a new Gold report

1. Create `dbt_project/models/gold/rpt_<name>.sql`.
2. Add the model to `schema.yml` with at least `not_null` tests on key columns.
3. `dbt run --select gold.rpt_<name>` and `dbt test --select gold.rpt_<name>`.
4. Append the new table name to `GOLD_TABLES` in
   `scripts/export_to_postgres.py` so it ships to PostgreSQL.

### Extend the time dimension range

`dbt_project/models/gold/dim_time.sql` builds the spine with `sequence()` between two
fixed timestamps. To extend through 2026:

```sql
sequence(
  to_timestamp('2024-01-01 00:00:00'),
  to_timestamp('2026-12-31 23:00:00'),
  interval 1 hour
)
```

Then `dbt run --select dim_time`.

---

## 13. Project Constraints

The pipeline deliberately enforces these rules. They're worth keeping in mind before
extending the codebase:

- **No hardcoded secrets.** Catalog and schema come from `dbutils.widgets`. Any future
  API key must come from `dbutils.secrets.get(scope="weather_energy", key=...)` —
  never from environment variables in a notebook, never from the source.
- **Unity Catalog only.** All tables use three-part names (`main.weather_energy_dev.*`)
  via `saveAsTable`. No `dbfs:/delta/` paths. No `CREATE TABLE … LOCATION`.
- **Serverless-friendly PySpark only.** No `sparkContext`, no RDD APIs, no
  `df.cache()`, no `dbutils.notebook.run()`. Free Edition serverless will reject all
  of those at runtime.
- **No external storage mounts.** ADLS, S3, GCS mounts are not supported on Free
  Edition.
- **Idempotency.** Bronze writes use `replaceWhere` on the current `ingested_date`
  partition, so re-running the same day overwrites cleanly without duplicating rows.
  The orchestrator's run-log guard prevents two concurrent runs.
- **Schema enforcement on Bronze.** Every `createDataFrame` call passes an explicit
  `StructType`. If an upstream API changes shape, the write fails fast rather than
  silently corrupting downstream models.
- **Every dbt model has tests.** At minimum, surrogate keys carry `not_null` and
  `unique` tests, defined alongside the model in `schema.yml`.
- **No commercial APIs.** The whole pipeline runs end-to-end on free, public,
  unauthenticated data sources. No payment method or paid tier required.

---

*Built as a portfolio data engineering project — Medallion architecture, PySpark on
serverless Databricks, dbt Core, SCD Type 2, incremental merges, and a star-schema
Gold layer feeding a local PostgreSQL + Metabase BI stack.*
