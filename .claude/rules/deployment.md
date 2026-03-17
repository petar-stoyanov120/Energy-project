# Deployment & Serving

## Running the pipeline

### Manual run (development)
Run notebooks individually in Databricks UI in order:
1. `01_bronze_weather` — verify output before proceeding
2. `02_bronze_energy` — wait ~10 minutes for rate-limited completion
3. Run `dbt run --select silver.*` locally
4. Run `dbt snapshot` locally
5. Run `dbt run --select gold.*` locally
6. Run `python scripts/export_to_postgres.py` locally

### Full automated run (orchestrator)
Open `03_orchestrator.py` in Databricks UI and click "Run All".
The orchestrator handles the notebook sequence and idempotency guard automatically.

## dbt — connecting to Databricks Free Edition

The dbt-databricks adapter connects via the SQL Warehouse HTTP endpoint.
The cluster must be RUNNING before dbt commands are issued.

**Finding your connection details:**
1. Databricks workspace → SQL Warehouses (left sidebar)
2. Click your warehouse → "Connection details" tab
3. Copy "Server hostname" and "HTTP path"

```yaml
# profiles.yml (local only, never commit)
weather_energy:
  outputs:
    dev:
      type: databricks
      host: <server-hostname>
      http_path: <http-path>
      token: <personal-access-token>
      schema: weather_energy_dev
      threads: 2
```

Run `dbt debug` from the `dbt_project/` directory to verify the connection.

## PostgreSQL — local setup

```sql
-- Run once in psql as a superuser
CREATE DATABASE weather_energy_db;
\c weather_energy_db
CREATE SCHEMA weather_energy;
```

The export script creates tables automatically on first run.

## Metabase — setup

Metabase is the recommended free BI tool. No Docker or subscription required.

```bash
# 1. Download the free open-source JAR from https://www.metabase.com/start/oss/
# 2. Run it (requires Java 11+)
java -jar metabase.jar

# 3. Open http://localhost:3000 in your browser
# 4. Complete the setup wizard:
#    - Choose PostgreSQL as the data source
#    - Host: localhost | Port: 5432
#    - Database: weather_energy_db
#    - Username: postgres | Password: <your PG_PASSWORD>
#    - Schema: weather_energy
```

Recommended starter dashboards:
- Daily carbon intensity per city (line chart from `agg_daily_energy`)
- Temperature vs. renewable percentage scatter (from `rpt_weather_energy_correlation`)
- City comparison bar chart (from `agg_daily_energy`)

## DBFS path fallback

If `dbfs:/delta/` paths produce permission errors on the Free Edition cluster:

**Option A — Unity Catalog managed volumes:**
```python
# Replace the path constant in notebooks:
BRONZE_WEATHER_PATH = "/Volumes/main/weather_energy/bronze/weather"
```

**Option B — Hive Metastore managed tables (no explicit LOCATION):**
```python
# Write to a managed Delta table; let Databricks manage the storage location.
df.write.format("delta").mode("overwrite").saveAsTable("default.bronze_weather_raw")
```

In this case, remove the explicit LOCATION from the CREATE TABLE statement
in `register_hive_table()` as well.

## Scheduling (future)

Databricks Free Edition does not support Jobs UI for scheduled runs.
Options for free automated scheduling:
- GitHub Actions workflow that triggers `03_orchestrator.py` via Databricks REST API
- Local cron job (Windows Task Scheduler) running the Databricks CLI
