# Security & Secrets

## The single rule: all secrets via Databricks Secrets

Every credential, token, or API key in this project must come from:

```python
dbutils.secrets.get(scope="weather_energy", key="<key_name>")
```

Zero exceptions. No environment variables in Databricks notebooks, no `.env` files,
no hardcoded strings, no default values that look like real keys.

## Secret scope setup

The secret scope must be created manually in the Databricks UI before running any notebook:

1. Go to: `https://<workspace-host>/#secrets/createScope`
2. Scope name: `weather_energy`
3. Manage principal: Creator (Free Edition — Azure Key Vault integration unavailable)

Add keys via Databricks CLI:

```bash
pip install databricks-cli
databricks configure --token
databricks secrets put --scope weather_energy --key electricity_maps_key
# Paste the key value at the prompt — it is never stored in shell history
```

## Keys managed in this scope

| Key name | Purpose |
|---|---|
| `electricity_maps_key` | Electricity Maps free-tier API auth |

## What must never be committed to GitHub

| File | Reason |
|---|---|
| `profiles.yml` | Contains Databricks token and HTTP path |
| `.env` | Any environment variable file with credentials |
| Any file matching `*secret*`, `*credential*`, `*token*` | Precautionary |

`dbt_project/.gitignore` already excludes `profiles.yml`.

## PostgreSQL password

The export script reads the PostgreSQL password from an environment variable:

```bash
export PG_PASSWORD=your_password
python scripts/export_to_postgres.py
```

Never store `PG_PASSWORD` in a `.env` file committed to the repo.
Add `.env` to the root `.gitignore` if you create one locally.

## Logging rules

- Log city name, zone code, row counts, timestamps — these are safe.
- Never log: API keys, auth headers, the full `headers` dict, raw response bodies
  (they may echo auth tokens back in error messages from some APIs).
- Use `logger.error(str(exc))` — Python exceptions from requests do not include
  the auth header value, so this is safe.

## GitHub repo hygiene

Before the first `git push`:
1. Confirm `dbt_project/.gitignore` is present and includes `profiles.yml`.
2. Add a root-level `.gitignore` that excludes `.env`, `*.pem`, `*.key`.
3. Run `git status` and review every file before staging — never `git add .` blindly.
