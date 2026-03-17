# Error Handling Patterns

## API calls — always retry with exponential backoff

Every external API call must be wrapped in a retry loop. Do not catch broad `Exception` —
catch `requests.exceptions.RequestException` specifically so programming errors still surface.

```python
MAX_RETRIES: int = 3
TIMEOUT_SECONDS: int = 30

for attempt in range(1, MAX_RETRIES + 1):
    try:
        resp = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as exc:
        if attempt == MAX_RETRIES:
            raise
        sleep_sec = 2 ** attempt  # 2s, 4s, 8s
        logging.warning(f"Attempt {attempt} failed: {exc}. Retrying in {sleep_sec}s")
        time.sleep(sleep_sec)
```

## Per-city / per-zone failures — partial success is acceptable

In ingestion notebooks, a failure for one city should be caught, logged, and written to
the audit log as FAILED, but ingestion should continue for remaining cities.

```python
for city_cfg in CITIES:
    try:
        ...
    except Exception as exc:
        logger.error(f"[{city}] Failed: {exc}")
        write_audit_row(spark, city, 0, ingested_at, "FAILED", str(exc))
        # continue to next city — do NOT re-raise here
```

## Abort only when ALL cities fail

Raise at the end of the loop if no rows were collected at all:

```python
if not all_rows:
    raise RuntimeError("No rows collected from any city — aborting Bronze write.")
```

## dbt model failures

- Silver models failing means Silver data is stale. Do NOT cascade to Gold automatically.
- Run `dbt test --select silver.*` after every Silver run before triggering Gold.
- Gold model failures should not overwrite existing Gold data — incremental models
  with `merge` strategy protect against partial writes.

## Audit log — always write, even on failure

The audit log write should go in a `finally` block or be called in both the success
and failure paths. A missing audit row for a run means the run is untraceable.
