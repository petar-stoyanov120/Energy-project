# dbt Testing Standards

## Every model must have tests in schema.yml

No dbt model is considered complete without at least these two column-level tests:
- `not_null` on the primary key column
- `unique` on the primary key column

Minimum template:

```yaml
- name: my_model
  columns:
    - name: surrogate_key
      tests:
        - not_null
        - unique
    - name: city
      tests:
        - not_null
```

## Additional tests by column type

| Column type | Required tests |
|---|---|
| Surrogate key | not_null, unique |
| Foreign key | not_null |
| Status / category column | not_null, accepted_values |
| Timestamp | not_null |
| Metric (temperature, etc.) | not_null only if always expected |

## Singular tests (custom SQL tests)

Custom tests live in `dbt_project/tests/`. File name describes the assertion:
- `assert_bronze_not_empty.sql` — returns rows if Bronze tables are empty (fail condition)

Add a singular test whenever a column-level test cannot express the requirement.

## Test execution order

Run tests after each layer, never skip:

```bash
dbt run --select silver.*   && dbt test --select silver.*
dbt snapshot                  # before gold
dbt run --select gold.*     && dbt test --select gold.*
```

## Test failure policy

- If a Silver test fails → do not run Gold. Fix Silver first.
- If a Gold `unique` test fails → check for incremental merge key conflicts.
- If `assert_bronze_not_empty` fails → Bronze notebook likely failed silently.
  Check the audit log: `SELECT * FROM default.bronze_audit ORDER BY ingested_at DESC LIMIT 20`

## Avoid testing derived aggregates too strictly

Do not add `not_null` to metric columns in `agg_daily_energy` or `rpt_*` models —
energy readings can legitimately be null if the Electricity Maps API was unavailable.
Use `not_null` only where the column is truly always expected.
