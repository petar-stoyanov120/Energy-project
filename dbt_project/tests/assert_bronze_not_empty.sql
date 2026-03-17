/*
  Custom singular test: assert_bronze_not_empty
  ---------------------------------------------
  Fails (returns rows) if either Bronze table has zero records.
  Run with: dbt test --select assert_bronze_not_empty

  This is a guard against a Bronze notebook failure that writes an empty table —
  which would silently produce an empty Silver layer without surfacing an error.
*/

with bronze_weather_count as (
  select count(*) as cnt from {{ source('bronze', 'bronze_weather') }}
),

bronze_energy_count as (
  select count(*) as cnt from {{ source('bronze', 'bronze_energy') }}
),

failures as (

  select 'bronze_weather is empty' as failure_reason
  from bronze_weather_count
  where cnt = 0

  union all

  select 'bronze_energy is empty' as failure_reason
  from bronze_energy_count
  where cnt = 0

)

-- dbt singular tests fail when this query returns any rows.
select * from failures
