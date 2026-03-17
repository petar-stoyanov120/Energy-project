{{
  config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'weather_id',
    on_schema_change    = 'sync_all_columns'
  )
}}

/*
  Silver: stg_weather_hourly
  --------------------------
  Cleans, types, and deduplicates raw hourly weather records from Bronze.
  Incremental merge on weather_id — only rows newer than the last load are processed,
  keeping run times short even as Bronze grows.

  Deduplication strategy: keep the most recently ingested row per (city, hour) pair.
  This handles cases where the orchestrator re-runs on the same day (replaceWhere on
  Bronze gives us a clean partition, but we apply QUALIFY here as a safety net).
*/

with source as (

  select *
  from {{ source('bronze', 'bronze_weather') }}

  {% if is_incremental() %}
    -- Only process rows ingested after the last successful Silver load.
    -- This avoids reprocessing the entire Bronze history on every dbt run.
    where ingested_at > (select max(_ingested_at) from {{ this }})
  {% endif %}

),

deduped as (

  select
    *,
    row_number() over (
      partition by city, cast(hour as date), date_format(hour, 'HH')
      order by ingested_at desc
    ) as _row_num

  from source

  -- QUALIFY is supported on Databricks SQL (Spark 3.3+).
  -- If your cluster uses Spark < 3.3, wrap this in a subquery and filter WHERE _row_num = 1.
  qualify _row_num = 1

),

final as (

  select
    -- Surrogate key: deterministic hash of the natural key (city + hour).
    sha2(concat(city, '|', cast(hour as string)), 256)  as weather_id,

    cast(city          as string)     as city,
    cast(country       as string)     as country,
    cast(latitude      as double)     as latitude,
    cast(longitude     as double)     as longitude,
    cast(timezone      as string)     as timezone,

    -- Normalise the timestamp to remove sub-second precision for clean joins.
    date_trunc('hour', cast(hour as timestamp))          as observation_hour,

    cast(temperature_2m as double)    as temperature_2m,
    cast(windspeed_10m  as double)    as windspeed_10m,
    cast(weathercode    as int)       as weathercode,
    cast(precipitation  as double)    as precipitation,

    cast(source      as string)       as _source,
    cast(ingested_at as timestamp)    as _ingested_at,
    current_timestamp()               as _loaded_at

  from deduped

)

select * from final
