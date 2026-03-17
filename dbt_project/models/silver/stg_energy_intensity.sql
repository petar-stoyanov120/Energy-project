{{
  config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'energy_id',
    on_schema_change    = 'sync_all_columns'
  )
}}

/*
  Silver: stg_energy_intensity
  ----------------------------
  Cleans, types, and deduplicates raw carbon intensity records from Bronze.
  Incremental merge on energy_id.

  The Electricity Maps API returns a snapshot of the current carbon intensity
  rather than a time-series, so each Bronze row represents "intensity at ingestion time".
  We truncate measurement_at to the hour to align it with weather observations for
  the fact table join.
*/

with source as (

  select *
  from {{ source('bronze', 'bronze_energy') }}

  {% if is_incremental() %}
    where ingested_at > (select max(_ingested_at) from {{ this }})
  {% endif %}

),

deduped as (

  select
    *,
    row_number() over (
      -- Deduplicate per zone per hour — take the latest ingested reading.
      partition by zone, date_trunc('hour', cast(measurement_at as timestamp))
      order by ingested_at desc
    ) as _row_num

  from source

  qualify _row_num = 1

),

final as (

  select
    -- Surrogate key: zone + ingested_at is unique since we call the API once per zone per run.
    sha2(concat(zone, '|', cast(ingested_at as string)), 256)  as energy_id,

    cast(zone    as string)   as zone,
    cast(country as string)   as country,
    cast(city    as string)   as city,

    -- Rename API fields to self-documenting column names with units in the suffix.
    cast(carbon_intensity        as double)  as carbon_intensity_gco2eq,
    cast(fossil_free_pct         as double)  as fossil_free_pct,
    cast(renewable_pct           as double)  as renewable_pct,
    -- Day-ahead price populated by SMARD (DE, AT); null for GB and future ENTSO-E zones.
    cast(day_ahead_price_eur_mwh as double)  as day_ahead_price_eur_mwh,

    -- Truncate to hour for clean joins with weather observations in the fact table.
    date_trunc('hour', cast(measurement_at as timestamp))       as measurement_hour,

    cast(source      as string)    as _source,
    cast(ingested_at as timestamp) as _ingested_at,
    current_timestamp()            as _loaded_at

  from deduped

)

select * from final
