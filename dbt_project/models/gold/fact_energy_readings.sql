{{
  config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'fact_id',
    on_schema_change    = 'sync_all_columns'
  )
}}

/*
  Gold: fact_energy_readings
  --------------------------
  Central fact table. Grain: one row per city per hour.

  Joins weather observations with energy readings via shared location (city) and
  time dimensions. The energy join is a left join because energy data may not always
  be available for every hour (API rate limiting, zone outages, etc.).

  Incremental: only processes Silver weather rows loaded since the last fact run.
*/

with weather as (

  select *
  from {{ ref('stg_weather_hourly') }}

  {% if is_incremental() %}
    -- Only pull Silver rows added since the last fact table refresh.
    where _loaded_at > (select max(_loaded_at) from {{ this }})
  {% endif %}

),

energy as (

  -- Pull all energy data — it's small (10 zones per day) and cheap to scan.
  select *
  from {{ ref('stg_energy_intensity') }}

),

dim_loc as (

  select *
  from {{ ref('dim_location') }}
  -- dim_location already filters to current records only.

),

dim_t as (

  select *
  from {{ ref('dim_time') }}

),

dim_wc as (

  select *
  from {{ ref('dim_weather_condition') }}

),

joined as (

  select
    -- Fact surrogate key: same derivation as weather_id for consistency.
    sha2(concat(w.city, '|', cast(w.observation_hour as string)), 256)  as fact_id,

    -- Dimension foreign keys
    dl.location_key,
    dt.time_key,
    w.weathercode                                                         as weathercode_key,

    -- Weather measures
    w.temperature_2m,
    w.windspeed_10m,
    w.precipitation,

    -- Energy measures (null for zones without a live source or if API call failed)
    e.carbon_intensity_gco2eq   as carbon_intensity,
    e.fossil_free_pct,
    e.renewable_pct,
    e.day_ahead_price_eur_mwh,

    -- Lineage
    w._ingested_at              as weather_ingested_at,
    e._ingested_at              as energy_ingested_at,
    current_timestamp()         as _loaded_at

  from weather w

  -- Join to location dimension via city name.
  left join dim_loc dl
    on w.city = dl.city

  -- Join to time dimension using the formatted time key.
  left join dim_t dt
    on date_format(w.observation_hour, 'yyyyMMddHH') = dt.time_key

  -- Join energy by matching electricity zone (from dim_location) and hour.
  -- Energy data is at zone level; weather is at city level — they're linked via location.
  left join energy e
    on dl.electricity_zone = e.zone
    and w.observation_hour = e.measurement_hour

  -- Left join — keeps weather rows even without a matching weather condition entry.
  left join dim_wc wc
    on w.weathercode = wc.weathercode

  -- Only include rows where we have a valid location key (all 10 cities should match).
  where dl.location_key is not null

)

select * from joined
