{{
  config(materialized = 'table')
}}

/*
  Gold: agg_daily_energy
  ----------------------
  Pre-aggregated daily rollup per city, derived from the fact table.
  Grain: one row per city per calendar day.

  Designed for Metabase dashboards showing daily trends — querying pre-aggregated
  data avoids scanning the full hourly fact table on every dashboard load.
*/

with fact as (

  select *
  from {{ ref('fact_energy_readings') }}

),

dim_loc as (

  select location_key, city, country
  from {{ ref('dim_location') }}

),

daily as (

  select
    -- Extract date from time_key (first 8 chars = yyyyMMdd)
    to_date(left(f.time_key, 8), 'yyyyMMdd')  as agg_date,

    dl.city,
    dl.country,

    -- Temperature stats
    round(avg(f.temperature_2m),  2)           as avg_temperature,
    round(max(f.temperature_2m),  2)           as max_temperature,
    round(min(f.temperature_2m),  2)           as min_temperature,

    -- Precipitation: sum across all hours in the day
    round(sum(f.precipitation),   2)           as total_precipitation_mm,

    -- Wind
    round(avg(f.windspeed_10m),   2)           as avg_windspeed,

    -- Energy
    round(avg(f.carbon_intensity), 2)          as avg_carbon_intensity,
    round(avg(f.renewable_pct),    2)          as avg_renewable_pct,
    round(avg(f.fossil_free_pct),  2)          as avg_fossil_free_pct,

    -- Data completeness
    count(*)                                   as reading_count,
    sum(case when f.carbon_intensity is null then 1 else 0 end) as missing_energy_readings

  from fact f
  left join dim_loc dl on f.location_key = dl.location_key

  group by 1, 2, 3

)

select * from daily
