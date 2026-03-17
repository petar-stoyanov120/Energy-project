{{
  config(materialized = 'table')
}}

/*
  Gold: rpt_weather_energy_correlation
  -------------------------------------
  Monthly cross-domain report linking weather patterns to energy characteristics.
  Grain: one row per city per year-month.

  Purpose: Portfolio storytelling — shows whether warmer months correlate with
  higher renewable energy percentage (e.g. more sun/wind) and lower carbon intensity.

  The correlation label is a qualitative classification to make dashboards readable
  without requiring end users to interpret raw numbers.
*/

with fact as (

  select *
  from {{ ref('fact_energy_readings') }}

),

dim_loc as (

  select location_key, city, country
  from {{ ref('dim_location') }}

),

dim_t as (

  select time_key, year, month, month_name
  from {{ ref('dim_time') }}

),

dim_wc as (

  select weathercode, condition_category
  from {{ ref('dim_weather_condition') }}

),

monthly as (

  select
    dl.city,
    dl.country,
    dt.year,
    dt.month,
    dt.month_name,

    round(avg(f.temperature_2m),    2)  as avg_temp_c,
    round(max(f.temperature_2m),    2)  as max_temp_c,
    round(min(f.temperature_2m),    2)  as min_temp_c,
    round(avg(f.carbon_intensity),  2)  as avg_carbon_intensity,
    round(avg(f.renewable_pct),     2)  as avg_renewable_pct,
    round(avg(f.fossil_free_pct),   2)  as avg_fossil_free_pct,
    round(avg(f.precipitation),     3)  as avg_hourly_precipitation,
    count(*)                            as reading_count

  from fact f
  left join dim_loc dl on f.location_key = dl.location_key
  left join dim_t   dt on f.time_key     = dt.time_key

  group by 1, 2, 3, 4, 5

),

dominant_condition as (

  -- Find the most common weather condition category per city per month.
  -- Used for a single-word weather summary in dashboards.
  select
    dl.city,
    dt.year,
    dt.month,
    wc.condition_category,
    count(*) as cnt,
    row_number() over (
      partition by dl.city, dt.year, dt.month
      order by count(*) desc
    ) as rn

  from fact f
  left join dim_loc dl on f.location_key = dl.location_key
  left join dim_t   dt on f.time_key     = dt.time_key
  left join dim_wc  wc on f.weathercode_key = wc.weathercode

  group by 1, 2, 3, 4

  qualify rn = 1

),

final as (

  select
    m.city,
    m.country,
    m.year,
    m.month,
    m.month_name,

    m.avg_temp_c,
    m.max_temp_c,
    m.min_temp_c,
    m.avg_carbon_intensity,
    m.avg_renewable_pct,
    m.avg_fossil_free_pct,
    m.avg_hourly_precipitation,
    m.reading_count,

    coalesce(dc.condition_category, 'unknown')  as dominant_weather_category,

    -- Qualitative correlation label for BI dashboards.
    -- High temp + low carbon = likely renewable-heavy month (solar/wind).
    -- Low temp + high carbon = likely fossil-heavy month (heating demand, less solar).
    case
      when m.avg_temp_c >= 15 and m.avg_carbon_intensity <= 200 then 'warm_clean'
      when m.avg_temp_c >= 15 and m.avg_carbon_intensity >  200 then 'warm_carbon'
      when m.avg_temp_c <  15 and m.avg_carbon_intensity <= 200 then 'cool_clean'
      when m.avg_temp_c <  15 and m.avg_carbon_intensity >  200 then 'cool_carbon'
      else 'unknown'
    end                                         as temp_carbon_profile

  from monthly m
  left join dominant_condition dc
    on m.city = dc.city
    and m.year = dc.year
    and m.month = dc.month

)

select * from final
order by city, year, month
