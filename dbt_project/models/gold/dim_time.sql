{{
  config(materialized = 'table')
}}

/*
  Gold: dim_time
  --------------
  Date/time dimension at hourly granularity covering 2024-01-01 to 2025-12-31.
  Generated via Spark SQL sequence() — no upstream data dependency.

  ~17,520 rows (2 years × 8,760 hours). Trivially fast even on a small cluster.

  To extend the range: change the end timestamp in the sequence() call and run
  dbt run --select dim_time. No data migration required.
*/

with hour_spine as (

  -- sequence() generates an array of timestamps at 1-hour intervals.
  -- explode() flattens the array into rows — one row per hour.
  select explode(
    sequence(
      to_timestamp('2024-01-01 00:00:00'),
      to_timestamp('2025-12-31 23:00:00'),
      interval 1 hour
    )
  ) as full_timestamp

),

final as (

  select
    -- time_key is the natural join key used in fact_energy_readings.
    -- Format: yyyyMMddHH — e.g. "2024011512" = 2024-01-15 at noon.
    date_format(full_timestamp, 'yyyyMMddHH')         as time_key,

    full_timestamp,

    cast(year(full_timestamp)  as int)                as year,
    cast(month(full_timestamp) as int)                as month,
    cast(day(full_timestamp)   as int)                as day,
    cast(hour(full_timestamp)  as int)                as hour,

    -- Day name for reporting (e.g. "Monday")
    date_format(full_timestamp, 'EEEE')               as day_of_week,

    -- ISO day of week: 2=Monday ... 1=Sunday in Spark (matches Java Calendar)
    cast(dayofweek(full_timestamp) as int)            as day_of_week_num,

    -- Weekend flag: Spark dayofweek returns 1=Sunday, 7=Saturday
    case
      when dayofweek(full_timestamp) in (1, 7) then true
      else false
    end                                               as is_weekend,

    cast(quarter(full_timestamp) as int)              as quarter,

    -- Month name for display in BI tools
    date_format(full_timestamp, 'MMMM')               as month_name

  from hour_spine

)

select * from final
