{{
  config(materialized = 'table')
}}

/*
  Gold: dim_location
  ------------------
  Current-state view of the city dimension, sourced from the SCD Type 2 snapshot.
  Only active records (dbt_is_current = true) are exposed here to simplify fact joins.

  Historical records are preserved in the snapshot table (snap_dim_location) for
  auditing and time-travel queries.
*/

with snapshot as (

  select *
  from {{ ref('snap_dim_location') }}
  -- Current records have dbt_valid_to IS NULL (open-ended validity).
  where dbt_valid_to is null

),

final as (

  select
    -- Surrogate key: hash of city name. Stable across SCD2 versions.
    sha2(city, 256)             as location_key,

    cast(city             as string) as city,
    cast(country          as string) as country,
    cast(latitude         as double) as latitude,
    cast(longitude        as double) as longitude,
    cast(timezone         as string) as timezone,
    cast(electricity_zone as string) as electricity_zone,

    -- Expose SCD2 metadata for analysts who need historical context.
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at

  from snapshot

)

select * from final
