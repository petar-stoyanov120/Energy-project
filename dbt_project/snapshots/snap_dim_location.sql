{% snapshot snap_dim_location %}

{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'city',
    strategy      = 'check',
    check_cols    = ['latitude', 'longitude', 'timezone', 'electricity_zone']
  )
}}

/*
  SCD Type 2 snapshot: snap_dim_location
  ----------------------------------------
  Tracks historical changes to city metadata from the regions seed.
  Whenever a city's coordinates, timezone, or electricity zone changes,
  a new record is created with updated dbt_valid_from/dbt_valid_to timestamps,
  and the old record is closed (dbt_valid_to is set).

  Run with: dbt snapshot

  dbt automatically manages:
    - dbt_scd_id       (unique row identifier)
    - dbt_updated_at   (when the snapshot was last checked)
    - dbt_valid_from   (when this version became active)
    - dbt_valid_to     (when this version was superseded; NULL = still current)
    - dbt_is_current   (convenience boolean; True for the active record)
*/

select
  city,
  country,
  latitude,
  longitude,
  timezone,
  electricity_zone
from {{ ref('regions') }}

{% endsnapshot %}
