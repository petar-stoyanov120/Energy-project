{{
  config(materialized = 'table')
}}

/*
  Gold: dim_weather_condition
  ---------------------------
  Static lookup table mapping WMO 4677 weather codes to human-readable labels,
  broad categories, and severity levels.

  No upstream dbt dependency — rebuilt from inline VALUES on every run.
  Adding a new WMO code means adding a row here and running dbt run --select dim_weather_condition.
*/

with wmo_codes as (

  select * from (values
    -- code  label                       category   severity
    (0,  'Clear Sky',              'sunny',   'mild'),
    (1,  'Mainly Clear',           'sunny',   'mild'),
    (2,  'Partly Cloudy',          'cloudy',  'mild'),
    (3,  'Overcast',               'cloudy',  'mild'),
    (45, 'Foggy',                  'foggy',   'moderate'),
    (48, 'Depositing Rime Fog',    'foggy',   'moderate'),
    (51, 'Light Drizzle',          'rainy',   'mild'),
    (53, 'Moderate Drizzle',       'rainy',   'moderate'),
    (55, 'Dense Drizzle',          'rainy',   'moderate'),
    (61, 'Slight Rain',            'rainy',   'mild'),
    (63, 'Moderate Rain',          'rainy',   'moderate'),
    (65, 'Heavy Rain',             'rainy',   'severe'),
    (71, 'Slight Snowfall',        'snowy',   'mild'),
    (73, 'Moderate Snowfall',      'snowy',   'moderate'),
    (75, 'Heavy Snowfall',         'snowy',   'severe'),
    (80, 'Slight Rain Showers',    'rainy',   'mild'),
    (81, 'Moderate Rain Showers',  'rainy',   'moderate'),
    (82, 'Violent Rain Showers',   'rainy',   'severe'),
    (95, 'Thunderstorm',           'stormy',  'severe'),
    (96, 'Thunderstorm with Hail', 'stormy',  'severe'),
    (99, 'Heavy Thunderstorm',     'stormy',  'severe')
  ) as t(weathercode, condition_label, condition_category, severity)

)

select
  cast(weathercode        as int)    as weathercode,
  cast(condition_label    as string) as condition_label,
  cast(condition_category as string) as condition_category,
  cast(severity           as string) as severity
from wmo_codes
