# API Call Conventions

## Open-Meteo

- **URL:** `https://api.open-meteo.com/v1/forecast`
- **Auth:** None — completely free, no key required.
- **Rate limit:** None documented — but do not loop faster than 1 req/sec as courtesy.
- **Parameters always used:**

```python
params = {
    "latitude":      lat,
    "longitude":     lon,
    "hourly":        "temperature_2m,weathercode,windspeed_10m,precipitation",
    "timezone":      timezone,   # must be IANA timezone string e.g. "Europe/London"
    "forecast_days": 1,          # today's forecast only
}
```

- **Response fields used:** `hourly.time`, `hourly.temperature_2m`, `hourly.weathercode`,
  `hourly.windspeed_10m`, `hourly.precipitation`
- **Timestamp format:** ISO 8601 local time string — parse with `datetime.fromisoformat()`

## Electricity Maps

- **URL:** `https://api.electricitymap.org/v3/carbon-intensity/latest`
- **Auth:** `auth-token` header — value from `dbutils.secrets.get(scope="weather_energy", key="electricity_maps_key")`
- **Rate limit:** **1 request per minute on free tier** — enforce 61-second sleep between calls.
- **Parameters:**

```python
headers = {"auth-token": api_key}
params  = {"zone": zone_code}  # e.g. "DE", "GB", "FR"
```

- **Response fields used:** `carbonIntensity` (gCO2eq/kWh), `fossilFreePercentage`,
  `renewablePercentage`, `datetime`
- **Zone codes for our 10 cities:**

| City | Zone |
|---|---|
| London | GB |
| Berlin | DE |
| Paris | FR |
| Madrid | ES |
| Rome | IT |
| Amsterdam | NL |
| Warsaw | PL |
| Vienna | AT |
| Stockholm | SE |
| Lisbon | PT |

## API key security rules

- Never pass the API key as a URL parameter (it would appear in server logs).
- Always pass as a request header.
- Never log the key, the full headers dict, or any string containing the key.
- Never store the key in a variable named `password` or `secret` in a way that
  could be printed by a generic debug statement.

## Adding a new API source

1. Create a new Bronze notebook (`0N_bronze_<source>.py`) following the same structure.
2. Register a secret key in the `weather_energy` scope.
3. Add the source table to `schema.yml` under `sources`.
4. Create a new Silver `stg_<source>.sql` model.
5. Update `03_orchestrator.py` to include the new notebook in the run sequence.
6. Update CLAUDE.md Section 2 (Tech Stack) and Section 7 (References).
