select
    city,
    forecast_date,
    concat(city, '|', to_varchar(forecast_date, 'YYYY-MM-DD')) as city_date_key,
    latitude,
    longitude,
    weather_code,
    temperature_max_c,
    temperature_min_c,
    precipitation_mm,
    last_changed_at
from {{ source('raw', 'weather_forecast') }}
