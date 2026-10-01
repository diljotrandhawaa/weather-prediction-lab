select city_date_key
from {{ ref('stg_weather_forecast') }}
where precipitation_mm < 0
