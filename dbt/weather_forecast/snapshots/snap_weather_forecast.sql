{% snapshot snap_weather_forecast %}
{{
    config(
        target_schema='ANALYTICS',
        unique_key='city_date_key',
        strategy='check',
        check_cols=[
            'weather_code',
            'temperature_max_c',
            'temperature_min_c',
            'precipitation_mm'
        ]
    )
}}

select
    city_date_key,
    city,
    forecast_date,
    weather_code,
    temperature_max_c,
    temperature_min_c,
    precipitation_mm,
    last_changed_at
from {{ ref('stg_weather_forecast') }}

{% endsnapshot %}
