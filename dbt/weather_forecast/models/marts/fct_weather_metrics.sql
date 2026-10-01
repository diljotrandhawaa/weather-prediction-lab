with forecasts as (
    select * from {{ ref('stg_weather_forecast') }}
),
wet_groups as (
    select
        *,
        sum(case when precipitation_mm >= 1 then 1 else 0 end) over (
            partition by city order by forecast_date
            rows between unbounded preceding and current row
        ) as wet_group
    from forecasts
),
metrics as (
    select
        city_date_key,
        city,
        forecast_date,
        weather_code,
        temperature_max_c,
        temperature_min_c,
        precipitation_mm,
        round(avg(temperature_max_c) over (
            partition by city order by forecast_date
            rows between 6 preceding and current row
        ), 2) as rolling_7d_max_temp_c,
        round(sum(coalesce(precipitation_mm, 0)) over (
            partition by city order by forecast_date
            rows between 6 preceding and current row
        ), 2) as rolling_7d_precipitation_mm,
        sum(case when precipitation_mm < 1 then 1 else 0 end) over (
            partition by city, wet_group order by forecast_date
            rows between unbounded preceding and current row
        ) as dry_spell_days,
        last_changed_at
    from wet_groups
)
select
    *,
    round(temperature_max_c - rolling_7d_max_temp_c, 2) as temp_anomaly_c
from metrics
