WITH avg_precip as (
    SELECT 
        year,
        week,
        city,
        region,
        country,
        MAX(totalprecip_mm) as wk_maxprecip_day,
        SUM(rainy_day) as wkly_precip_days,
        SUM(totalprecip_mm) as wkly_totalprecip,
        CASE
            WHEN wkly_precip_days > 0 THEN 1/wkly_precip_days
            WHEN wkly_precip_days = 0 THEN 0
            END AS rainfall_spread
    FROM {{ref('prep_temp')}}
    GROUP BY year, week, city, region, country
    ORDER BY city, year, week
)
SELECT *
FROM avg_precip