WITH avg_precip as (
    SELECT 
        year,
        month,
        city,
        region,
        country,
        MAX(totalprecip_mm) as wk_day_maxprecip,
        SUM(rainy_day) as wkly_precip_days,
        SUM(totalprecip_mm) as wkly_totalprecip
    FROM {{ref('prep_temp')}}
    GROUP BY year, month, city, region, country
    ORDER BY city, year, month
)
SELECT *
FROM avg_precip