WITH avg_precip as (
    SELECT 
        year,
        month,
        city,
        region,
        country,
        MAX(totalprecip_mm) as mnth_maxprecip_day,
        SUM(rainy_day) as mnthly_precip_days,
        SUM(totalprecip_mm) as mnth_totalprecip
    FROM {{ref('prep_temp')}}
    GROUP BY year, month, city, region, country
    ORDER BY city, year, month
)
SELECT *
FROM avg_precip