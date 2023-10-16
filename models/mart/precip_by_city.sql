WITH avg_precip as (
    SELECT 
        year,
        week,
        city,
        region,
        country,
        MAX(totalprecip_mm) as wk_day_maxprecip,
        COUNT(totalprecip_mm WHERE totalprecip_mm > 0) as wkly_precip_occurrence,
        SUM(totalprecip_mm) as wkly_totalprecip
    FROM {{ref('prep_temp')}}
    GROUP BY year, week, city, region, country
    ORDER BY city, year, week
)
SELECT *
FROM avg_precip