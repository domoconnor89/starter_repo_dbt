WITH avg_vis as (
    SELECT 
        year,
        month,
        city,
        region,
        country,
        MAX(avgvis_km) as wk_day_maxvis,
        AVG(avgvis_km) as wkly_avg_vis,
        MIN(avgvis_km) as wk_day_minv_vis
    FROM {{ref('prep_temp')}}
    GROUP BY year, month, city, region, country
    ORDER BY city, year, month
)
SELECT *
FROM avg_vis