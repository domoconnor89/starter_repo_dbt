WITH avg_temps as (
    SELECT 
        year,
        week,
        city,
        region,
        country,
        MAX(maxtemp_c) as wkly_maxtemp,
        MIN(mintemp_c) as wkly_mintemp,
        AVG(avgtemp_c) as wkly_avgtemp
    FROM {{ref('prep_temp')}}
    GROUP BY year, week, city, region, country
)
SELECT *
FROM avg_temps
