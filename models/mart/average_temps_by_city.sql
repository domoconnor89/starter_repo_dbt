/*

WITH avg_temps as (
    SELECT 
        city,
        region,
        country,
        avg(maxtemp_c) as avg_maxtemp,
        avg(mintemp_c) as avg_mintemp,
        avg(avgtemp_c) as avg_avgtemp
    FROM {{ref('prep_temp')}}
    GROUP BY city, region, country
)

SELECT *
FROM avg_temps

*/