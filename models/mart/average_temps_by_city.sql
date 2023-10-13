with avg_temps as (
    select 
        city,
        region,
        country,
        avg(maxtemp_c) as avg_maxtemp,
        avg(mintemp_c) as avg_mintemp,
        avg(avgtemp_c) as avg_avgtemp
    from {{ref('prep_temp')}}
    group by city, region, country
)

select *
from avg_temps