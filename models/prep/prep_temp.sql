WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_weather')}}
),
time_cnvrt AS (
    SELECT *,
        TO_CHAR(date,'Day') AS weekday,
        TO_CHAR(date,'DD') AS day_num,
		TO_CHAR(date,'week') AS week,
		TO_CHAR(date,'month') AS month,
        TO_CHAR(date,'year') AS year,
        TO_CHAR(TO_TIMESTAMP(sunrise, 'HH12:MI PM'), 'HH24:MI') AS sunrise_24hr,
        TO_CHAR(TO_TIMESTAMP(sunset, 'HH12:MI PM'), 'HH24:MI') AS sunset_24hr,
        TO_CHAR(TO_TIMESTAMP(moonrise, 'HH12:MI PM'), 'HH24:MI') AS moonrise_24hr,
        TO_CHAR(TO_TIMESTAMP(moonset, 'HH12:MI PM'), 'HH24:MI') AS moonset_24hr,
        TO_CHAR(moon_illumination, NUMERIC) AS moon_illumination_num
    FROM temp_daily
),
temp_update AS (
    SELECT 
        date,
        weekday,
        day_num,
        week,
        month,
        year,
        city,
        region,
        country,
        lat,
        lon,
        sunrise_24hr,
        sunset_24hr,
        moonrise_24hr,
        moonset_24hr,
        moon_phase,
        moon_illumination_num,
        maxtemp_c,
        avgtemp_c,
        mintemp_c,
        maxwind_kph,
        totalprecip_mm,
        avgvis_km,
        avghumidity,
        condition
    FROM time_cnvrt
)
SELECT *
FROM temp_update
