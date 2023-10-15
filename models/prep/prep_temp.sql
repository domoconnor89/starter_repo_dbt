WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_weather')}}
),
(UPDATE temp_daily
    SET moonrise = ''
        WHERE moonrise = 'No moonrise'
    SET moonrise = TO_CHAR(TO_TIMESTAMP(moonrise, 'HH12:MI AM'), 'HH24:MI')
        WHERE moonrise IS NOT NULL),
moonrise_cte AS (
  SELECT
    moonrise,
    LAG(moonrise) OVER (ORDER BY some_order_column) AS prev_moonrise,
    LEAD(moonrise) OVER (ORDER BY some_order_column) AS next_moonrise
  FROM temp_daily
),
(UPDATE temp_daily
SET moonrise = TO_CHAR(
  (TO_TIMESTAMP(prev_moonrise, 'HH24:MI') + TO_TIMESTAMP(next_moonrise, 'HH24:MI')) / 2,
  'HH24:MI'
)
WHERE moonrise IS NULL
),
time_cnvrt AS (
    SELECT *,
        TO_CHAR(date,'Day') AS weekday,
        TO_CHAR(date,'DD') AS day_num,
		TO_CHAR(date,'week') AS week,
		TO_CHAR(date,'month') AS month,
        TO_CHAR(date,'year') AS year,
        --TO_CHAR(TO_TIMESTAMP(sunrise, 'HH12:MI PM'), 'HH24:MI') AS sunrise_24hr,
        --TO_CHAR(TO_TIMESTAMP(sunset, 'HH12:MI PM'), 'HH24:MI') AS sunset_24hr,
        --TO_CHAR(TO_TIMESTAMP(moonrise, 'HH12:MI PM'), 'HH24:MI') AS moonrise_24hr,
        --TO_CHAR(TO_TIMESTAMP(moonset, 'HH12:MI PM'), 'HH24:MI') AS moonset_24hr,
        CAST(moon_illumination AS INTEGER) AS moon_illumination_num
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
        --sunrise_24hr,
        --sunset_24hr,
        moonrise,
        --moonrise_24hr,
        --moonset_24hr,
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
