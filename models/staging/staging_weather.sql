WITH temperature_daily AS (
    SELECT ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'date')::VARCHAR)::date AS date,
        (extracted_data -> 'location' -> 'name')::VARCHAR  AS city,
        (extracted_data -> 'location' -> 'region')::VARCHAR  AS region,
        (extracted_data -> 'location' -> 'country')::VARCHAR  AS country,
        ((extracted_data -> 'location' -> 'lat')::VARCHAR)::NUMERIC  AS lat, 
        ((extracted_data -> 'location' -> 'lon')::VARCHAR)::NUMERIC  AS lon,
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'sunrise')::VARCHAR AS sunrise, 
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'sunset')::VARCHAR AS sunset, 
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'moonrise')::VARCHAR AS moonrise, 
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'moonset')::VARCHAR AS moonset, 
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'moon_phase')::VARCHAR AS moon_phase, 
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxtemp_c')::VARCHAR)::FLOAT AS maxtemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgtemp_c')::VARCHAR)::FLOAT AS avgtemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'mintemp_c')::VARCHAR)::FLOAT AS mintemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxwind_kph')::VARCHAR)::FLOAT AS maxwind_kph,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'totalprecip_mm')::VARCHAR)::FLOAT AS totalprecip_mm,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgvis_km')::VARCHAR)::FLOAT AS avgvis_km,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avghumidity')::VARCHAR)::FLOAT AS avghumidity,
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'condition' -> 'text')::VARCHAR AS condition
    FROM {{source("staging", "raw_temp")}}),
        temperatutre_daily_updated AS (
            SELECT 
                date,
                substring(city, 2, (length(city)-2)) as city,
                substring(region, 2, (length(region)-2)) as region,
                substring(country, 2, (length(country)-2)) as country,
                lat,
                lon,
                substring(sunrise, 2, (length(country)-2)) as sunrise,
                substring(sunset, 2, (length(country)-2)) as sunset,
                substring(moonrise, 2, (length(country)-2)) as moonrise,
                substring(moonset, 2, (length(country)-2)) as moonset,
                substring(moon_phase, 2, (length(country)-2)) as moon_phase,
                maxtemp_c,
                avgtemp_c,
                mintemp_c,
                maxwind_kmh,
                totalprecip_mm,
                avgvis_km,
                avghumidity,
                substring(condition, 2, (length(country)-2)) as condition
            FROM temperature_daily
)
SELECT *
FROM temperatutre_daily_updated



/*
WITH temperature_daily AS (
    SELECT ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'date')::VARCHAR)::date  AS date,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxtemp_c')::VARCHAR)::FLOAT AS maxtemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'mintemp_c')::VARCHAR)::FLOAT AS mintemp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgtemp_c')::VARCHAR)::FLOAT AS avgtemp_c,
        (extracted_data -> 'location' -> 'name')::VARCHAR  AS city,
        (extracted_data -> 'location' -> 'region')::VARCHAR  AS region,
        (extracted_data -> 'location' -> 'country')::VARCHAR  AS country,
        ((extracted_data -> 'location' -> 'lat')::VARCHAR)::NUMERIC  AS lat, 
        ((extracted_data -> 'location' -> 'lon')::VARCHAR)::NUMERIC  AS lon
    FROM {{source("staging", "raw_temp")}}),
        temperatutre_daily_updated AS (
            SELECT 
                date,
                maxtemp_c as maxtemp_c,
                mintemp_c,
                avgtemp_c,
                substring(city, 2, (length(city)-2)) as city,
                substring(region, 2, (length(region)-2)) as region,
                substring(country, 2, (length(country)-2)) as country,
                lat,
                lon
            FROM temperature_daily
)
SELECT *
FROM temperatutre_daily_updated 



        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'date')::VARCHAR)::date  AS date,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'time')::VARCHAR)::date  AS time,   
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'temp_c')::VARCHAR)::FLOAT AS temp_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'wind_mph')::VARCHAR)::FLOAT AS wind_mph,
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'wind_dir')::VARCHAR AS wind_dir,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'precip_mm')::VARCHAR)::FLOAT AS precip_mm,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'cloud')::VARCHAR)::NUMERIC AS cloud_cover_pc,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'feelslike_c')::VARCHAR)::FLOAT AS feelslike_c,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'chance_of_rain')::VARCHAR)::NUMERIC AS chance_of_rain_pc,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'chance_of_snow')::VARCHAR)::NUMERIC AS chance_of_snow_pc,
        ((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour' -> 'vis_km')::VARCHAR)::FLOAT AS vis_km,
*/