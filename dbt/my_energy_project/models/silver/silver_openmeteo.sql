{{ config(
    materialized='incremental',
    unique_key=['location_name', 'timestamp']
) }}

WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'bronze_openmeteo_raw') }}

),

expanded AS (

    SELECT
        s.location_name,

        t.time_val::timestamp AS timestamp,
        temp.temp_val::float AS temperature_2m,
        p.precip_val::float AS precipitation,
        w.wind_val::float AS windspeed_10m,

        s.start_dt,
        s.upsert_ts

    FROM source_data s

    -- time = anchor
    CROSS JOIN LATERAL jsonb_array_elements_text(s.hourly_json->'time')
        WITH ORDINALITY AS t(time_val, idx)

    -- join all other arrays on SAME index
    JOIN LATERAL jsonb_array_elements_text(s.hourly_json->'temperature_2m')
        WITH ORDINALITY AS temp(temp_val, idx)
        ON temp.idx = t.idx

    JOIN LATERAL jsonb_array_elements_text(s.hourly_json->'precipitation')
        WITH ORDINALITY AS p(precip_val, idx)
        ON p.idx = t.idx

    JOIN LATERAL jsonb_array_elements_text(s.hourly_json->'windspeed_10m')
        WITH ORDINALITY AS w(wind_val, idx)
        ON w.idx = t.idx

),

deduplicated AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY location_name, timestamp
               ORDER BY upsert_ts DESC
           ) AS rn
    FROM expanded

)

SELECT
    location_name,
    timestamp,
    temperature_2m,
    precipitation,
    windspeed_10m,
    start_dt,
    upsert_ts

FROM deduplicated
WHERE rn = 1

{% if is_incremental() %}
AND upsert_ts > (SELECT MAX(upsert_ts) FROM {{ this }})
{% endif %}