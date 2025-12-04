WITH surge AS (
    SELECT
        s.region,
        s.surge_multiplier,
        s.event_ts,
        CAST(s.event_ts AS DATE) AS day,
        DATE_TRUNC('hour', s.event_ts) AS hour_bucket
    FROM {{ ref('uber_silver') }} s
    WHERE s.event_type = 'surge_pricing'
)

SELECT
    md5(s.region || s.hour_bucket) AS surge_sk,

    r.region_sk,
    dt.date_sk,
    s.region,
    s.hour_bucket,

    AVG(s.surge_multiplier) AS avg_surge,
    MAX(s.surge_multiplier) AS max_surge,
    COUNT(*) AS surge_event_count

FROM surge s
LEFT JOIN {{ ref('dim_region') }} r ON s.region = r.region
LEFT JOIN {{ ref('dim_date') }}   dt ON s.day    = dt.day

GROUP BY
    r.region_sk,
    dt.date_sk,
    s.region,
    s.hour_bucket

ORDER BY s.hour_bucket DESC
