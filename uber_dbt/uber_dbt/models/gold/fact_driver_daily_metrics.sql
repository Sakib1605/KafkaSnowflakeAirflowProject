WITH trips AS (
    SELECT
        s.driver_id,
        s.region,
        s.trip_distance,
        s.trip_minutes,
        s.fare_amount,
        s.event_ts,
        CAST(s.event_ts AS DATE) AS day
    FROM {{ ref('uber_silver') }} s
    WHERE s.event_type = 'trip_completed'
)

SELECT
    md5(t.driver_id || t.day) AS driver_daily_sk,

    d.driver_sk,
    r.region_sk,
    dt.date_sk,

    COUNT(*) AS trips_completed,
    SUM(t.trip_distance) AS total_distance,
    SUM(t.trip_minutes) AS total_trip_minutes,
    SUM(t.fare_amount) AS total_revenue,

    AVG(t.fare_amount) AS avg_fare,
    AVG(t.trip_distance) AS avg_distance

FROM trips t
LEFT JOIN {{ ref('dim_driver') }} d ON t.driver_id = d.driver_id
LEFT JOIN {{ ref('dim_region') }} r ON t.region    = r.region
LEFT JOIN {{ ref('dim_date') }}   dt ON t.day      = dt.day

GROUP BY
    t.driver_id, t.day,
    d.driver_sk, r.region_sk, dt.date_sk

ORDER BY t.day DESC
