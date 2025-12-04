WITH completed_trips AS (
    SELECT
        s.event_id,
        s.user_id,
        s.driver_id,
        s.region,
        s.vehicle_type,
        s.trip_distance,
        s.trip_minutes,
        s.fare_amount,
        s.event_ts,
        CAST(s.event_ts AS DATE) AS day
    FROM {{ ref('uber_silver') }} s
    WHERE s.event_type = 'trip_completed'
)

SELECT
    md5(c.event_id) AS trip_sk,

    u.user_sk,
    d.driver_sk,
    r.region_sk,
    dt.date_sk,

    c.event_id,
    c.region,
    c.vehicle_type,
    c.trip_distance,
    c.trip_minutes,
    c.fare_amount,
    c.event_ts

FROM completed_trips c
LEFT JOIN {{ ref('dim_user') }}   u  ON c.user_id   = u.user_id
LEFT JOIN {{ ref('dim_driver') }} d  ON c.driver_id = d.driver_id
LEFT JOIN {{ ref('dim_region') }} r  ON c.region    = r.region
LEFT JOIN {{ ref('dim_date') }}   dt ON c.day       = dt.day

ORDER BY c.event_ts DESC
