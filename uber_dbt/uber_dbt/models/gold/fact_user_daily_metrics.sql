WITH user_events AS (
    SELECT
        s.user_id,
        s.event_type,
        s.event_ts,
        s.trip_distance,
        s.trip_minutes,
        s.fare_amount,
        CAST(s.event_ts AS DATE) AS day
    FROM {{ ref('uber_silver') }} s
    WHERE s.user_id IS NOT NULL
)

SELECT
    md5(u.user_id || u.day) AS user_daily_sk,

    du.user_sk,
    dt.date_sk,

    COUNT(*) AS total_events,
    COUNT(CASE WHEN u.event_type = 'trip_requested' THEN 1 END) AS requests,
    COUNT(CASE WHEN u.event_type = 'trip_started' THEN 1 END) AS starts,
    COUNT(CASE WHEN u.event_type = 'trip_completed' THEN 1 END) AS completions,

    SUM(u.trip_distance) AS total_distance,
    SUM(u.fare_amount) AS total_fare,

    AVG(u.fare_amount) AS avg_fare

FROM user_events u
LEFT JOIN {{ ref('dim_user') }} du ON u.user_id = du.user_id
LEFT JOIN {{ ref('dim_date') }} dt ON u.day     = dt.day

GROUP BY
    u.user_id, u.day,
    du.user_sk, dt.date_sk

ORDER BY u.day DESC
