SELECT
    md5(CAST(event_ts AS DATE)) AS date_sk,
    CAST(event_ts AS DATE) AS day,
    YEAR(event_ts) AS year,
    MONTH(event_ts) AS month,
    DAY(event_ts) AS date,
    DAYOFWEEK(event_ts) AS weekday,
    WEEK(event_ts) AS week_num
FROM {{ ref('uber_silver') }}
WHERE event_ts IS NOT NULL
GROUP BY
    CAST(event_ts AS DATE),
    YEAR(event_ts),
    MONTH(event_ts),
    DAY(event_ts),
    DAYOFWEEK(event_ts),
    WEEK(event_ts)
ORDER BY day
