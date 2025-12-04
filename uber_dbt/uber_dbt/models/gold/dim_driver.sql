SELECT
    md5(driver_id) AS driver_sk,
    driver_id
FROM {{ ref('uber_silver') }}
WHERE driver_id IS NOT NULL
GROUP BY driver_id
ORDER BY driver_id
