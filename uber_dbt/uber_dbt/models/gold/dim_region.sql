SELECT
    md5(region) AS region_sk,
    region
FROM {{ ref('uber_silver') }}
WHERE region IS NOT NULL
GROUP BY region
ORDER BY region
