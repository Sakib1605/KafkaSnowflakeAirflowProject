SELECT
    md5(user_id) AS user_sk,
    user_id
FROM {{ ref('uber_silver') }}
WHERE user_id IS NOT NULL
GROUP BY user_id
ORDER BY user_id
