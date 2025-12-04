WITH bronze_data AS (

    SELECT
        event_id,
        event_type,
        TRY_TO_TIMESTAMP_TZ(timestamp) AS event_ts,

        -- Trip-related fields
        user_id,
        driver_id,
        vehicle_type,
        pickup_lat,
        pickup_lng,
        dropoff_lat,
        dropoff_lng,
        trip_distance,
        trip_minutes,
        fare_amount,
        payment_type,

        -- Surge pricing
        surge_multiplier,

        -- Driver location update
        lat,
        lng,
        status,

        -- Common
        region

    FROM {{ source('bronze', 'uber_events_bronze') }}
)

SELECT *
FROM bronze_data
WHERE event_id IS NOT NULL
  AND event_type IS NOT NULL
  AND event_ts IS NOT NULL
