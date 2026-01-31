
with source as (
    select * from {{ source('telegram_source', 'image_detections') }}
),

cleaned as (
    select
        -- ID columns
        cast(message_id as integer) as message_id,
        channel_name,

-- Detection Metrics


detected_class as image_category, -- 'promotional', 'lifestyle', etc.
        cast(confidence_score as numeric) as confidence_score
        
    from source
    -- Filter out any failed detections if necessary
    where message_id is not null
)

select * from cleaned