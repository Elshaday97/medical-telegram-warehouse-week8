
with detections as (
    select * from {{ ref('stg_image_detections') }}
),

messages as (
    -- This tells dbt: "Find the table where you built the fct_messages model"
    select * from {{ ref('fct_messages') }}
),

final as (
    select
        -- 1. Create a Unique Key for this detection event
        md5(detections.image_path) as detection_key,

-- 2. Foreign Keys (Inherited from the message)
messages.channel_key, messages.date_key, messages.message_id,

-- 3. Detection Data


detections.image_category, 
        detections.confidence_score

    from detections
    -- Join to fct_messages to attach the correct Channel and Date keys
    inner join messages 
        on detections.message_id = messages.message_id
        and detections.channel_name = messages.channel_name
)

select * from final