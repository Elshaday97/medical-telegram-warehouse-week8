
with source as (
    -- 1. Import raw data
    select * from {{ source('telegram_source', 'telegram_messages') }}
),

cleaned as (
    -- 2. Basic cleaning and renaming
    select
        id as raw_id,
        message_id,
        channel_name,
        channel_title,
        message_date,
        message_text,
        coalesce(views, 0) as view_count,
        coalesce(forwards, 0) as forward_count,
        has_media,
        case
            when has_media = true then concat(
                '../../../../data/raw/images/',
                channel_name,
                '/',
                message_id,
                '.jpg'
            )
            else null
        end as image_path,
        length(message_text) as message_length

    from source
    where message_id is not null  
)

select * from cleaned