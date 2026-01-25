
with stg_messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

dim_channels as (
    select * from {{ ref('dim_channels') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
),

final as (
    select
        -- 1. Create Key for the Fact Table
        md5(stg.message_id || stg.channel_name) as message_key,

-- 2. Foreign Keys
channels.channel_key, dates.date_key,

-- 3. Degenerate Dimensions (IDs that are still useful)
stg.message_id, stg.channel_name,

-- 4. Metrics
stg.view_count, stg.forward_count, stg.message_length,

-- 5. Context Flags
stg.has_media from stg_messages as stg

-- Join


left join dim_channels as channels
        on stg.channel_name = channels.channel_name
        
    left join dim_dates as dates
        on date(stg.message_date) = dates.date_day
)

select * from final