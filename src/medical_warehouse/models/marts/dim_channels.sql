
with stg_messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channel_stats as (
    -- 1. Group by channel to get unique list & basic stats
    select
        channel_name,
        channel_title,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts
    from stg_messages
    group by 1, 2
)

select
    -- 2. Generate a Surrogate Key (Unique ID)
    -- md5 hash of the name to create a consistent, unique ID
    md5(channel_name) as channel_key,
    channel_name,
    channel_title,
    first_post_date,
    last_post_date,
    total_posts,

-- 3. Categorize Channel Tyoes
case
    when lower(channel_title) like '%cosmetics%' then 'Cosmetics'
    when lower(channel_title) like '%pharma%' then 'Pharmaceutical'
    when lower(channel_title) like '%medical%' then 'Medical'
    else 'General Health'
end as channel_type
from channel_stats