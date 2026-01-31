-- Looks for records that violate the rule.
-- Rule: Views must be >= 0
-- Failing Condition: Views < 0

select
    message_id,
    view_count
from {{ ref('stg_telegram_messages') }}
where view_count < 0