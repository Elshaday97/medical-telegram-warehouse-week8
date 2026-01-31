with date_series as (
    -- 1. Range of dates covering the data range
    select 
        generate_series(
            '2022-01-01'::timestamp, 
            '2030-12-31'::timestamp, 
            '1 day'::interval
        )::date as date_day
)


select
    -- 2. Create the Primary Key 
    to_char(date_day, 'YYYYMMDD')::int as date_key,
    
    date_day,

-- 3. Extract Useful Attributes for Filtering
extract(
    year
    from date_day
) as year,
extract(
    month
    from date_day
) as month,
to_char (date_day, 'Month') as month_name,
extract(
    day
    from date_day
) as day,
extract(
    dow
    from date_day
) as day_of_week,
to_char (date_day, 'Day') as day_name,
extract(
    quarter
    from date_day
) as quarter,
case
    when extract(
        dow
        from date_day
    ) in (0, 6) then true
    else false
end as is_weekend
from date_series