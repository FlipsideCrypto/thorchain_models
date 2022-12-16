

 with max_recency as (

    select max(cast(DAY as 
    timestamp_ntz
)) as max_timestamp
    from
        THORCHAIN_DEV.silver.daily_pool_stats
    where
        -- to exclude erroneous future dates
        cast(DAY as 
    timestamp_ntz
) <= cast(convert_timezone('UTC', 'GMT', 
    current_timestamp::
    timestamp_ntz

) as 
    timestamp_ntz
)
        
)
select
    *
from
    max_recency
where
    -- if the row_condition excludes all rows, we need to compare against a default date
    -- to avoid false negatives
    coalesce(max_timestamp, cast('1970-01-01' as 
    timestamp_ntz
))
        <
        cast(

    dateadd(
        day,
        -2,
        cast(convert_timezone('UTC', 'GMT', 
    current_timestamp::
    timestamp_ntz

) as 
    timestamp_ntz
)
        )

 as 
    timestamp_ntz
)



