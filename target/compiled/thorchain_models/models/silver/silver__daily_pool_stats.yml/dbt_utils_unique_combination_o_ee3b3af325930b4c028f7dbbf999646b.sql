





with validation_errors as (

    select
        DAY, POOL_NAME
    from THORCHAIN_DEV.silver.daily_pool_stats
    group by DAY, POOL_NAME
    having count(*) > 1

)

select *
from validation_errors


