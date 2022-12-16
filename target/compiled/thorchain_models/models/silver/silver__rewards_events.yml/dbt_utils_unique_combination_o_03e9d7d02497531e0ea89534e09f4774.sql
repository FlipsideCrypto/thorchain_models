





with validation_errors as (

    select
        BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.rewards_events
    group by BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors


