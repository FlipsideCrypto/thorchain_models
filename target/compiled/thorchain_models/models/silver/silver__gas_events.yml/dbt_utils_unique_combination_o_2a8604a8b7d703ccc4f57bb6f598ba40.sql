





with validation_errors as (

    select
        EVENT_ID, ASSET, BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.gas_events
    group by EVENT_ID, ASSET, BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors


