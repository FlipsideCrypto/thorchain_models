





with validation_errors as (

    select
        EVENT_ID, TX_ID, BLOCKCHAIN, POOL_NAME, FROM_ADDRESS, TO_ADDRESS, MEMO
    from THORCHAIN_DEV.silver.add_events
    group by EVENT_ID, TX_ID, BLOCKCHAIN, POOL_NAME, FROM_ADDRESS, TO_ADDRESS, MEMO
    having count(*) > 1

)

select *
from validation_errors


