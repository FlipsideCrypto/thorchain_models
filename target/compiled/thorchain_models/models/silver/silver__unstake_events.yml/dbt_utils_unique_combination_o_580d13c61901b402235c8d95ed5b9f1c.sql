





with validation_errors as (

    select
        TX_ID, BLOCKCHAIN, FROM_ADDRESS, TO_ADDRESS, ASSET, MEMO, POOL_NAME, BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.unstake_events
    group by TX_ID, BLOCKCHAIN, FROM_ADDRESS, TO_ADDRESS, ASSET, MEMO, POOL_NAME, BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors


