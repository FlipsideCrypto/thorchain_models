





with validation_errors as (

    select
        TX_ID, BLOCKCHAIN, FROM_ADDRESS, TO_ADDRESS, ASSET, ASSET_2ND, MEMO, CODE, REASON, BLOCK_TIMESTAMP, EVENT_ID
    from THORCHAIN_DEV.silver.refund_events
    group by TX_ID, BLOCKCHAIN, FROM_ADDRESS, TO_ADDRESS, ASSET, ASSET_2ND, MEMO, CODE, REASON, BLOCK_TIMESTAMP, EVENT_ID
    having count(*) > 1

)

select *
from validation_errors


