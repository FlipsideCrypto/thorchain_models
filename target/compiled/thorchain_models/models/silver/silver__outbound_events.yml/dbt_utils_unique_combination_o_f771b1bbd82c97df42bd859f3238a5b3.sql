





with validation_errors as (

    select
        TX_ID, BLOCKCHAIN, FROM_ADDRESS, TO_ADDRESS, ASSET, MEMO, IN_TX, BLOCK_TIMESTAMP, EVENT_ID
    from THORCHAIN_DEV.silver.outbound_events
    group by TX_ID, BLOCKCHAIN, FROM_ADDRESS, TO_ADDRESS, ASSET, MEMO, IN_TX, BLOCK_TIMESTAMP, EVENT_ID
    having count(*) > 1

)

select *
from validation_errors


