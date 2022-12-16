





with validation_errors as (

    select
        EVENT_ID, TX_ID, BLOCK_TIMESTAMP, FROM_ADDRESS, NODE_ADDRESS
    from THORCHAIN_DEV.silver.validator_request_leave_events
    group by EVENT_ID, TX_ID, BLOCK_TIMESTAMP, FROM_ADDRESS, NODE_ADDRESS
    having count(*) > 1

)

select *
from validation_errors


