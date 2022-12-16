





with validation_errors as (

    select
        EVENT_ID, BLOCK_TIMESTAMP, ADD_ASGARD_ADDR
    from THORCHAIN_DEV.silver.active_vault_events
    group by EVENT_ID, BLOCK_TIMESTAMP, ADD_ASGARD_ADDR
    having count(*) > 1

)

select *
from validation_errors


