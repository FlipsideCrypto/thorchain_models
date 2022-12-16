





with validation_errors as (

    select
        NODE_ADDRESS, CURRENT_STATUS, FORMER_STATUS, BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.update_node_account_status_events
    group by NODE_ADDRESS, CURRENT_STATUS, FORMER_STATUS, BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors


