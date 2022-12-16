





with validation_errors as (

    select
        IN_TX, ASSET, BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.errata_events
    group by IN_TX, ASSET, BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors


