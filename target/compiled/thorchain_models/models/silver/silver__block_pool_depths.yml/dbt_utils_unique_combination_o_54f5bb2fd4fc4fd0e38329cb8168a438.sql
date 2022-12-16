





with validation_errors as (

    select
        BLOCK_TIMESTAMP, POOL_NAME
    from THORCHAIN_DEV.silver.block_pool_depths
    group by BLOCK_TIMESTAMP, POOL_NAME
    having count(*) > 1

)

select *
from validation_errors


