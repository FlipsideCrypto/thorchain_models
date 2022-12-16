





with validation_errors as (

    select
        DAY, POOL_NAME
    from THORCHAIN_DEV.silver.pool_block_fees
    group by DAY, POOL_NAME
    having count(*) > 1

)

select *
from validation_errors


