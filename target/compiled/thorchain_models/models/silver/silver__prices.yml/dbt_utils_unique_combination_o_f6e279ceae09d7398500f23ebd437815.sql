





with validation_errors as (

    select
        BLOCK_ID, POOL_NAME
    from THORCHAIN_DEV.silver.prices
    group by BLOCK_ID, POOL_NAME
    having count(*) > 1

)

select *
from validation_errors


