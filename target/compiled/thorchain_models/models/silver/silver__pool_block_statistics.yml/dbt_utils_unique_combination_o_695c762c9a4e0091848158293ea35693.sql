





with validation_errors as (

    select
        DAY, ASSET
    from THORCHAIN_DEV.silver.pool_block_statistics
    group by DAY, ASSET
    having count(*) > 1

)

select *
from validation_errors


