





with validation_errors as (

    select
        HEIGHT, TIMESTAMP, HASH, AGG_STATE
    from THORCHAIN_DEV.silver.block_log
    group by HEIGHT, TIMESTAMP, HASH, AGG_STATE
    having count(*) > 1

)

select *
from validation_errors


