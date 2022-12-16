





with validation_errors as (

    select
        TX_ID, BLOCK_TIMESTAMP, FROM_ADDRESS, TO_ADDRESS, BURN_ASSET
    from THORCHAIN_DEV.silver.upgrades
    group by TX_ID, BLOCK_TIMESTAMP, FROM_ADDRESS, TO_ADDRESS, BURN_ASSET
    having count(*) > 1

)

select *
from validation_errors


