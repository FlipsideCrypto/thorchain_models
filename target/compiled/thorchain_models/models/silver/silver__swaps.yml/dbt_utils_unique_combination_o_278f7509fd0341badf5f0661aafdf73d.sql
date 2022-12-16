





with validation_errors as (

    select
        TX_ID, POOL_NAME, FROM_ADDRESS, TO_POOL_ADDRESS, FROM_ASSET, TO_ASSET, NATIVE_TO_ADDRESS, FROM_AMOUNT, TO_AMOUNT
    from THORCHAIN_DEV.silver.swaps
    group by TX_ID, POOL_NAME, FROM_ADDRESS, TO_POOL_ADDRESS, FROM_ASSET, TO_ASSET, NATIVE_TO_ADDRESS, FROM_AMOUNT, TO_AMOUNT
    having count(*) > 1

)

select *
from validation_errors


