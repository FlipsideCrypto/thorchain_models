





with validation_errors as (

    select
        TX_ID, BLOCKCHAIN, FROM_ADDRESS, TO_ADDRESS, FROM_ASSET, FROM_E8, TO_ASSET, TO_E8, MEMO, POOL_NAME, TO_E8_MIN, SWAP_SLIP_BP, LIQ_FEE_E8, LIQ_FEE_IN_RUNE_E8, _DIRECTION, BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.swap_events
    group by TX_ID, BLOCKCHAIN, FROM_ADDRESS, TO_ADDRESS, FROM_ASSET, FROM_E8, TO_ASSET, TO_E8, MEMO, POOL_NAME, TO_E8_MIN, SWAP_SLIP_BP, LIQ_FEE_E8, LIQ_FEE_IN_RUNE_E8, _DIRECTION, BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors

