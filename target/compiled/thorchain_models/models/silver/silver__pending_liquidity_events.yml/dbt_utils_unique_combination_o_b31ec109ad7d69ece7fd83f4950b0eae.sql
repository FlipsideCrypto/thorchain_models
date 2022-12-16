





with validation_errors as (

    select
        EVENT_ID, POOL_NAME, ASSET_TX_ID, ASSET_BLOCKCHAIN, ASSET_ADDRESS, RUNE_TX_ID, RUNE_ADDRESS, PENDING_TYPE, BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.pending_liquidity_events
    group by EVENT_ID, POOL_NAME, ASSET_TX_ID, ASSET_BLOCKCHAIN, ASSET_ADDRESS, RUNE_TX_ID, RUNE_ADDRESS, PENDING_TYPE, BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors


