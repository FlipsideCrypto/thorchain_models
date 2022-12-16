





with validation_errors as (

    select
        POOL_NAME, ASSET_TX_ID, ASSET_BLOCKCHAIN, ASSET_ADDRESS, STAKE_UNITS, RUNE_TX_ID, RUNE_ADDRESS, BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.stake_events
    group by POOL_NAME, ASSET_TX_ID, ASSET_BLOCKCHAIN, ASSET_ADDRESS, STAKE_UNITS, RUNE_TX_ID, RUNE_ADDRESS, BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors


