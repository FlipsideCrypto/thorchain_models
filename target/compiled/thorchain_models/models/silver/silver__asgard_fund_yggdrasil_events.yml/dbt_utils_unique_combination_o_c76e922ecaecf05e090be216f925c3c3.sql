





with validation_errors as (

    select
        EVENT_ID, TX_ID, ASSET, ASSET_E8, VAULT_KEY, BLOCK_TIMESTAMP
    from THORCHAIN_DEV.silver.asgard_fund_yggdrasil_events
    group by EVENT_ID, TX_ID, ASSET, ASSET_E8, VAULT_KEY, BLOCK_TIMESTAMP
    having count(*) > 1

)

select *
from validation_errors


