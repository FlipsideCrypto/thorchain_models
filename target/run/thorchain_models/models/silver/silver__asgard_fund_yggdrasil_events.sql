
  create or replace  view THORCHAIN_DEV.silver.asgard_fund_yggdrasil_events
  
    
    
(
  
    "TX_ID" COMMENT $$$$, 
  
    "ASSET" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "VAULT_KEY" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "EVENT_ID_2" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  tx AS tx_id,
  asset,
  asset_e8,
  vault_key,
  event_id,
  block_timestamp,
  event_id,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.asgard_fund_yggdrasil_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, asset, asset_e8, vault_key, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
