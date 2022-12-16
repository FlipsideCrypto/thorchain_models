
  create or replace  view THORCHAIN_DEV.silver.gas_events
  
    
    
(
  
    "ASSET" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "RUNE_E8" COMMENT $$$$, 
  
    "TX_COUNT" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  asset,
  asset_e8,
  rune_e8,
  tx_count,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.gas_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, asset, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
