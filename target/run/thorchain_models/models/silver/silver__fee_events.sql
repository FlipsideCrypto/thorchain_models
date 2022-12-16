
  create or replace  view THORCHAIN_DEV.silver.fee_events
  
    
    
(
  
    "TX_ID" COMMENT $$$$, 
  
    "ASSET" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "POOL_DEDUCT" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  tx AS tx_id,
  asset,
  asset_e8,
  pool_deduct,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.fee_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, asset, asset_e8, pool_deduct, block_timestamp, tx
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
