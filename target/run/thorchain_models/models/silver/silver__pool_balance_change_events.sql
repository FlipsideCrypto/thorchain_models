
  create or replace  view THORCHAIN_DEV.silver.pool_balance_change_events
  
    
    
(
  
    "ASSET" COMMENT $$$$, 
  
    "RUNE_AMOUNT" COMMENT $$$$, 
  
    "RUNE_ADD" COMMENT $$$$, 
  
    "ASSET_AMOUNT" COMMENT $$$$, 
  
    "ASSET_ADD" COMMENT $$$$, 
  
    "REASON" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  asset,
  rune_amt AS rune_amount,
  rune_add,
  asset_amt AS asset_amount,
  asset_add,
  reason,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.pool_balance_change_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, asset, reason, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
