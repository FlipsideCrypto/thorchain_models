
  create or replace  view THORCHAIN_DEV.bronze.pool_balance_change_events
  
    
    
(
  
    "ASSET" COMMENT $$$$, 
  
    "RUNE_AMT" COMMENT $$$$, 
  
    "RUNE_ADD" COMMENT $$$$, 
  
    "ASSET_AMT" COMMENT $$$$, 
  
    "ASSET_ADD" COMMENT $$$$, 
  
    "REASON" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "__HEVO_XMIN" COMMENT $$$$, 
  
    "__HEVO__DATABASE_NAME" COMMENT $$$$, 
  
    "__HEVO__SCHEMA_NAME" COMMENT $$$$, 
  
    "__HEVO__INGESTED_AT" COMMENT $$$$, 
  
    "__HEVO__LOADED_AT" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  asset,
  rune_amt,
  rune_add,
  asset_amt,
  asset_add,
  reason,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_pool_balance_change_events
  );
