
  create or replace  view THORCHAIN_DEV.bronze.stake_events
  
    
    
(
  
    "POOL" COMMENT $$$$, 
  
    "ASSET_TX" COMMENT $$$$, 
  
    "ASSET_CHAIN" COMMENT $$$$, 
  
    "ASSET_ADDR" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "STAKE_UNITS" COMMENT $$$$, 
  
    "RUNE_TX" COMMENT $$$$, 
  
    "RUNE_ADDR" COMMENT $$$$, 
  
    "RUNE_E8" COMMENT $$$$, 
  
    "_ASSET_IN_RUNE_E8" COMMENT $$$$, 
  
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
  pool,
  asset_tx,
  asset_chain,
  asset_addr,
  asset_e8,
  stake_units,
  rune_tx,
  rune_addr,
  rune_e8,
  _ASSET_IN_RUNE_E8,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_stake_events
  );
