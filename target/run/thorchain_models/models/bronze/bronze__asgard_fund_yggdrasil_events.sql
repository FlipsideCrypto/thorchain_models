
  create or replace  view THORCHAIN_DEV.bronze.asgard_fund_yggdrasil_events
  
    
    
(
  
    "TX" COMMENT $$$$, 
  
    "ASSET" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "VAULT_KEY" COMMENT $$$$, 
  
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
  tx,
  asset,
  asset_e8,
  vault_key,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_asgard_fund_yggdrasil_events
  );
