
  create or replace  view THORCHAIN_DEV.bronze.switch_events
  
    
    
(
  
    "TX" COMMENT $$$$, 
  
    "FROM_ADDR" COMMENT $$$$, 
  
    "TO_ADDR" COMMENT $$$$, 
  
    "BURN_ASSET" COMMENT $$$$, 
  
    "BURN_E8" COMMENT $$$$, 
  
    "MINT_E8" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "__HEVO_XMIN" COMMENT $$$$, 
  
    "__HEVO__DATABASE_NAME" COMMENT $$$$, 
  
    "__HEVO__SCHEMA_NAME" COMMENT $$$$, 
  
    "__HEVO__INGESTED_AT" COMMENT $$$$, 
  
    "__HEVO__LOADED_AT" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  COALESCE(
    tx,
    ''
  ) AS tx,
  COALESCE(
    from_addr,
    ''
  ) AS from_addr,
  COALESCE(
    to_addr,
    ''
  ) AS to_addr,
  COALESCE(
    burn_asset,
    ''
  ) AS burn_asset,
  burn_e8,
  mint_e8,
  block_timestamp,
  event_id,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_switch_events
  );
