
  create or replace  view THORCHAIN_DEV.bronze.transfer_events
  
    
    
(
  
    "FROM_ADDR" COMMENT $$$$, 
  
    "TO_ADDR" COMMENT $$$$, 
  
    "ASSET" COMMENT $$$$, 
  
    "AMOUNT_E8" COMMENT $$$$, 
  
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
  from_addr,
  to_addr,
  asset,
  amount_e8,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_transfer_events
  );
