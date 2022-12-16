
  create or replace  view THORCHAIN_DEV.bronze.thorname_change_events
  
    
    
(
  
    "NAME" COMMENT $$$$, 
  
    "CHAIN" COMMENT $$$$, 
  
    "ADDRESS" COMMENT $$$$, 
  
    "REGISTRATION_FEE_E8" COMMENT $$$$, 
  
    "FUND_AMOUNT_E8" COMMENT $$$$, 
  
    "EXPIRE" COMMENT $$$$, 
  
    "OWNER" COMMENT $$$$, 
  
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
  NAME,
  chain,
  address,
  registration_fee_e8,
  fund_amount_e8,
  expire,
  owner,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_thorname_change_events
  );
