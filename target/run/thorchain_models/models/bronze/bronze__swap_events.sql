
  create or replace  view THORCHAIN_DEV.bronze.swap_events
  
    
    
(
  
    "TX" COMMENT $$$$, 
  
    "CHAIN" COMMENT $$$$, 
  
    "FROM_ADDR" COMMENT $$$$, 
  
    "TO_ADDR" COMMENT $$$$, 
  
    "FROM_ASSET" COMMENT $$$$, 
  
    "FROM_E8" COMMENT $$$$, 
  
    "TO_ASSET" COMMENT $$$$, 
  
    "TO_E8" COMMENT $$$$, 
  
    "MEMO" COMMENT $$$$, 
  
    "POOL" COMMENT $$$$, 
  
    "TO_E8_MIN" COMMENT $$$$, 
  
    "SWAP_SLIP_BP" COMMENT $$$$, 
  
    "LIQ_FEE_E8" COMMENT $$$$, 
  
    "LIQ_FEE_IN_RUNE_E8" COMMENT $$$$, 
  
    "_DIRECTION" COMMENT $$$$, 
  
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
  chain,
  from_addr,
  to_addr,
  from_asset,
  from_e8,
  to_asset,
  to_e8,
  memo,
  pool,
  to_e8_min,
  swap_slip_bp,
  liq_fee_e8,
  liq_fee_in_rune_e8,
  _DIRECTION,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_swap_events
  );
