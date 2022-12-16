
  create or replace  view THORCHAIN_DEV.bronze.bond_events
  
    
    
(
  
    "TX" COMMENT $$$$, 
  
    "CHAIN" COMMENT $$$$, 
  
    "FROM_ADDR" COMMENT $$$$, 
  
    "TO_ADDR" COMMENT $$$$, 
  
    "ASSET" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "MEMO" COMMENT $$$$, 
  
    "BOND_TYPE" COMMENT $$$$, 
  
    "E8" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "__HEVO__LOADED_AT" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  tx,
  COALESCE(
    chain,
    ''
  ) AS chain,
  COALESCE(
    from_addr,
    ''
  ) AS from_addr,
  COALESCE(
    to_addr,
    ''
  ) AS to_addr,
  COALESCE(
    asset,
    ''
  ) AS asset,
  asset_e8,
  COALESCE(
    memo,
    ''
  ) AS memo,
  COALESCE(
    bond_type,
    ''
  ) AS bond_type,
  e8,
  block_timestamp,
  event_id,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_bond_events
  );
