
  create or replace  view THORCHAIN_DEV.silver.swap_events
  
    
    
(
  
    "TX_ID" COMMENT $$$$, 
  
    "BLOCKCHAIN" COMMENT $$$$, 
  
    "FROM_ADDRESS" COMMENT $$$$, 
  
    "TO_ADDRESS" COMMENT $$$$, 
  
    "FROM_ASSET" COMMENT $$$$, 
  
    "FROM_E8" COMMENT $$$$, 
  
    "TO_ASSET" COMMENT $$$$, 
  
    "TO_E8" COMMENT $$$$, 
  
    "MEMO" COMMENT $$$$, 
  
    "POOL_NAME" COMMENT $$$$, 
  
    "TO_E8_MIN" COMMENT $$$$, 
  
    "SWAP_SLIP_BP" COMMENT $$$$, 
  
    "LIQ_FEE_E8" COMMENT $$$$, 
  
    "LIQ_FEE_IN_RUNE_E8" COMMENT $$$$, 
  
    "_DIRECTION" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  tx AS tx_id,
  chain AS blockchain,
  from_addr AS from_address,
  to_addr AS to_address,
  from_asset,
  from_e8,
  to_asset,
  to_e8,
  memo,
  pool AS pool_name,
  to_e8_min,
  swap_slip_bp,
  liq_fee_e8,
  liq_fee_in_rune_e8,
  _DIRECTION,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.swap_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, chain, to_addr, from_addr, from_asset, from_e8, to_asset, to_e8, memo, pool, _direction
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1
  );
