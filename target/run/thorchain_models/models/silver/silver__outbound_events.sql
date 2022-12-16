
  create or replace  view THORCHAIN_DEV.silver.outbound_events
  
    
    
(
  
    "TX_ID" COMMENT $$$$, 
  
    "BLOCKCHAIN" COMMENT $$$$, 
  
    "FROM_ADDRESS" COMMENT $$$$, 
  
    "TO_ADDRESS" COMMENT $$$$, 
  
    "ASSET" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "MEMO" COMMENT $$$$, 
  
    "IN_TX" COMMENT $$$$, 
  
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
  asset,
  asset_e8,
  memo,
  in_tx,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.outbound_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, chain, from_addr, to_addr, asset, memo, in_tx, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
