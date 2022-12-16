
  create or replace  view THORCHAIN_DEV.silver.switch_events
  
    
    
(
  
    "TX_ID" COMMENT $$$$, 
  
    "FROM_ADDRESS" COMMENT $$$$, 
  
    "TO_ADDRESS" COMMENT $$$$, 
  
    "BURN_ASSET" COMMENT $$$$, 
  
    "BURN_E8" COMMENT $$$$, 
  
    "MINT_E8" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  tx AS tx_id,
  from_addr AS from_address,
  to_addr AS to_address,
  burn_asset,
  burn_e8,
  mint_e8,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.switch_events
  qualify(ROW_NUMBER() over(PARTITION BY tx, from_addr, to_addr, burn_asset, burn_e8, mint_e8, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
