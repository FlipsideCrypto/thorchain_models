
  create or replace  view THORCHAIN_DEV.silver.transfer_events
  
    
    
(
  
    "FROM_ADDRESS" COMMENT $$$$, 
  
    "TO_ADDRESS" COMMENT $$$$, 
  
    "ASSET" COMMENT $$$$, 
  
    "AMOUNT_E8" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  from_addr AS from_address,
  to_addr AS to_address,
  asset,
  amount_e8,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.transfer_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, from_addr, to_addr, asset, amount_e8, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
