
  create or replace  view THORCHAIN_DEV.silver.validator_request_leave_events
  
    
    
(
  
    "TX_ID" COMMENT $$$$, 
  
    "FROM_ADDRESS" COMMENT $$$$, 
  
    "NODE_ADDRESS" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  tx AS tx_id,
  from_addr AS from_address,
  node_addr AS node_address,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.validator_request_leave_events
  e qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, block_timestamp, from_addr, node_addr
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1
  );
