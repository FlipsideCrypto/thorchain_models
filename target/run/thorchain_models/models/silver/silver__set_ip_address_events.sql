
  create or replace  view THORCHAIN_DEV.silver.set_ip_address_events
  
    
    
(
  
    "NODE_ADDRESS" COMMENT $$$$, 
  
    "IP_ADDR" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  node_addr AS node_address,
  ip_addr,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.set_ip_address_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, ip_addr, block_timestamp, node_addr
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
