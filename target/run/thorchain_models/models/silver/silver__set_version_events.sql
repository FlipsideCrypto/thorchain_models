
  create or replace  view THORCHAIN_DEV.silver.set_version_events
  
    
    
(
  
    "NODE_ADDRESS" COMMENT $$$$, 
  
    "VERSION" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  node_addr AS node_address,
  version,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.set_version_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, node_addr, block_timestamp, version
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
