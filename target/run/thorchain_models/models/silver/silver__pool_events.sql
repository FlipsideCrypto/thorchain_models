
  create or replace  view THORCHAIN_DEV.silver.pool_events
  
    
    
(
  
    "ASSET" COMMENT $$$$, 
  
    "STATUS" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  asset,
  status,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.pool_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, asset, status, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
