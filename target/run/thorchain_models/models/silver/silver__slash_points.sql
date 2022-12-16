
  create or replace  view THORCHAIN_DEV.silver.slash_points
  
    
    
(
  
    "NODE_ADDRESS" COMMENT $$$$, 
  
    "SLASH_POINTS" COMMENT $$$$, 
  
    "REASON" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  node_address,
  slash_points,
  reason,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.slash_points
  );
