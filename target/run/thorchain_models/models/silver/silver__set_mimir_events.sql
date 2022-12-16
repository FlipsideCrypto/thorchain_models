
  create or replace  view THORCHAIN_DEV.silver.set_mimir_events
  
    
    
(
  
    "KEY" COMMENT $$$$, 
  
    "VALUE" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  key,
  VALUE,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.set_mimir_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, key, block_timestamp
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1
  );