
  create or replace  view THORCHAIN_DEV.silver.inactive_vault_events
  
    
    
(
  
    "ADD_ASGARD_ADDRESS" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  add_asgard_addr AS add_asgard_address,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.inactive_vault_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, add_asgard_addr, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
