
  create or replace  view THORCHAIN_DEV.bronze.block_log
  
    
    
(
  
    "HEIGHT" COMMENT $$$$, 
  
    "TIMESTAMP" COMMENT $$$$, 
  
    "HASH" COMMENT $$$$, 
  
    "AGG_STATE" COMMENT $$$$, 
  
    "__HEVO_XMIN" COMMENT $$$$, 
  
    "__HEVO__DATABASE_NAME" COMMENT $$$$, 
  
    "__HEVO__SCHEMA_NAME" COMMENT $$$$, 
  
    "__HEVO__INGESTED_AT" COMMENT $$$$, 
  
    "__HEVO__LOADED_AT" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  height,
  TIMESTAMP,
  HASH,
  agg_state,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_block_log
  );
