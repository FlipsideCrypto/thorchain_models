
  create or replace  view THORCHAIN_DEV.bronze.block_pool_depths
  
    
    
(
  
    "POOL" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "RUNE_E8" COMMENT $$$$, 
  
    "SYNTH_E8" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "__HEVO_XMIN" COMMENT $$$$, 
  
    "__HEVO__DATABASE_NAME" COMMENT $$$$, 
  
    "__HEVO__SCHEMA_NAME" COMMENT $$$$, 
  
    "__HEVO__INGESTED_AT" COMMENT $$$$, 
  
    "__HEVO__LOADED_AT" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  pool,
  asset_e8,
  rune_e8,
  synth_e8,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_block_pool_depths
  );
