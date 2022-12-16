
  create or replace  view THORCHAIN_DEV.bronze.set_node_keys_events
  
    
    
(
  
    "NODE_ADDR" COMMENT $$$$, 
  
    "SECP256K1" COMMENT $$$$, 
  
    "ED25519" COMMENT $$$$, 
  
    "VALIDATOR_CONSENSUS" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "__HEVO_XMIN" COMMENT $$$$, 
  
    "__HEVO__DATABASE_NAME" COMMENT $$$$, 
  
    "__HEVO__SCHEMA_NAME" COMMENT $$$$, 
  
    "__HEVO__INGESTED_AT" COMMENT $$$$, 
  
    "__HEVO__LOADED_AT" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  node_addr,
  secp256k1,
  ed25519,
  validator_consensus,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  HEVO.THORCHAIN_MIDGARD_2_10.midgard_set_node_keys_events
  );
