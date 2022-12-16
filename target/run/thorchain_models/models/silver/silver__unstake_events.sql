
  create or replace  view THORCHAIN_DEV.silver.unstake_events
  
    
    
(
  
    "TX_ID" COMMENT $$$$, 
  
    "BLOCKCHAIN" COMMENT $$$$, 
  
    "FROM_ADDRESS" COMMENT $$$$, 
  
    "TO_ADDRESS" COMMENT $$$$, 
  
    "ASSET" COMMENT $$$$, 
  
    "ASSET_E8" COMMENT $$$$, 
  
    "EMIT_ASSET_E8" COMMENT $$$$, 
  
    "EMIT_RUNE_E8" COMMENT $$$$, 
  
    "MEMO" COMMENT $$$$, 
  
    "POOL_NAME" COMMENT $$$$, 
  
    "STAKE_UNITS" COMMENT $$$$, 
  
    "BASIS_POINTS" COMMENT $$$$, 
  
    "ASYMMETRY" COMMENT $$$$, 
  
    "IMP_LOSS_PROTECTION_E8" COMMENT $$$$, 
  
    "_EMIT_ASSET_IN_RUNE_E8" COMMENT $$$$, 
  
    "EVENT_ID" COMMENT $$$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  tx as tx_id,
  chain as blockchain,
  from_addr as from_address,
  to_addr as to_address,
  asset,
  asset_e8,
  emit_asset_e8,
  emit_rune_e8,
  memo,
  pool as pool_name,
  stake_units,
  basis_points,
  asymmetry,
  imp_loss_protection_e8,
  _emit_asset_in_rune_e8,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP

FROM
  THORCHAIN_DEV.bronze.unstake_events
  e qualify(ROW_NUMBER() over(PARTITION BY event_id, tx, chain, memo, stake_units, basis_points, block_timestamp, pool_name, asset, from_addr, to_addr
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
  );
