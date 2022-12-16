

SELECT
  pool AS pool_name,
  asset_tx AS asset_tx_id,
  asset_chain AS asset_blockchain,
  asset_addr AS asset_address,
  asset_e8,
  rune_tx AS rune_tx_id,
  rune_addr AS rune_address,
  rune_e8,
  pending_type,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.pending_liquidity_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, pool, asset_tx, asset_chain, asset_addr, rune_tx, rune_addr, pending_type, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1