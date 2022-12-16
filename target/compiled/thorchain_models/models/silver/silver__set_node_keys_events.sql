

SELECT
  node_addr AS node_address,
  secp256k1,
  ed25519,
  validator_consensus,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.set_node_keys_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, node_addr, secp256k1, ed25519, block_timestamp, validator_consensus
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1