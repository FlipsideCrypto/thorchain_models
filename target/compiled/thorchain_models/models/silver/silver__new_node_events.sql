

SELECT
  node_addr AS node_address,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.new_node_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, node_addr, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1