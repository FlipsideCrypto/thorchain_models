

SELECT
  add_asgard_addr,
  event_id,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.active_vault_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, add_asgard_addr, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1