

SELECT
  bond_e8,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  THORCHAIN_DEV.bronze.rewards_events
  qualify(ROW_NUMBER() over(PARTITION BY event_id, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1