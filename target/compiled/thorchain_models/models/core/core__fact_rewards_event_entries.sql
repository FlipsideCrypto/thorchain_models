

WITH base AS (

  SELECT
    pool_name,
    rune_e8,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.rewards_event_entries


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.pool_name as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_rewards_event_entries_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  pool_name,
  rune_e8,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp