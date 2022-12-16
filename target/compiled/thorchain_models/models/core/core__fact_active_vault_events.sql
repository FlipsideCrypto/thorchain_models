

WITH base AS (

  SELECT
    block_timestamp,
    add_asgard_addr,
    event_id,
    _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.active_vault_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') || '-' || coalesce(cast(a.add_asgard_addr as 
    varchar
), '') as 
    varchar
)) AS fact_active_vault_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  add_asgard_addr,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp