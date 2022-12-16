

WITH base AS (

  SELECT
    block_timestamp,
    former_status,
    current_status,
    node_address,
    _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.update_node_account_status_events


)
SELECT
  md5(cast(coalesce(cast(a.node_address as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') || '-' || coalesce(cast(a.current_status as 
    varchar
), '') || '-' || coalesce(cast(a.former_status as 
    varchar
), '') as 
    varchar
)) AS fact_update_node_account_status_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  former_status,
  current_status,
  node_address,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp