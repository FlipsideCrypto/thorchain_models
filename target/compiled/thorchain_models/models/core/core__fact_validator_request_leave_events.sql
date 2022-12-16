

WITH base AS (

  SELECT
    block_timestamp,
    tx_id,
    from_address,
    node_address,
    event_id,
    _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.validator_request_leave_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') || '-' || coalesce(cast(a.tx_id as 
    varchar
), '') || '-' || coalesce(cast(a.from_address as 
    varchar
), '') || '-' || coalesce(cast(a.node_address as 
    varchar
), '') as 
    varchar
)) AS fact_validator_request_leave_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  from_address,
  node_address,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp