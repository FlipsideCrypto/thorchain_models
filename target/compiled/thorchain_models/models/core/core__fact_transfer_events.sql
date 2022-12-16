

WITH base AS (

  SELECT
    from_address,
    to_address,
    asset,
    amount_e8,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.transfer_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.from_address as 
    varchar
), '') || '-' || coalesce(cast(a.to_address as 
    varchar
), '') || '-' || coalesce(cast(a.asset as 
    varchar
), '') || '-' || coalesce(cast(a.amount_e8 as 
    varchar
), '') as 
    varchar
)) AS fact_transfer_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  from_address,
  to_address,
  asset,
  amount_e8,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp