

WITH base AS (

  SELECT
    e.block_timestamp,
    e.tx_id,
    e.rune_e8,
    e.blockchain,
    e.asset_e8,
    e.pool_name,
    e.memo,
    e.to_address,
    e.from_address,
    e.asset,
    e.event_id,
    _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.add_events
    e


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.tx_id as 
    varchar
), '') || '-' || coalesce(cast(a.blockchain as 
    varchar
), '') || '-' || coalesce(cast(a.from_address as 
    varchar
), '') || '-' || coalesce(cast(a.to_address as 
    varchar
), '') || '-' || coalesce(cast(a.asset as 
    varchar
), '') || '-' || coalesce(cast(a.memo as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_add_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.tx_id,
  A.rune_e8,
  A.blockchain,
  A.asset_e8,
  A.pool_name,
  A.memo,
  A.to_address,
  A.from_address,
  A.asset,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp