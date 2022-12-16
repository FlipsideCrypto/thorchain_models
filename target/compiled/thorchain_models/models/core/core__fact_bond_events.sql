

WITH base AS (

  SELECT
    tx_id,
    blockchain,
    from_address,
    to_address,
    asset,
    asset_e8,
    memo,
    bond_type,
    e8,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.bond_events


)
SELECT
  md5(cast(coalesce(cast(a.tx_id as 
    varchar
), '') || '-' || coalesce(cast(a.from_address as 
    varchar
), '') || '-' || coalesce(cast(a.to_address  as 
    varchar
), '') || '-' || coalesce(cast(a.asset_e8 as 
    varchar
), '') || '-' || coalesce(cast(a.bond_type as 
    varchar
), '') || '-' || coalesce(cast(a.e8 as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') || '-' || coalesce(cast(a.blockchain as 
    varchar
), '') || '-' || coalesce(cast(a.asset as 
    varchar
), '') || '-' || coalesce(cast(a.memo as 
    varchar
), '') as 
    varchar
)) AS fact_bond_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  blockchain,
  from_address,
  to_address,
  asset,
  asset_e8,
  memo,
  bond_type,
  e8,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp