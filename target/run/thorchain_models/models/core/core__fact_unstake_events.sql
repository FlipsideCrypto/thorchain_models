

      create or replace transient table THORCHAIN_DEV.core.fact_unstake_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    e.tx_id,
    e.blockchain,
    e.from_address,
    e.to_address,
    e.asset,
    e.asset_e8,
    e.emit_asset_e8,
    e.emit_rune_e8,
    e.memo,
    e.pool_name,
    e.stake_units,
    e.basis_points,
    e.asymmetry,
    e.imp_loss_protection_e8,
    e._emit_asset_in_rune_e8,
    e.block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.unstake_events
    e


)
SELECT
  md5(cast(coalesce(cast(a.tx_id as 
    varchar
), '') || '-' || coalesce(cast(a.blockchain as 
    varchar
), '') || '-' || coalesce(cast(a.from_address as 
    varchar
), '') || '-' || coalesce(cast(a.to_address as 
    varchar
), '') || '-' || coalesce(cast(a.asset as 
    varchar
), '') || '-' || coalesce(cast(a.asset_e8 as 
    varchar
), '') || '-' || coalesce(cast(a.emit_asset_e8 as 
    varchar
), '') || '-' || coalesce(cast(a.emit_rune_e8 as 
    varchar
), '') || '-' || coalesce(cast(a.memo as 
    varchar
), '') || '-' || coalesce(cast(a.pool_name as 
    varchar
), '') || '-' || coalesce(cast(a.stake_units as 
    varchar
), '') || '-' || coalesce(cast(a.basis_points as 
    varchar
), '') || '-' || coalesce(cast(a.asymmetry as 
    varchar
), '') || '-' || coalesce(cast(a.imp_loss_protection_e8 as 
    varchar
), '') || '-' || coalesce(cast(a._emit_asset_in_rune_e8 as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_unstake_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.tx_id,
  A.blockchain,
  A.from_address,
  A.to_address,
  A.asset,
  A.asset_e8,
  A.emit_asset_e8,
  A.emit_rune_e8,
  A.memo,
  A.pool_name,
  A.stake_units,
  A.basis_points,
  A.asymmetry,
  A.imp_loss_protection_e8,
  A._emit_asset_in_rune_e8,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_unstake_events cluster by (block_timestamp::DATE);