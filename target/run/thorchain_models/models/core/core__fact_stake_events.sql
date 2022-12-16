

      create or replace transient table THORCHAIN_DEV.core.fact_stake_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    pool_name,
    asset_tx_id,
    asset_blockchain,
    asset_address,
    asset_e8,
    stake_units,
    rune_tx_id,
    rune_address,
    rune_e8,
    _ASSET_IN_RUNE_E8,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.stake_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.pool_name as 
    varchar
), '') || '-' || coalesce(cast(a.asset_blockchain as 
    varchar
), '') || '-' || coalesce(cast(a.stake_units as 
    varchar
), '') || '-' || coalesce(cast(a.rune_address as 
    varchar
), '') || '-' || coalesce(cast(a.asset_tx_id as 
    varchar
), '') || '-' || coalesce(cast(a.asset_address as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_stake_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  pool_name,
  asset_tx_id,
  asset_blockchain,
  asset_address,
  asset_e8,
  stake_units,
  rune_tx_id,
  rune_address,
  rune_e8,
  _ASSET_IN_RUNE_E8,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_stake_events cluster by (block_timestamp::DATE);