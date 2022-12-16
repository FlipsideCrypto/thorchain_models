

      create or replace transient table THORCHAIN_DEV.core.fact_pending_liquidity_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    pool_name,
    asset_tx_id,
    asset_blockchain,
    asset_address,
    asset_e8,
    rune_tx_id,
    rune_address,
    rune_e8,
    pending_type,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.pending_liquidity_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.pool_name  as 
    varchar
), '') || '-' || coalesce(cast(a.asset_tx_id as 
    varchar
), '') || '-' || coalesce(cast(a.asset_blockchain as 
    varchar
), '') || '-' || coalesce(cast(a.asset_address as 
    varchar
), '') || '-' || coalesce(cast(a.rune_tx_id as 
    varchar
), '') || '-' || coalesce(cast(a.rune_address  as 
    varchar
), '') || '-' || coalesce(cast(a.pending_type as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_pending_liquidity_events_id,
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
  rune_tx_id,
  rune_address,
  rune_e8,
  pending_type,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_pending_liquidity_events cluster by (block_timestamp::DATE);