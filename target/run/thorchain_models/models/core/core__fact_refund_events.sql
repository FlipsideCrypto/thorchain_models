

      create or replace transient table THORCHAIN_DEV.core.fact_refund_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    tx_id,
    blockchain,
    from_address,
    to_address,
    asset,
    asset_e8,
    asset_2nd,
    asset_2nd_e8,
    memo,
    code,
    reason,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.refund_events


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
), '') || '-' || coalesce(cast(a. asset as 
    varchar
), '') || '-' || coalesce(cast(a.asset_2nd as 
    varchar
), '') || '-' || coalesce(cast(a.memo as 
    varchar
), '') || '-' || coalesce(cast(a.code as 
    varchar
), '') || '-' || coalesce(cast(a.reason as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_refund_events_id,
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
  asset_2nd,
  asset_2nd_e8,
  memo,
  code,
  reason,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_refund_events cluster by (block_timestamp::DATE);