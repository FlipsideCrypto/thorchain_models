

      create or replace transient table THORCHAIN_DEV.core.fact_switch_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    tx_id,
    from_address,
    to_address,
    burn_asset,
    burn_e8,
    mint_e8,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.switch_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.tx_id as 
    varchar
), '') || '-' || coalesce(cast(from_address as 
    varchar
), '') || '-' || coalesce(cast(a.to_address as 
    varchar
), '') || '-' || coalesce(cast(a.burn_asset as 
    varchar
), '') || '-' || coalesce(cast(a.burn_e8 as 
    varchar
), '') || '-' || coalesce(cast(a.mint_e8 as 
    varchar
), '') as 
    varchar
)) AS fact_switch_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  from_address,
  to_address,
  burn_asset,
  burn_e8,
  mint_e8,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_switch_events cluster by (block_timestamp::DATE);