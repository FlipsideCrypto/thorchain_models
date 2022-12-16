

      create or replace transient table THORCHAIN_DEV.core.fact_asgard_fund_yggdrasil_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    tx_id,
    asset,
    asset_e8,
    vault_key,
    event_id,
    block_timestamp,
    _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.asgard_fund_yggdrasil_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.tx_id as 
    varchar
), '') || '-' || coalesce(cast(a.asset  as 
    varchar
), '') || '-' || coalesce(cast(a.asset_e8 as 
    varchar
), '') || '-' || coalesce(cast(a.vault_key as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_asgard_fund_yggdrasil_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.asset,
  A.tx_id,
  A.vault_key,
  A.asset_e8,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_asgard_fund_yggdrasil_events cluster by (block_timestamp::DATE);