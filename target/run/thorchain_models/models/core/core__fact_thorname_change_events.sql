

      create or replace transient table THORCHAIN_DEV.core.fact_thorname_change_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    owner,
    chain,
    address,
    expire,
    NAME,
    fund_amount_e8,
    registration_fee_e8,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.thorname_change_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') || '-' || coalesce(cast(a.owner as 
    varchar
), '') || '-' || coalesce(cast(a.chain as 
    varchar
), '') || '-' || coalesce(cast(a.address as 
    varchar
), '') || '-' || coalesce(cast(a.expire as 
    varchar
), '') || '-' || coalesce(cast(a.name as 
    varchar
), '') as 
    varchar
)) AS fact_thorname_change_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  owner,
  chain,
  address,
  expire,
  NAME,
  fund_amount_e8,
  registration_fee_e8,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_thorname_change_events cluster by (block_timestamp::DATE);