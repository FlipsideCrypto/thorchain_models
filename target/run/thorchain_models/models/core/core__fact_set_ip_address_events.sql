

      create or replace transient table THORCHAIN_DEV.core.fact_set_ip_address_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    node_address,
    ip_addr,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.set_ip_address_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.node_address as 
    varchar
), '') || '-' || coalesce(cast(a.ip_addr as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_set_ip_address_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  node_address,
  ip_addr,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_set_ip_address_events cluster by (block_timestamp::DATE);