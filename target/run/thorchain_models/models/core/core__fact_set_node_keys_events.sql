

      create or replace transient table THORCHAIN_DEV.core.fact_set_node_keys_events copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    node_address,
    secp256k1,
    ed25519,
    validator_consensus,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.set_node_keys_events


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.node_address as 
    varchar
), '') || '-' || coalesce(cast(a.secp256k1 as 
    varchar
), '') || '-' || coalesce(cast(a.ed25519 as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') || '-' || coalesce(cast(a.validator_consensus as 
    varchar
), '') as 
    varchar
)) AS fact_set_node_keys_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  node_address,
  secp256k1,
  ed25519,
  validator_consensus,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_set_node_keys_events cluster by (block_timestamp::DATE);