

      create or replace transient table THORCHAIN_DEV.core.fact_block_pool_depths copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    pool_name,
    asset_e8,
    rune_e8,
    synth_e8,
    block_timestamp,
    _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.block_pool_depths


)
SELECT
  md5(cast(coalesce(cast(a.pool_name as 
    varchar
), '') || '-' || coalesce(cast(a.block_timestamp as 
    varchar
), '') as 
    varchar
)) AS fact_pool_depths_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  rune_e8,
  asset_e8,
  synth_e8,
  pool_name,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_block_pool_depths cluster by (block_timestamp::DATE);