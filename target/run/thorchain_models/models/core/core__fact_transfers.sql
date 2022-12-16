

      create or replace transient table THORCHAIN_DEV.core.fact_transfers copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    block_id,
    from_address,
    to_address,
    asset,
    rune_amount,
    rune_amount_usd,
    _unique_key,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.transfers


)
SELECT
  md5(cast(coalesce(cast(a._unique_key as 
    varchar
), '') as 
    varchar
)) AS fact_transfers_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  from_address,
  to_address,
  asset,
  rune_amount,
  rune_amount_usd,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_id = b.block_id
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_transfers cluster by (block_timestamp::DATE);