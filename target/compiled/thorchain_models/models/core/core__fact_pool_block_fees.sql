

WITH base AS (

  SELECT
    DAY,
    pool_name,
    rewards,
    total_liquidity_fees_rune,
    asset_liquidity_fees,
    rune_liquidity_fees,
    earnings,
    _unique_key,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.pool_block_fees


)
SELECT
  md5(cast(coalesce(cast(a._unique_key as 
    varchar
), '') as 
    varchar
)) AS fact_pool_block_fees_id,
  DAY,
  pool_name,
  rewards,
  total_liquidity_fees_rune,
  asset_liquidity_fees,
  rune_liquidity_fees,
  earnings,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A