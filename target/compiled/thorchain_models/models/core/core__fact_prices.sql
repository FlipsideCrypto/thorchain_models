

WITH base AS (

  SELECT
    block_id,
    block_timestamp,
    price_rune_asset,
    price_asset_rune,
    asset_usd,
    rune_usd,
    pool_name,
    _unique_key
  FROM
    THORCHAIN_DEV.silver.prices


)
SELECT
  md5(cast(coalesce(cast(a._unique_key as 
    varchar
), '') as 
    varchar
)) AS fact_prices_id,
  A.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  price_rune_asset,
  price_asset_rune,
  asset_usd,
  rune_usd,
  pool_name,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_id = b.block_id