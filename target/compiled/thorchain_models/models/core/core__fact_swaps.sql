

WITH base AS (

  SELECT
    block_timestamp,
    block_id,
    tx_id,
    blockchain,
    pool_name,
    from_address,
    native_to_address,
    to_pool_address,
    affiliate_address,
    affiliate_fee_basis_points,
    from_asset,
    to_asset,
    from_amount,
    to_amount,
    min_to_amount,
    from_amount_usd,
    to_amount_usd,
    rune_usd,
    asset_usd,
    to_amount_min_usd,
    swap_slip_bp,
    liq_fee_rune,
    liq_fee_rune_usd,
    liq_fee_asset,
    liq_fee_asset_usd,
    _unique_key,
    _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.swaps


)
SELECT
  md5(cast(coalesce(cast(a._unique_key as 
    varchar
), '') as 
    varchar
)) AS fact_swaps_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  blockchain,
  pool_name,
  from_address,
  native_to_address,
  to_pool_address,
  affiliate_address,
  affiliate_fee_basis_points,
  from_asset,
  to_asset,
  from_amount,
  to_amount,
  min_to_amount,
  from_amount_usd,
  to_amount_usd,
  rune_usd,
  asset_usd,
  to_amount_min_usd,
  swap_slip_bp,
  liq_fee_rune,
  liq_fee_rune_usd,
  liq_fee_asset,
  liq_fee_asset_usd,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_id = b.block_id