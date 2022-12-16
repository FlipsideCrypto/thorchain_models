

WITH base AS (

  SELECT
    DAY,
    add_asset_liquidity_volume,
    add_liquidity_count,
    add_liquidity_volume,
    add_rune_liquidity_volume,
    asset,
    asset_depth,
    asset_price,
    asset_price_usd,
    average_slip,
    impermanent_loss_protection_paid,
    rune_depth,
    status,
    swap_count,
    swap_volume,
    to_asset_average_slip,
    to_asset_count,
    to_asset_fees,
    to_asset_volume,
    to_rune_average_slip,
    to_rune_count,
    to_rune_fees,
    to_rune_volume,
    totalfees,
    unique_member_count,
    unique_swapper_count,
    units,
    withdraw_asset_volume,
    withdraw_count,
    withdraw_rune_volume,
    withdraw_volume,
    total_stake,
    depth_product,
    liquidity_unit_value_index,
    prev_liquidity_unit_value_index,
    _UNIQUE_KEY
  FROM
    THORCHAIN_DEV.silver.pool_block_statistics


)
SELECT
  md5(cast(coalesce(cast(a._unique_key as 
    varchar
), '') as 
    varchar
)) AS fact_pool_block_statistics_id,
  DAY,
  add_asset_liquidity_volume,
  add_liquidity_count,
  add_liquidity_volume,
  add_rune_liquidity_volume,
  asset,
  asset_depth,
  asset_price,
  asset_price_usd,
  average_slip,
  impermanent_loss_protection_paid,
  rune_depth,
  status,
  swap_count,
  swap_volume,
  to_asset_average_slip,
  to_asset_count,
  to_asset_fees,
  to_asset_volume,
  to_rune_average_slip,
  to_rune_count,
  to_rune_fees,
  to_rune_volume,
  totalfees,
  unique_member_count,
  unique_swapper_count,
  units,
  withdraw_asset_volume,
  withdraw_count,
  withdraw_rune_volume,
  withdraw_volume,
  total_stake,
  depth_product,
  liquidity_unit_value_index,
  prev_liquidity_unit_value_index,
  'manual' AS _audit_run_id
FROM
  base A