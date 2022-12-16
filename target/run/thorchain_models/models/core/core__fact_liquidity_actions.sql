

      create or replace transient table THORCHAIN_DEV.core.fact_liquidity_actions copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    block_id,
    tx_id,
    lp_action,
    pool_name,
    from_address,
    to_address,
    rune_amount,
    rune_amount_usd,
    asset_amount,
    asset_amount_usd,
    stake_units,
    asset_tx_id,
    asset_address,
    asset_blockchain,
    il_protection,
    il_protection_usd,
    unstake_asymmetry,
    unstake_basis_points,
    _unique_key,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.liquidity_actions


)
SELECT
  md5(cast(coalesce(cast(a._unique_key as 
    varchar
), '') as 
    varchar
)) AS fact_liquidity_actions_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  lp_action,
  pool_name,
  from_address,
  to_address,
  rune_amount,
  rune_amount_usd,
  asset_amount,
  asset_amount_usd,
  stake_units,
  asset_tx_id,
  asset_address,
  asset_blockchain,
  il_protection,
  il_protection_usd,
  unstake_asymmetry,
  unstake_basis_points,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_id = b.block_id
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_liquidity_actions cluster by (block_timestamp::DATE);