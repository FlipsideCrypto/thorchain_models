

WITH base AS (

  SELECT
    block_id,
    reward_entity,
    rune_amount,
    rune_amount_usd,
    _unique_key,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.total_block_rewards


)
SELECT
  md5(cast(coalesce(cast(a._unique_key as 
    varchar
), '') as 
    varchar
)) AS fact_total_block_rewards_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  reward_entity,
  rune_amount,
  rune_amount_usd,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_id = b.block_id