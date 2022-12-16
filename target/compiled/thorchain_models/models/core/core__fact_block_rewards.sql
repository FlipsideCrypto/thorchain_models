

WITH base AS (

  SELECT
    DAY,
    liquidity_fee,
    block_rewards,
    earnings,
    bonding_earnings,
    liquidity_earnings,
    avg_node_count,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.block_rewards


)
SELECT
  md5(cast(coalesce(cast(a.day as 
    varchar
), '') as 
    varchar
)) AS fact_block_rewards_id,
  DAY,
  liquidity_fee,
  block_rewards,
  earnings,
  bonding_earnings,
  liquidity_earnings,
  avg_node_count,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A