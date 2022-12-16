

WITH base AS (

  SELECT
    DAY,
    liquidity_fees,
    liquidity_fees_usd,
    block_rewards,
    block_rewards_usd,
    total_earnings,
    total_earnings_usd,
    earnings_to_nodes,
    earnings_to_nodes_usd,
    earnings_to_pools,
    earnings_to_pools_usd,
    avg_node_count,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.daily_earnings


)
SELECT
  md5(cast(coalesce(cast(a.day as 
    varchar
), '') as 
    varchar
)) AS fact_daily_earnings_id,
  DAY,
  liquidity_fees,
  liquidity_fees_usd,
  block_rewards,
  block_rewards_usd,
  total_earnings,
  total_earnings_usd,
  earnings_to_nodes,
  earnings_to_nodes_usd,
  earnings_to_pools,
  earnings_to_pools_usd,
  avg_node_count,
  A._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  base A