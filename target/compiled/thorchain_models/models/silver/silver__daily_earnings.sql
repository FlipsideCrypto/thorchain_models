

WITH max_daily_block AS (

  SELECT
    MAX(
      block_id
    ) AS block_id,
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DAY
  FROM
    THORCHAIN_DEV.silver.prices A


GROUP BY
  DAY
),
daily_rune_price AS (
  SELECT
    p.block_id,
    DAY,
    AVG(rune_usd) AS rune_usd
  FROM
    THORCHAIN_DEV.silver.prices
    p
    JOIN max_daily_block mdb
    ON p.block_id = mdb.block_id


GROUP BY
  DAY,
  p.block_id
)
SELECT
  br.day,
  COALESCE(
    liquidity_fee,
    0
  ) AS liquidity_fees,
  COALESCE(
    liquidity_fee * rune_usd,
    0
  ) AS liquidity_fees_usd,
  block_rewards AS block_rewards,
  block_rewards * rune_usd AS block_rewards_usd,
  COALESCE(
    earnings,
    0
  ) AS total_earnings,
  COALESCE(
    earnings * rune_usd,
    0
  ) AS total_earnings_usd,
  bonding_earnings AS earnings_to_nodes,
  bonding_earnings * rune_usd AS earnings_to_nodes_usd,
  COALESCE(
    liquidity_earnings,
    0
  ) AS earnings_to_pools,
  COALESCE(
    liquidity_earnings * rune_usd,
    0
  ) AS earnings_to_pools_usd,
  avg_node_count,
  br._inserted_timestamp
FROM
  THORCHAIN_DEV.silver.block_rewards
  br
  JOIN daily_rune_price drp
  ON br.day = drp.day

