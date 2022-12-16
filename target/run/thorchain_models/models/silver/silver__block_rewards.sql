

      create or replace transient table THORCHAIN_DEV.silver.block_rewards copy grants as
      (select * from(
            

WITH all_block_id AS (

  SELECT
    block_timestamp,
    MAX(_inserted_timestamp) AS _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.block_pool_depths


GROUP BY
  block_timestamp
),
avg_nodes_tbl AS (
  SELECT
    block_timestamp,
    SUM(
      CASE
        WHEN current_status = 'Active' THEN 1
        WHEN former_status = 'Active' THEN -1
        ELSE 0
      END
    ) AS delta
  FROM
    THORCHAIN_DEV.silver.update_node_account_status_events


GROUP BY
  block_timestamp
),
all_block_with_nodes AS (
  SELECT
    all_block_id.block_timestamp,
    delta,
    SUM(delta) over (
      ORDER BY
        all_block_id.block_timestamp ASC
    ) AS avg_nodes,
    _inserted_timestamp AS _inserted_timestamp
  FROM
    all_block_id
    LEFT JOIN avg_nodes_tbl
    ON all_block_id.block_timestamp = avg_nodes_tbl.block_timestamp
),
all_block_with_nodes_date AS (
  SELECT
    b.block_timestamp :: DATE AS DAY,
    AVG(avg_nodes) AS avg_nodes,
    MAX(
      A._inserted_timestamp
    ) AS _inserted_timestamp
  FROM
    all_block_with_nodes A
    JOIN THORCHAIN_DEV.silver.block_log
    b
    ON A.block_timestamp = b.timestamp
  GROUP BY
    DAY
),
liquidity_fee_tbl AS (
  SELECT
    DATE(
      b.block_timestamp
    ) AS DAY,
    COALESCE(SUM(liq_fee_in_rune_e8), 0) AS liquidity_fee
  FROM
    THORCHAIN_DEV.silver.swap_events A
    JOIN THORCHAIN_DEV.silver.block_log
    b
    ON A.block_timestamp = b.timestamp


GROUP BY
  1
),
bond_earnings_tbl AS (
  SELECT
    DATE(
      b.block_timestamp
    ) AS DAY,
    SUM(bond_e8) AS bond_earnings
  FROM
    THORCHAIN_DEV.silver.rewards_events A
    JOIN THORCHAIN_DEV.silver.block_log
    b
    ON A.block_timestamp = b.timestamp


GROUP BY
  DAY
),
total_pool_rewards_tbl AS (
  SELECT
    DATE(
      b.block_timestamp
    ) AS DAY,
    SUM(rune_e8) AS total_pool_rewards
  FROM
    THORCHAIN_DEV.silver.rewards_event_entries A
    JOIN THORCHAIN_DEV.silver.block_log
    b
    ON A.block_timestamp = b.timestamp


GROUP BY
  DAY
)
SELECT
  all_block_with_nodes_date.day,
  COALESCE((liquidity_fee_tbl.liquidity_fee / power(10, 8)), 0) AS liquidity_fee,
  (
    (
      COALESCE(
        total_pool_rewards_tbl.total_pool_rewards,
        0
      ) + COALESCE(
        bond_earnings_tbl.bond_earnings,
        0
      )
    )
  ) / power(
    10,
    8
  ) AS block_rewards,
  (
    (
      COALESCE(
        total_pool_rewards_tbl.total_pool_rewards,
        0
      ) + COALESCE(
        liquidity_fee_tbl.liquidity_fee,
        0
      ) + COALESCE(
        bond_earnings_tbl.bond_earnings,
        0
      )
    )
  ) / power(
    10,
    8
  ) AS earnings,
  COALESCE((bond_earnings_tbl.bond_earnings / power(10, 8)), 0) AS bonding_earnings,
  (
    (
      COALESCE(
        total_pool_rewards_tbl.total_pool_rewards,
        0
      ) + COALESCE(
        liquidity_fee_tbl.liquidity_fee,
        0
      )
    )
  ) / power(
    10,
    8
  ) AS liquidity_earnings,
  all_block_with_nodes_date.avg_nodes + 2 AS avg_node_count,
  all_block_with_nodes_date._inserted_timestamp
FROM
  all_block_with_nodes_date
  LEFT JOIN liquidity_fee_tbl
  ON all_block_with_nodes_date.day = liquidity_fee_tbl.day
  LEFT JOIN total_pool_rewards_tbl
  ON all_block_with_nodes_date.day = total_pool_rewards_tbl.day
  LEFT JOIN bond_earnings_tbl
  ON all_block_with_nodes_date.day = bond_earnings_tbl.day
            ) order by (_inserted_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.silver.block_rewards cluster by (_inserted_timestamp::DATE);