

      create or replace transient table THORCHAIN_DEV.silver.total_block_rewards copy grants as
      (select * from(
            

WITH block_prices AS (

  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
  FROM
    THORCHAIN_DEV.silver.prices
  GROUP BY
    block_id
)
SELECT
  b.block_timestamp,
  b.height AS block_id,
  ree.pool_name AS reward_entity,
  COALESCE(rune_e8 / pow(10, 8), 0) AS rune_amount,
  COALESCE(rune_e8 / pow(10, 8) * rune_usd, 0) AS rune_amount_usd,
  concat_ws(
    '-',
    b.height,
    reward_entity
  ) AS _unique_key,
  ree._inserted_timestamp
FROM
  THORCHAIN_DEV.silver.rewards_event_entries
  ree
  JOIN THORCHAIN_DEV.silver.block_log
  b
  ON ree.block_timestamp = b.timestamp
  LEFT JOIN THORCHAIN_DEV.silver.prices
  p
  ON b.height = p.block_id
  AND ree.pool_name = p.pool_name


UNION
SELECT
  b.block_timestamp,
  b.height AS block_id,
  'bond_holders' AS reward_entity,
  bond_e8 / pow(
    10,
    8
  ) AS rune_amount,
  bond_e8 / pow(
    10,
    8
  ) * rune_usd AS rune_amount_usd,
  concat_ws(
    '-',
    b.height,
    reward_entity
  ) AS _unique_key,
  re._inserted_timestamp
FROM
  THORCHAIN_DEV.silver.rewards_events
  re
  JOIN THORCHAIN_DEV.silver.block_log
  b
  ON re.block_timestamp = b.timestamp
  LEFT JOIN block_prices p
  ON b.height = p.block_id


            ) order by (_inserted_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.silver.total_block_rewards cluster by (_inserted_timestamp::DATE);