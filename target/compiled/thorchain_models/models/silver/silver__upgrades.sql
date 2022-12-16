

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
  tx_id,
  from_address,
  to_address,
  burn_asset,
  burn_e8 / pow(
    10,
    8
  ) AS rune_amount,
  burn_e8 / pow(
    10,
    8
  ) * rune_usd AS rune_amount_usd,
  mint_e8 / pow(
    10,
    8
  ) AS mint_amount,
  mint_e8 / pow(
    10,
    8
  ) * rune_usd AS mint_amount_usd,
  concat_ws(
    '-',
    tx_id,
    se.block_timestamp,
    from_address,
    to_address,
    burn_asset
  ) AS _unique_key,
  se._inserted_timestamp
FROM
  THORCHAIN_DEV.silver.switch_events
  se
  JOIN THORCHAIN_DEV.silver.block_log
  b
  ON se.block_timestamp = b.timestamp
  LEFT JOIN block_prices p
  ON b.height = p.block_id

