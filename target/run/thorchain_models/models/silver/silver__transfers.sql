

      create or replace transient table THORCHAIN_DEV.silver.transfers copy grants as
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
  from_address,
  to_address,
  asset,
  COALESCE(amount_e8 / pow(10, 8), 0) AS rune_amount,
  COALESCE(amount_e8 / pow(10, 8) * rune_usd, 0) AS rune_amount_usd,
  event_id,
  concat_ws(
    b.height :: STRING,
    from_address :: STRING,
    to_address :: STRING,
    asset :: STRING,
    event_id :: STRING
  ) _unique_key,
  se._inserted_timestamp
FROM
  THORCHAIN_DEV.silver.transfer_events
  se
  JOIN THORCHAIN_DEV.silver.block_log
  b
  ON se.block_timestamp = b.timestamp
  LEFT JOIN block_prices p
  ON b.height = p.block_id


            ) order by (_inserted_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.silver.transfers cluster by (_inserted_timestamp::DATE);