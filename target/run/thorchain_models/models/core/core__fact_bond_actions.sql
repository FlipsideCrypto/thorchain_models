

      create or replace transient table THORCHAIN_DEV.core.fact_bond_actions copy grants as
      (select * from(
            

WITH block_prices AS (

  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
  FROM
    THORCHAIN_DEV.silver.prices
  GROUP BY
    block_id
),
bond_events AS (
  SELECT
    block_timestamp,
    tx_id,
    from_address,
    to_address,
    asset,
    blockchain,
    bond_type,
    asset_e8,
    e8,
    memo,
    event_id,
    _inserted_timestamp
  FROM
    THORCHAIN_DEV.silver.bond_events


)
SELECT
  md5(cast(coalesce(cast(be.tx_id as 
    varchar
), '') || '-' || coalesce(cast(be.from_address as 
    varchar
), '') || '-' || coalesce(cast(be.to_address  as 
    varchar
), '') || '-' || coalesce(cast(be.asset_e8 as 
    varchar
), '') || '-' || coalesce(cast(be.bond_type as 
    varchar
), '') || '-' || coalesce(cast(be.e8 as 
    varchar
), '') || '-' || coalesce(cast(be.block_timestamp as 
    varchar
), '') || '-' || coalesce(cast(be.blockchain as 
    varchar
), '') || '-' || coalesce(cast(be.asset as 
    varchar
), '') || '-' || coalesce(cast(be.memo as 
    varchar
), '') as 
    varchar
)) AS fact_bond_actions_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  from_address,
  to_address,
  asset,
  blockchain,
  bond_type,
  COALESCE(e8 / pow(10, 8), 0) AS asset_amount,
  COALESCE(
    rune_usd * asset_e8,
    0
  ) AS asset_usd,
  memo,
  be._inserted_timestamp,
  'manual' AS _audit_run_id
FROM
  bond_events be
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON be.block_timestamp = b.timestamp
  LEFT JOIN block_prices p
  ON b.block_id = p.block_id
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.fact_bond_actions cluster by (block_timestamp::DATE);