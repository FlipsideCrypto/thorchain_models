{{ config(
  materialized = 'view'
) }}

SELECT
  tx AS tx_id,
  chain AS blockchain,
  from_addr AS from_address,
  to_addr AS to_address,
  asset,
  asset_e8,
  memo,
  bond_type,
  e8,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__bond_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY tx, from_addr, asset_e8, bond_type, e8, block_timestamp, to_addr, chain, asset, memo
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
