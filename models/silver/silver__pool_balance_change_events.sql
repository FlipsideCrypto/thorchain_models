{{ config(
  materialized = 'view'
) }}

SELECT
  asset,
  rune_amt AS rune_amount,
  rune_add,
  asset_amt AS asset_amount,
  asset_add,
  reason,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__pool_balance_change_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, asset, reason, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
