{{ config(
  materialized = 'view'
) }}

SELECT
  in_tx,
  asset,
  asset_e8,
  rune_e8,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__errata_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, in_tx, asset, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
