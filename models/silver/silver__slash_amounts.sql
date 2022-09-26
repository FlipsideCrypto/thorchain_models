{{ config(
  materialized = 'view'
) }}

SELECT
  pool AS pool_name,
  asset,
  asset_e8,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__slash_amounts') }}
