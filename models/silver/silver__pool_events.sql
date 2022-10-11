{{ config(
  materialized = 'view'
) }}

SELECT
  asset,
  status,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__pool_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, asset, status, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1