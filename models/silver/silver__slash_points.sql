{{ config(
  materialized = 'view'
) }}

SELECT
  node_address,
  slash_points,
  reason,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__slash_points') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, node_address
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1
