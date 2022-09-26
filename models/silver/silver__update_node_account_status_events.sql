{{ config(
  materialized = 'view'
) }}

SELECT
  node_addr as node_address,
  current_flag AS current_status,
  former AS former_status,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__update_node_account_status_events') }}
  e qualify(ROW_NUMBER() over(PARTITION BY node_addr, block_timestamp, former, current_flag
  ORDER BY
  __HEVO__LOADED_AT DESC)) = 1