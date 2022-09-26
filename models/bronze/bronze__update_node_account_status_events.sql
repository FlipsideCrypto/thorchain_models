{{ config(
  materialized = 'view'
) }}

SELECT
  node_addr,
  "CURRENT" AS current_flag,
  former,
  block_timestamp,
  __HEVO_XMIN,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT
FROM
  {{ source(
    'thorchain_midgard',
    'midgard_update_node_account_status_events'
  ) }}
