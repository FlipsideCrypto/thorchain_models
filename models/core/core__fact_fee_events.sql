{{ config(
  materialized = 'incremental',
  unique_key = 'fact_fee_events_id',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    tx_id,
    asset,
    pool_deduct,
    asset_e8,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__fee_events') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(
        _inserted_timestamp
      )
    FROM
      {{ this }}
  )
{% endif %}
)
SELECT
  {{ dbt_utils.surrogate_key(
    ['a.event_id','a.asset','a.asset_e8','a.pool_deduct','a.block_timestamp','a.tx_id ']
  ) }} AS fact_fee_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  asset,
  pool_deduct,
  asset_e8,
  A._INSERTED_TIMESTAMP,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
  LEFT JOIN {{ ref('core__dim_block') }}
  b
  ON A.block_timestamp = b.timestamp