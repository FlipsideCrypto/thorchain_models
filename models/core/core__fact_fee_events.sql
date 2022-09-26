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
    block_timestamp,
    concat_ws(
      '-',
      event_id :: STRING,
      asset :: STRING,
      asset_e8 :: STRING,
      pool_deduct :: STRING,
      block_timestamp :: STRING,
      tx_id :: STRING
    ) AS _unique_key,
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
    ['a._unique_key']
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
