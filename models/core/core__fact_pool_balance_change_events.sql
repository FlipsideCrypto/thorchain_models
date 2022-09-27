{{ config(
  materialized = 'incremental',
  unique_key = 'fact_pool_balance_change_events_id',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    asset,
    rune_amount,
    rune_add,
    asset_amount,
    asset_add,
    reason,
    event_id,
    block_timestamp,
    concat_ws(
      '-',
      event_id :: STRING,
      asset :: STRING,
      block_timestamp :: STRING,
      COALESCE(
        reason :: STRING,
        ''
      )
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__pool_balance_change_events') }}

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
  ) }} AS fact_pool_balance_change_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  asset,
  rune_amount,
  rune_add,
  asset_amount,
  asset_add,
  reason,
  A._INSERTED_TIMESTAMP,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
  LEFT JOIN {{ ref('core__dim_block') }}
  b
  ON A.block_timestamp = b.timestamp