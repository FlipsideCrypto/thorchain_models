{{ config(
  materialized = 'incremental',
  unique_key = 'fact_add_events_id',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    e.block_timestamp,
    e.tx_id,
    e.rune_e8,
    e.blockchain,
    e.asset_e8,
    e.pool_name,
    e.memo,
    e.to_address,
    e.from_address,
    e.asset,
    concat_ws(
      '-',
      event_id :: STRING,
      tx_id :: STRING,
      blockchain :: STRING,
      from_address :: STRING,
      to_address :: STRING,
      asset :: STRING,
      memo :: STRING,
      pool_name :: STRING,
      block_timestamp :: STRING
    ) AS _unique_key,
    _inserted_timestamp
  FROM
    {{ ref('silver__add_events') }}
    e

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
  ) }} AS fact_add_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.tx_id,
  A.rune_e8,
  A.blockchain,
  A.asset_e8,
  A.pool_name,
  A.memo,
  A.to_address,
  A.from_address,
  A.asset,
  A._inserted_timestamp,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
  LEFT JOIN {{ ref('core__dim_block') }}
  b
  ON A.block_timestamp = b.timestamp
