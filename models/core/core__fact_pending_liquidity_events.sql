{{ config(
  materialized = 'incremental',
  unique_key = 'fact_pending_liquidity_events_id',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    pool_name,
    asset_tx_id,
    asset_blockchain,
    asset_address,
    asset_e8,
    rune_tx_id,
    rune_address,
    rune_e8,
    pending_type,
    event_id,
    block_timestamp,
    concat_ws(
      '-',
      event_id :: STRING,
      pool_name :: STRING,
      COALESCE(
        asset_tx_id :: STRING,
        ''
      ),
      COALESCE(
        asset_blockchain :: STRING,
        ''
      ),
      COALESCE(
        asset_address :: STRING,
        ''
      ),
      COALESCE(
        rune_tx_id :: STRING,
        ''
      ),
      rune_address :: STRING,
      pending_type :: STRING,
      block_timestamp :: STRING
    ) AS _unique_key,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__pending_liquidity_events') }}

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
  ) }} AS fact_pending_liquidity_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  pool_name,
  asset_tx_id,
  asset_blockchain,
  asset_address,
  asset_e8,
  rune_tx_id,
  rune_address,
  rune_e8,
  pending_type,
  A._INSERTED_TIMESTAMP,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
  LEFT JOIN {{ ref('core__dim_block') }}
  b
  ON A.block_timestamp = b.timestamp
