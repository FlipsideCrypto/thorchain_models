{{ config(
  materialized = 'incremental',
  unique_key = 'fact_stake_events_id',
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
    stake_units,
    rune_tx_id,
    rune_address,
    rune_e8,
    _ASSET_IN_RUNE_E8,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__stake_events') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(
        _inserted_timestamp
      )
    FROM
      {{ this }}
  ) - INTERVAL '48 HOURS'
{% endif %}
)
SELECT
  {{ dbt_utils.surrogate_key(
    ['a.event_id', 'a.pool_name', 'a.asset_blockchain', 'a.stake_units', 'a.rune_address', 'a.asset_tx_id', 'a.asset_address', 'a.block_timestamp']
  ) }} AS fact_stake_events_id,
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
  stake_units,
  rune_tx_id,
  rune_address,
  rune_e8,
  _ASSET_IN_RUNE_E8,
  A._inserted_timestamp,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
  LEFT JOIN {{ ref('core__dim_block') }}
  b
  ON A.block_timestamp = b.timestamp
