{{ config(
  materialized = 'incremental',
  unique_key = 'fact_asgard_fund_yggdrasil_events_id',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    tx_id,
    asset,
    asset_e8,
    vault_key,
    event_id,
    block_timestamp,
    concat_ws(
      '-',
      event_id :: STRING,
      tx_id :: STRING,
      asset :: STRING,
      asset_e8 :: STRING,
      vault_key :: STRING,
      block_timestamp :: STRING
    ) AS _unique_key,
    _inserted_timestamp
  FROM
    {{ ref('silver__asgard_fund_yggdrasil_events') }}

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
  ) }} AS fact_asgard_fund_yggdrasil_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.asset,
  A.tx_id,
  A.vault_key,
  A.asset_e8,
  A._inserted_timestamp,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
  LEFT JOIN {{ ref('core__dim_block') }}
  b
  ON A.block_timestamp = b.timestamp
