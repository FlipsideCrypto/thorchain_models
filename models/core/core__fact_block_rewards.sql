{{ config(
  materialized = 'incremental',
  unique_key = 'fact_block_rewards_id',
  incremental_strategy = 'merge'
) }}

WITH base AS (

  SELECT
    DAY,
    liquidity_fee,
    block_rewards,
    earnings,
    bonding_earnings,
    liquidity_earnings,
    avg_node_count,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__block_rewards') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(
        _inserted_timestamp
      )
    FROM
      {{ this }}
  ) - INTERVAL '4 HOURS'
{% endif %}
)
SELECT
  {{ dbt_utils.surrogate_key(
    ['a.day']
  ) }} AS fact_block_rewards_id,
  DAY,
  liquidity_fee,
  block_rewards,
  earnings,
  bonding_earnings,
  liquidity_earnings,
  avg_node_count,
  A._inserted_timestamp,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
