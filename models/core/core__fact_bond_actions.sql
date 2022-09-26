{{ config(
  materialized = 'incremental',
  unique_key = "fact_bond_actions_id",
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH block_prices AS (

  SELECT
    AVG(rune_usd) AS rune_usd,
    block_id
  FROM
    {{ ref('silver__prices') }}
  GROUP BY
    block_id
),
bond_events AS (
  SELECT
    block_timestamp,
    tx_id,
    from_address,
    to_address,
    asset,
    blockchain,
    bond_type,
    asset_e8,
    e8,
    event_id,
    concat_ws(
      '-',
      tx_id :: STRING,
      from_address :: STRING,
      to_address :: STRING,
      asset_e8 :: STRING,
      bond_type :: STRING,
      e8 :: STRING,
      block_timestamp :: STRING,
      blockchain :: STRING,
      asset :: STRING,
      memo :: STRING
    ) AS _unique_key,
    _inserted_timestamp
  FROM
    {{ ref('silver__bond_events') }}

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
    ['be._unique_key']
  ) }} AS fact_bond_actions_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  from_address,
  to_address,
  asset,
  blockchain,
  bond_type,
  COALESCE(e8 / pow(10, 8), 0) AS asset_amount,
  COALESCE(
    rune_usd * asset_e8,
    0
  ) AS asset_usd,
  be._inserted_timestamp,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  bond_events be
  LEFT JOIN {{ ref('core__dim_block') }}
  b
  ON be.block_timestamp = b.timestamp
  LEFT JOIN block_prices p
  ON b.block_id = p.block_id
