{{ config(
  materialized = 'incremental',
  unique_key = 'fact_total_value_locked_id',
  incremental_strategy = 'merge'
) }}

WITH base AS (

  SELECT
    DAY,
    total_value_pooled,
    total_value_bonded,
    total_value_locked,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__total_value_locked') }}

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
  ) }} AS fact_total_value_locked_id,
  DAY,
  total_value_pooled,
  total_value_bonded,
  total_value_locked,
  _INSERTED_TIMESTAMP,
  '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' AS _audit_run_id
FROM
  base A
