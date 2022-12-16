

WITH base AS (

  SELECT
    DAY,
    total_value_pooled,
    total_value_bonded,
    total_value_locked,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.total_value_locked


)
SELECT
  md5(cast(coalesce(cast(a.day as 
    varchar
), '') as 
    varchar
)) AS fact_total_value_locked_id,
  DAY,
  total_value_pooled,
  total_value_bonded,
  total_value_locked,
  _INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A