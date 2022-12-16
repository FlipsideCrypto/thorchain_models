

      create or replace transient table THORCHAIN_DEV.core.fact_daily_tvl copy grants as
      (select * from(
            

WITH base AS (

  SELECT
    DAY,
    total_value_pooled,
    total_value_pooled_usd,
    total_value_bonded,
    total_value_bonded_usd,
    total_value_locked,
    total_value_locked_usd
  FROM
    THORCHAIN_DEV.silver.daily_tvl


)
SELECT
  md5(cast(coalesce(cast(a.day as 
    varchar
), '') as 
    varchar
)) AS fact_daily_tvl_id,
  DAY,
  total_value_pooled,
  total_value_pooled_usd,
  total_value_bonded,
  total_value_bonded_usd,
  total_value_locked,
  total_value_locked_usd,
  'manual' AS _audit_run_id
FROM
  base A
            ) order by (day)
      );
    alter table THORCHAIN_DEV.core.fact_daily_tvl cluster by (day);