

      create or replace transient table THORCHAIN_DEV.core.dim_block copy grants as
      (select * from(
            

SELECT
    md5(cast(coalesce(cast(height as 
    varchar
), '') as 
    varchar
)) AS dim_block_id,
    height AS block_id,
    block_timestamp,
    block_date,
    block_hour,
    block_week,
    block_month,
    block_quarter,
    block_year,
    block_DAYOFMONTH,
    block_DAYOFWEEK,
    block_DAYOFYEAR,
    TIMESTAMP,
    HASH,
    agg_state,
    _INSERTED_TIMESTAMP,
    'manual' AS _audit_run_id
FROM
    THORCHAIN_DEV.silver.block_log


UNION ALL
SELECT
    '-1' AS dim_block_id,
    -1 AS block_id,
    '1900-01-01' :: datetime AS block_timestamp,
    NULL AS block_date,
    NULL AS block_hour,
    NULL AS block_week,
    NULL AS block_month,
    NULL AS block_quarter,
    NULL AS block_year,
    NULL AS block_DAYOFMONTH,
    NULL AS block_DAYOFWEEK,
    NULL AS block_DAYOFYEAR,
    NULL AS TIMESTAMP,
    NULL AS HASH,
    NULL AS agg_state,
    '1900-01-01' :: DATE AS _inserted_timestamp,
    'manual' AS _audit_run_id
UNION ALL
SELECT
    '-2' AS dim_block_id,
    -2 AS block_id,
    NULL AS block_timestamp,
    NULL AS block_date,
    NULL AS block_hour,
    NULL AS block_week,
    NULL AS block_month,
    NULL AS block_quarter,
    NULL AS block_year,
    NULL AS block_DAYOFMONTH,
    NULL AS block_DAYOFWEEK,
    NULL AS block_DAYOFYEAR,
    NULL AS TIMESTAMP,
    NULL AS HASH,
    NULL AS agg_state,
    '1900-01-01' :: DATE AS _inserted_timestamp,
    'manual' AS _audit_run_id
            ) order by (block_timestamp::DATE)
      );
    alter table THORCHAIN_DEV.core.dim_block cluster by (block_timestamp::DATE);