
  create or replace  view THORCHAIN_DEV.core.fact_slash_amounts
  
    
    
(
  
    "FACT_SLASH_AMOUNTS_ID" COMMENT $$The surrogate key for the table. Will be unique and is used as a foreign key in other tables$$, 
  
    "BLOCK_TIMESTAMP" COMMENT $$Timestamp of block minting(without a timezone)$$, 
  
    "DIM_BLOCK_ID" COMMENT $$FK to DIM_BLOCK table$$, 
  
    "POOL_NAME" COMMENT $$Name of the pool -- also asset name in other tables$$, 
  
    "ASSET" COMMENT $$Asset name or pool name$$, 
  
    "ASSET_E8" COMMENT $$ The asset amount$$, 
  
    "_INSERTED_TIMESTAMP" COMMENT $$$$, 
  
    "_AUDIT_RUN_ID" COMMENT $$$$
  
)

  copy grants as (
    

WITH base AS (

  SELECT
    pool_name,
    asset,
    asset_e8,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    THORCHAIN_DEV.silver.slash_amounts


)
SELECT
  md5(cast(coalesce(cast(a.event_id as 
    varchar
), '') || '-' || coalesce(cast(a.pool_name as 
    varchar
), '') || '-' || coalesce(cast(a.asset as 
    varchar
), '') as 
    varchar
)) AS fact_slash_amounts_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  pool_name,
  asset,
  asset_e8,
  A._INSERTED_TIMESTAMP,
  'manual' AS _audit_run_id
FROM
  base A
  LEFT JOIN THORCHAIN_DEV.core.dim_block
  b
  ON A.block_timestamp = b.timestamp
  );
