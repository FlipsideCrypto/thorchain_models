
  create or replace  view THORCHAIN_DEV.silver.constants
  
    
    
(
  
    "KEY" COMMENT $$$$, 
  
    "VALUE" COMMENT $$$$
  
)

  copy grants as (
    

SELECT
  C.key,
  C.value
FROM
  THORCHAIN_DEV.bronze.constants C
  );
