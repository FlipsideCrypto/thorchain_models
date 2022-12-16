
    
    



select *
from (select * from THORCHAIN_DEV.silver.upgrades where BLOCK_TIMESTAMP <= current_date -2 AND BLOCK_TIMESTAMP >= '2021-04-13') dbt_subquery
where MINT_AMOUNT_USD is null


