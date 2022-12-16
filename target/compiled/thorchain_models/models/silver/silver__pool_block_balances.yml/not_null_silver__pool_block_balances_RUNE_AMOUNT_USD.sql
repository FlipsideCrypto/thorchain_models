
    
    



select *
from (select * from THORCHAIN_DEV.silver.pool_block_balances where BLOCK_TIMESTAMP <= current_date -2 AND BLOCK_TIMESTAMP >= '2021-04-13') dbt_subquery
where RUNE_AMOUNT_USD is null


