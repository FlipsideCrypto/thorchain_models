
    
    



select *
from (select * from THORCHAIN_DEV.silver.liquidity_actions where BLOCK_TIMESTAMP <= SYSDATE() - interval '2 day' AND BLOCK_TIMESTAMP >= '2021-04-13') dbt_subquery
where RUNE_AMOUNT_USD is null


