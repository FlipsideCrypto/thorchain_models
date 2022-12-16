
    
    



select *
from (select * from THORCHAIN_DEV.silver.total_block_rewards where BLOCK_TIMESTAMP <= CURRENT_DATE - 2 AND BLOCK_TIMESTAMP >= '2021-04-13') dbt_subquery
where RUNE_AMOUNT_USD is null


