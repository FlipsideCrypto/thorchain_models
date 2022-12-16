





with validation_errors as (

    select
        BLOCK_ID, REWARD_ENTITY
    from THORCHAIN_DEV.silver.total_block_rewards
    group by BLOCK_ID, REWARD_ENTITY
    having count(*) > 1

)

select *
from validation_errors


