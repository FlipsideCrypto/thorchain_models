





with validation_errors as (

    select
        DAY
    from THORCHAIN_DEV.silver.daily_earnings
    group by DAY
    having count(*) > 1

)

select *
from validation_errors

