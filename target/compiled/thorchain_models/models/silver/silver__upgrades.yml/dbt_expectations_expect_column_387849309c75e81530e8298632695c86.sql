




    with grouped_expression as (
    select
        
        
    
  


    
regexp_instr(TO_ADDRESS, 'thor[0-9a-zA-Z]{39}', 1, 1)


 > 0
 as expression


    from THORCHAIN_DEV.silver.upgrades
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors




