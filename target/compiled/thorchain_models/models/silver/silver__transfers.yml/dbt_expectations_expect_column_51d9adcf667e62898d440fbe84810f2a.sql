




    with grouped_expression as (
    select
        
        
    
  


    
regexp_instr(FROM_ADDRESS, 'thor[0-9a-zA-Z]{39}', 1, 1)


 > 0
 as expression


    from (select * from THORCHAIN_DEV.silver.transfers where FROM_ADDRESS <> 'MidgardBalanceCorrectionAddress') dbt_subquery
    

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




