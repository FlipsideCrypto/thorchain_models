select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.swaps_LIQ_FEE_RUNE_USD
    
      
    ) dbt_internal_test