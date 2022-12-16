select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.not_null_silver.daily_tvl_TOTAL_VALUE_LOCKED_USD
    
      
    ) dbt_internal_test