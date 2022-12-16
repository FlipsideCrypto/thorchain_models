select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.dbt_expectations_expect_column_values_to_match_regex_silver.transfers_FROM_ADDRESS
    
      
    ) dbt_internal_test