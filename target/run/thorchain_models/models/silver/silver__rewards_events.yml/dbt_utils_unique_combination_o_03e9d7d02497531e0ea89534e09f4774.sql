select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.dbt_utils_unique_combination_of_columns_silver.rewards_events_BLOCK_TIMESTAMP
    
      
    ) dbt_internal_test