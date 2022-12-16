select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from THORCHAIN_DEV.dbt_expectations_expect_row_values_to_have_recent_data_silver.total_block_rewards_BLOCK_TIMESTAMP
    
      
    ) dbt_internal_test